/*
AtliQ Commerce — M5 pipeline audit log
Target: the existing Azure SQL OLTP database (atliq_commerce)
Purpose: log each master ADF pipeline run and, on success, current SQL source-table counts.

IMPORTANT: source_*_rows below are current OLTP source-table totals, NOT incremental
rows copied, nor Gold fact_sales counts. Gold validation remains in Databricks.

Run this script once in VS Code's MSSQL extension connected to atliq_commerce.
Commit this .sql file to your repository; it contains no credentials.
*/

IF SCHEMA_ID(N'etl') IS NULL
    EXEC(N'CREATE SCHEMA etl');
GO

IF OBJECT_ID(N'etl.pipeline_run_audit', N'U') IS NULL
BEGIN
    CREATE TABLE etl.pipeline_run_audit
    (
        run_id                 NVARCHAR(64)   NOT NULL PRIMARY KEY,
        pipeline_name          NVARCHAR(128)  NOT NULL,
        run_date_utc           DATE           NOT NULL,
        started_at_utc         DATETIME2(3)   NOT NULL
            CONSTRAINT DF_pipeline_audit_started DEFAULT SYSUTCDATETIME(),
        finished_at_utc        DATETIME2(3)   NULL,
        run_status             NVARCHAR(12)   NOT NULL
            CONSTRAINT DF_pipeline_audit_status DEFAULT N'Running',
        source_orders_rows     BIGINT         NULL,
        source_order_items_rows BIGINT        NULL,
        source_payments_rows   BIGINT         NULL,
        error_stage            NVARCHAR(128)  NULL,
        error_message          NVARCHAR(2000) NULL,
        CONSTRAINT CK_pipeline_audit_status
            CHECK (run_status IN (N'Running', N'Succeeded', N'Failed'))
    );
END;
GO

CREATE OR ALTER PROCEDURE etl.usp_log_pipeline_run
    @run_id         NVARCHAR(64),
    @pipeline_name  NVARCHAR(128),
    @run_date_utc   NVARCHAR(10),
    @run_status     NVARCHAR(12),
    @error_stage    NVARCHAR(128) = NULL,
    @error_message  NVARCHAR(MAX) = NULL
AS
BEGIN
    SET NOCOUNT ON;
    SET XACT_ABORT ON;

    IF @run_status NOT IN (N'Running', N'Succeeded', N'Failed')
        THROW 50001, 'Invalid audit status.', 1;

    DECLARE @parsed_date DATE = TRY_CONVERT(DATE, @run_date_utc, 23);
    IF @parsed_date IS NULL
        THROW 50002, 'run_date_utc must be YYYY-MM-DD.', 1;

    BEGIN TRY
        BEGIN TRANSACTION;

        -- Idempotent start: avoid creating a second row on retry.
        IF NOT EXISTS (
            SELECT 1
            FROM etl.pipeline_run_audit WITH (UPDLOCK, HOLDLOCK)
            WHERE run_id = @run_id
        )
        BEGIN
            INSERT INTO etl.pipeline_run_audit
                (run_id, pipeline_name, run_date_utc, run_status)
            VALUES
                (@run_id, @pipeline_name, @parsed_date, N'Running');
        END;

        IF @run_status = N'Succeeded'
        BEGIN
            UPDATE a
               SET run_status = N'Succeeded',
                   finished_at_utc = SYSUTCDATETIME(),
                   source_orders_rows = (SELECT COUNT_BIG(*) FROM dbo.orders),
                   source_order_items_rows = (SELECT COUNT_BIG(*) FROM dbo.order_items),
                   source_payments_rows = (SELECT COUNT_BIG(*) FROM dbo.payments)
            FROM etl.pipeline_run_audit AS a
            WHERE a.run_id = @run_id;
        END;

        IF @run_status = N'Failed'
        BEGIN
            UPDATE a
               SET run_status = N'Failed',
                   finished_at_utc = SYSUTCDATETIME(),
                   error_stage = COALESCE(a.error_stage, @error_stage),
                   error_message = COALESCE(a.error_message, LEFT(@error_message, 2000))
            FROM etl.pipeline_run_audit AS a
            WHERE a.run_id = @run_id
              AND a.run_status <> N'Succeeded';
        END;

        COMMIT TRANSACTION;
    END TRY
    BEGIN CATCH
        IF @@TRANCOUNT > 0 ROLLBACK TRANSACTION;
        THROW;
    END CATCH;
END;
GO

