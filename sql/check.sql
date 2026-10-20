
SELECT
    DB_NAME() AS database_name,
    SUSER_SNAME() AS connected_user,
    SYSDATETIMEOFFSET() AS server_datetime;
