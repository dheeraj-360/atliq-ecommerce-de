SELECT
    d AS date_day,
    CAST(DATE_FORMAT(d, 'yyyyMMdd') AS INT) AS date_key,
    YEAR(d) AS year,
    QUARTER(d) AS quarter,
    MONTH(d) AS month,
    DATE_FORMAT(d, 'MMM') AS month_name,
    DATE_FORMAT(d, 'yyyy-MM') AS year_month,
    DAYOFWEEK(d) AS day_of_week,
    DATE_FORMAT(d, 'EEE') AS weekday_name
FROM (
    SELECT EXPLODE(
        SEQUENCE(
            DATE '2024-01-01',
            MAKE_DATE(YEAR(CURRENT_DATE()) + 1, 12, 31),
            INTERVAL 1 DAY
        )
    ) AS d
)