-- Step 1: Create the dim_datetime table
CREATE TABLE dim_datetime (
    datetime_key BIGINT PRIMARY KEY,
    datetime TIMESTAMP NOT NULL,
    date DATE NOT NULL,
    year INT NOT NULL,
    quarter INT NOT NULL,
    month INT NOT NULL,
    day INT NOT NULL,
    day_of_week INT NOT NULL,
    hour INT NOT NULL,
    minute INT NOT NULL
);

-- Step 2: Populate data from 2025-01-01 00:00 to 2026-12-31 23:59
INSERT INTO dim_datetime (
    datetime_key, datetime, date, year, quarter, month, day,
    day_of_week, hour, minute
)
SELECT
    EXTRACT(YEAR FROM ts) * 100000000 + EXTRACT(MONTH FROM ts) * 1000000 +
    EXTRACT(DAY FROM ts) * 10000 + EXTRACT(HOUR FROM ts) * 100 +
    EXTRACT(MINUTE FROM ts) AS datetime_key,
    ts AS datetime,
    DATE(ts) AS date,
    EXTRACT(YEAR FROM ts)::INT AS year,
    EXTRACT(QUARTER FROM ts)::INT AS quarter,
    EXTRACT(MONTH FROM ts)::INT AS month,
    EXTRACT(DAY FROM ts)::INT AS day,
    EXTRACT(ISODOW FROM ts)::INT AS day_of_week,
    EXTRACT(HOUR FROM ts)::INT AS hour,
    EXTRACT(MINUTE FROM ts)::INT AS minute
FROM generate_series(
    TIMESTAMP '2025-01-01 00:00:00',
    TIMESTAMP '2026-12-31 23:59:00',
    INTERVAL '1 minute'
) AS ts;
