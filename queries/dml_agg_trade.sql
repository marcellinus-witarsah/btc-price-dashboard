CREATE MATERIALIZED VIEW btc_candlestick_minute
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 minute', "event_timestamp") AS day,
    symbol,
    sum(qty) qty,
    max(price) AS high,
    first(price, event_timestamp) AS open,
    last(price, event_timestamp) AS close,
    min(price) AS low
FROM trades srt
GROUP BY day, symbol;