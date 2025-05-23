from src.consumer.trade_consumer import TradeConsumer
from src.logger import logger
import json
from src.utils import load_config
from src.timescaledb_ops import TimescaleDBOps
from src.utils import convert_unixtime_to_timestamp_tz

if __name__ == "__main__":
    #############################################################
    # LOAD CONFIGURATION AND CONNECT TO TIMESCALEDB
    #############################################################
    config = load_config("src/timescaledb.ini", "timescaledb")
    db_ops = TimescaleDBOps(config)
    db_ops.connect()

    #############################################################
    # CREATE TABLE AND HYPERTABLE
    #############################################################
    db_ops.create_table(
        "trades",
        [
            "trade_id TEXT",
            "symbol TEXT",
            "price DOUBLE PRECISION",
            "qty DOUBLE PRECISION",
            "side TEXT",
            "event_timestamp TIMESTAMPTZ",
            "PRIMARY KEY (trade_id, event_timestamp)",
        ],
    )
    db_ops.create_hypertable("trades", time_column="event_timestamp")

    #############################################################
    # CREATE CONTINUOUS AGGREGATE VIEWS
    #############################################################
    db_ops.create_continuous_aggregation(
        "trades",
        "btc_10_seconds_ohlcv",
        cols=[
            "time_bucket('10 seconds', \"event_timestamp\") AS timestamp_10_seconds",
            "symbol",
            'candlestick_agg("event_timestamp", price, qty) as candlestick',
        ],
        groupby_cols=["timestamp_10_seconds", "symbol"],
    )
    db_ops.create_automatic_refresh_policy(
        "btc_10_seconds_ohlcv", "1 week", "10 seconds", "10 seconds"
    )

    # Create additional continuous aggregates using base table of tc_10_seconds_ohlcv
    db_ops.create_continuous_aggregation(
        "btc_10_seconds_ohlcv",
        "btc_20_seconds_ohlcv",
        cols=[
            "time_bucket('20 seconds', \"timestamp_10_seconds\") AS timestamp_20_seconds",
            "symbol",
            "rollup(candlestick) as candlestick",
        ],
        groupby_cols=["timestamp_20_seconds", "symbol"],
    )
    db_ops.create_automatic_refresh_policy(
        "btc_20_seconds_ohlcv", "1 week", "20 seconds", "20 seconds"
    )
    
    # Create additional continuous aggregates using base table of tc_10_seconds_ohlcv
    db_ops.create_continuous_aggregation(
        "btc_10_seconds_ohlcv",
        "btc_30_seconds_ohlcv",
        cols=[
            "time_bucket('30 seconds', \"timestamp_10_seconds\") AS timestamp_30_seconds",
            "symbol",
            "rollup(candlestick) as candlestick",
        ],
        groupby_cols=["timestamp_30_seconds", "symbol"],
    )
    db_ops.create_automatic_refresh_policy(
        "btc_30_seconds_ohlcv", "1 week", "30 seconds", "30 seconds"
    )
    ##############################################################
    # CREATE CONSUMER FOR SUBCRIBING TO KAFKA TOPIC
    ##############################################################
    consumer = TradeConsumer(
        "btc-trades-topic",
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        key_deserializer=lambda serialized_key: serialized_key.decode("utf-8"),
        value_deserializer=lambda serialized_value: json.loads(
            serialized_value.decode("utf-8")
        ),
    )

    try:
        #############################################################
        # INSERT NEW RECORD INTO TIMESCALEDB ONE BY ONE
        #############################################################
        while True:
            messages = consumer.consume_trade()
            for message in messages:
                if message is not None:
                    db_ops.insert_data(
                        "trades",
                        (
                            message.value["trade_id"],
                            message.value["symbol"],
                            message.value["price"],
                            message.value["qty"],
                            message.value["side"],
                            convert_unixtime_to_timestamp_tz(
                                message.value["trade_timestamp"], "Asia/Jakarta"
                            ),
                        ),
                    )
    except KeyboardInterrupt:
        logger.info("Exiting...")
        consumer.shutdown()

    #############################################################
    # CLOSE CONNECTION TO TIMESCALEDB
    #############################################################
    db_ops.close_connection()
