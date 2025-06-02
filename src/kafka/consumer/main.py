from src.kafka.consumer.trade_consumer import TradeConsumer
from src.logger import logger
import json
from src.utils import load_config
from src.db_ops.timescaledb_ops import TimescaleDBOps
from datetime import datetime

if __name__ == "__main__":
    #############################################################
    # LOAD CONFIGURATION AND CONNECT TO TIMESCALEDB
    #############################################################
    config = load_config("src/db_ops/timescaledb.ini", "timescaledb")
    db_ops = TimescaleDBOps(config)

    #############################################################
    # CREATE TABLE AND HYPERTABLE
    #############################################################
    db_ops.create_table(
        "trades",
        columns={
            "trade_id": "TEXT",
            "symbol": "TEXT",
            "price": "DOUBLE PRECISION",
            "qty": "DOUBLE PRECISION",
            "side": "TEXT",
            "event_timestamp": "TIMESTAMPTZ",
        },
        primary_key=["trade_id", "event_timestamp"],
    )
    db_ops.create_hypertable("trades", time_column="event_timestamp")

    #############################################################
    # CREATE CONTINUOUS AGGREGATE VIEWS
    #############################################################
    db_ops.create_continuous_aggregation(
        "trades",
        "btc_10_seconds_ohlcv",
        columns=[
            "time_bucket('10 seconds', \"event_timestamp\") AS timestamp_10_seconds",
            "symbol",
            'candlestick_agg("event_timestamp", price, qty) as candlestick',
        ],
        group_by_columns=["timestamp_10_seconds", "symbol"],
    )
    db_ops.create_automatic_refresh_policy(
        "btc_10_seconds_ohlcv", "1 week", "10 seconds", "10 seconds"
    )

    # Create additional continuous aggregates using base table of btc_10_seconds_ohlcv
    db_ops.create_continuous_aggregation(
        "btc_10_seconds_ohlcv",
        "btc_30_seconds_ohlcv",
        columns=[
            "time_bucket('30 seconds', \"timestamp_10_seconds\") AS timestamp_30_seconds",
            "symbol",
            "rollup(candlestick) as candlestick",
        ],
        group_by_columns=["timestamp_30_seconds", "symbol"],
    )
    db_ops.create_automatic_refresh_policy(
        "btc_30_seconds_ohlcv", "1 week", "30 seconds", "30 seconds"
    )

    # Create additional continuous aggregates using base table of btc_30_seconds_ohlcv
    db_ops.create_continuous_aggregation(
        "btc_30_seconds_ohlcv",
        "btc_1_minute_ohlcv",
        columns=[
            "time_bucket('1 minute', \"timestamp_30_seconds\") AS timestamp_1_minute",
            "symbol",
            "rollup(candlestick) as candlestick",
        ],
        group_by_columns=["timestamp_1_minute", "symbol"],
    )
    db_ops.create_automatic_refresh_policy(
        "btc_1_minute_ohlcv", "1 week", "1 minute", "1 minute"
    )

    #############################################################
    # CREATE NOTIFICATION CHANNEL AND TRIGGER
    #############################################################
    db_ops.listen_notification(channel_name="btc_kraken_trades_channel")
    db_ops.create_notify(
        channel_name="btc_kraken_trades_channel", function_name="notify_trade_event"
    )
    db_ops.create_trigger(
        table_name="trades",
        trigger_name="trades_notify_trigger",
        function_name="notify_trade_event",
    )

    ##############################################################
    # CREATE CONSUMER FOR SUBCRIBING TO KAFKA TOPIC
    ##############################################################
    consumer = TradeConsumer(
        "btc-kraken-trades-topic",
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
                        columns=[
                            "trade_id",
                            "symbol",
                            "price",
                            "qty",
                            "side",
                            "event_timestamp",
                        ],
                        values=(
                            message.value["trade_id"],
                            message.value["symbol"],
                            message.value["price"],
                            message.value["qty"],
                            message.value["side"],
                            datetime.strptime(
                                message.value["trade_timestamp"],
                                "%Y-%m-%dT%H:%M:%S.%fZ",
                            ),
                        ),
                    )
    except KeyboardInterrupt:
        logger.info("Exiting...")
        consumer.shutdown()

    # #############################################################
    # # CLOSE CONNECTION TO TIMESCALEDB
    # #############################################################
    db_ops.close_connection()
