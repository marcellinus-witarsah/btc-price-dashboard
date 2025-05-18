from src.consumer.trade_consumer import TradeConsumer
from src.logger import logger
import json
from src.utils import load_config
from src.db_ops.timescaledb_ops import TimescaleDBOps
from src.utils import convert_unixtime_to_timestamp_tz

if __name__ == "__main__":

    config = load_config("src/db_ops/timescaledb.ini", "timescaledb")
    db_ops = TimescaleDBOps(config)
    db_ops.connect()
    db_ops.create_table("trades", [
        "trade_id TEXT", 
        "symbol TEXT", 
        "price FLOAT", 
        "qty FLOAT",
        "side TEXT",
        "event_timestamp TIMESTAMPTZ",
        "PRIMARY KEY (trade_id, event_timestamp)"
    ], time_column="event_timestamp")
    
    consumer = TradeConsumer(
        "btc-trades-topic",
        bootstrap_servers=["localhost:9092"],
        auto_offset_reset="earliest",
        enable_auto_commit=True,
        key_deserializer=lambda serialized_key: serialized_key.decode("utf-8"),
        value_deserializer=lambda serialized_value: json.loads(serialized_value.decode("utf-8"))
    )
    
    try:
        while True:
            messages = consumer.consume_trade()
            for message in messages:
                if message is not None:
                    logger.info(f"Trade ID: {message.value['trade_id']}")
                    db_ops.insert_data(
                        "trades",
                        (
                            message.value["trade_id"],
                            message.value["symbol"],
                            message.value["price"],
                            message.value["qty"],
                            message.value["side"],
                            convert_unixtime_to_timestamp_tz(message.value["trade_timestamp"], 'Asia/Jakarta')
                        )
                    )
            
    except KeyboardInterrupt:
        logger.info("Exiting...")
        consumer.shutdown()
    
    db_ops.close_connection()