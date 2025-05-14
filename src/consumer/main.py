from src.consumer.trade_consumer import TradeConsumer
from src.logger import logger
import json
from src.utils import load_config
from src.db_ops.postgres_db_ops import PostgresDBOps

if __name__ == "__main__":

    config = load_config("src/db_ops/postgres.ini", "postgresql")
    db_ops = PostgresDBOps(config)
    db_ops.connect()
    db_ops.create_table("trades", [
        "trade_id TEXT PRIMARY KEY", 
        "symbol TEXT", 
        "price MONEY", 
        "qty NUMERIC",
        "side TEXT",
        "timestamp INT"
    ])
    
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
                    logger.info(f"Symbol: {message.value['symbol']}")
                    logger.info(f"Price: {message.value['price']}")
                    logger.info(f"Quantity: {message.value['qty']}")
                    logger.info(f"Side: {message.value['side']}")
                    logger.info(f"Timestamp: {message.value['timestamp']}")
                    db_ops.insert_data(
                        "trades",
                        (
                            message.value["trade_id"],
                            message.value["symbol"],
                            message.value["price"],
                            message.value["qty"],
                            message.value["side"],
                            message.value["timestamp"]
                        )
                    )
            
    except KeyboardInterrupt:
        logger.info("Exiting...")
        consumer.shutdown()
        
    db_ops.close_connection()