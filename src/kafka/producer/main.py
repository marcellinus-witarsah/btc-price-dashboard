from src.data.kraken_ws import KrakenWebSocket
import json
from src.kafka.producer.trade_producer import TradeProducer
from src.logger import logger

if __name__ == "__main__":
    kraken_source = KrakenWebSocket(url="wss://ws.kraken.com/v2", symbols=["BTC/USD"])
    kraken_source.subscribe()

    # Create a Kafka producer
    producer = TradeProducer(
        topic="btc-kraken-trades-topic",
        bootstrap_servers=["localhost:9092"],
        key_serializer=lambda key: str(key).encode("utf-8"),
        value_serializer=lambda json_obj: json.dumps(json_obj).encode("utf-8"),
    )

    try:
        while True:
            trades_data = kraken_source.get_trades()
            if trades_data is not None:
                # Produce messages to the topic
                for trade_data in trades_data:
                    logger.info(f"Trade Data: {trade_data}")
                    producer.produce_trade(trade_data)
    except KeyboardInterrupt:
        logger.info("Exiting...")
        producer.shutdown()
