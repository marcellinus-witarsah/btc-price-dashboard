from datetime import datetime
from kafka import KafkaProducer
from src.logger import logger


class TradeProducer:
    def __init__(self, topic, **configs):
        self.topic = topic
        self.producer = KafkaProducer(**configs)

    def produce_trade(self, trade_data):
        record = self.producer.send(
            self.topic,
            key=datetime.fromtimestamp(trade_data.timestamp).date(),
            value=trade_data.to_dict(),
        )
        logger.info(
            f"Produced message: for key: {trade_data.trade_id} offset of : {record.get().offset}"
        )

    def shutdown(self):
        self.producer.flush()
        self.producer.close()
