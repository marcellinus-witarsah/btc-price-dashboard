from datetime import datetime
from kafka import KafkaConsumer
from src.logger import logger


class TradeConsumer:
    def __init__(self, *topics, **configs):
        self.consumer = KafkaConsumer(*topics, **configs)

    def consume_trade(self):
        record = self.consumer.poll(timeout_ms=1000)
        for topic_partition, messages in record.items():
            for message in messages:
                yield message
                
    def shutdown(self):
        self.consumer.close()
