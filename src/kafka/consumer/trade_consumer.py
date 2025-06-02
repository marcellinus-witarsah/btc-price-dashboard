from kafka import KafkaConsumer


class TradeConsumer:
    def __init__(self, *topics, **configs):
        """Initialize the Kafka consumer with the given topics and configurations."""
        self.consumer = KafkaConsumer(*topics, **configs)

    def consume_trade(self):
        """Consume messages from the Kafka topic."""
        record = self.consumer.poll(timeout_ms=1000)
        for _, messages in record.items():
            for message in messages:
                yield message

    def shutdown(self):
        """Shutdown the Kafka consumer."""
        self.consumer.close()
