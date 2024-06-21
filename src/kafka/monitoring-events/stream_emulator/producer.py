from loguru import logger
from confluent_kafka import Producer
from config import KafkaConfig


class KafkaProducer:
    def __init__(self):
        conf = KafkaConfig()
        producer = Producer(conf.config)
        self.producer = producer
        self.topic = conf.topic

    def produce_event(self, event: bytes) -> None:
        try:
            self.producer.produce(self.topic, value=event)
        except Exception as e:
            logger.error(f"Exception in producing event: {e}")
            exit(1)

    def flush(self) -> None:
        self.producer.flush()
