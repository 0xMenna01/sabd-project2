import json
import pandas as pd
from loguru import logger
from confluent_kafka import Producer
from utils import KafkaConfig


class KafkaProducer:
    def __init__(self):
        conf = KafkaConfig()
        producer = Producer(conf.config)
        self.producer = producer
        self.topic = conf.topic

    def produce_event(self, event: bytes) -> None:
        try:
            self.producer.produce(self.topic,
                                  value=event,
                                  callback=delivery_report)
        except Exception as e:
            logger.error(f"Exception in producing event: {e}")

    def flush(self) -> None:
        self.producer.flush()

    def free(self) -> None:
        self.producer.flush()
        self.producer.close()


# Asyncronous callback for message delivery
def delivery_report(err, msg):
    if err is not None:
        logger.error(f'Message delivery failed: {err}')
    else:
        logger.info(f'Message delivered to {msg.topic()} [{msg.partition()}]')
