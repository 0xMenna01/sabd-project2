from loguru import logger
from kafka import KafkaConsumer
import json
import write_utils


class Consumer:
    def __init__(self, name: str):
        self._consumer: KafkaConsumer = KafkaConsumer(
            "query1_day",
            "query1_3days",
            "query1_global",
            bootstrap_servers="kafka-broker:9092",
            group_id=name,
            auto_offset_reset="earliest",
        )
        logger.info(f"Consumer initialized with name: {name}")

    def consume(self):
        logger.info("Starting to consume query results")
        for msg in self._consumer:
            event_res = tuple(json.loads(msg.value))
            logger.info(f"Consumed message from topic {msg.topic}: {event_res}")
            write_utils.write_csv_from(msg.topic, event_res)
