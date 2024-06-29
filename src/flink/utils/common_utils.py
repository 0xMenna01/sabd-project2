from __future__ import annotations
import json
import os
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema


CONF_PATH = os.getenv("CONF_PATH", "conf.json")


class KafkaConfig:
    def __init__(self):
        with open(CONF_PATH, "r") as f:
            data = json.load(f)
            self.broker = data["broker"]
            # Topic in which streams are ingested.
            self.src_topic = data["topicIngestion"]
            # Topic where results may be optionally stored.
            self.sink_topic = data["topicSink"]

    @property
    def broker_url(self):
        return f"kafka://{self.broker}"
