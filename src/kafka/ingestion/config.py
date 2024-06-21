from __future__ import annotations
import json
import os


CONF_PATH = os.getenv("CONF_PATH", "conf.json")


class KafkaConfig:
    def __init__(self):
        with open(CONF_PATH, "r") as f:
            data = json.load(f)
            self.broker = data["broker"]
            self.topic_src = data["topicMonitoring"]
            self.topic_dest = data["topicIngestion"]

    @property
    def broker_url(self):
        return f"kafka://{self.broker}"
