from __future__ import annotations
import json
import os


CONF_PATH = os.getenv("CONF_PATH", "conf.json")


class KafkaConfig:
    def __init__(self, faust_topic: bool):
        with open(CONF_PATH, "r") as f:
            data = json.load(f)
            self.broker = data["broker"]
            if faust_topic:
                self.src_topic = data["topicFaust"]
            else:
                self.src_topic = data["topicMonitoring"]

    @property
    def broker_url(self):
        return f"kafka://{self.broker}"
