from __future__ import annotations
import json
import os


DATASET_PATH = os.getenv("DATASET_PATH", "dataset/raw_data_medium-utv_sorted.csv")

CONF_PATH = os.getenv("CONF_PATH", "conf.json")


class KafkaConfig:
    def __init__(self):
        with open(CONF_PATH, "r") as f:
            data = json.load(f)
            self.config = {"bootstrap.servers": data["broker"]}
            self.topic = data["topicMonitoring"]
