from __future__ import annotations
import json
import os
import pandas as pd


DATASET_PATH = os.getenv(
    "DATASET_PATH", "dataset/raw_data_medium-utv_sorted.csv")


class KafkaConfig:
    def __init__(self):
        with open(os.path.join(os.path.dirname(__file__), "../../conf/kafka-config.json"), "r") as f:
            data = json.load(f)
            self.config = {"bootstrap.servers": data["broker"]}
            self.topic = data["topicMonitoring"]


def read_df_from_csv() -> pd.DataFrame:
    return pd.read_csv(DATASET_PATH)
