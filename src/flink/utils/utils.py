import json
from .config import KafkaConfig
from pyflink.datastream.connectors.kafka import KafkaSource, KafkaOffsetsInitializer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common import Row


def build_kafka_source() -> KafkaSource:
    kafka_conf = KafkaConfig()
    return KafkaSource \
        .builder() \
        .set_bootstrap_servers(kafka_conf.broker) \
        .set_group_id("flink_group") \
        .set_topics(kafka_conf.src_topic) \
        .set_value_only_deserializer(SimpleStringSchema()) \
        .set_starting_offsets(KafkaOffsetsInitializer.earliest()) \
        .build()


def json_event_to_row(json_str: str) -> Row:
    json_data = json.loads(json_str)
    DiskEvent = Row("timestamp", "serial_number", "model", "failure",
                    "vault_id", "s9_power_on_hours", "s194_temperature_celsius")

    return DiskEvent(
        json_data[DiskEvent[0]],
        json_data[DiskEvent[1]],
        json_data[DiskEvent[2]],
        json_data[DiskEvent[3]],
        json_data[DiskEvent[4]],
        json_data[DiskEvent[5]],
        json_data[DiskEvent[6]]
    )
