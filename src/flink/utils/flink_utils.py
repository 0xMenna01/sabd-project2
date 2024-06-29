from pyflink.datastream import (
    StreamExecutionEnvironment,
    RuntimeExecutionMode,
    TimeCharacteristic,
    DataStream,
)
from utils.common_utils import KafkaConfig
from pyflink.datastream.connectors.kafka import FlinkKafkaConsumer, FlinkKafkaProducer
from pyflink.common.serialization import SimpleStringSchema
from pyflink.common.watermark_strategy import TimestampAssigner
import json
from pyflink.common import Row
from pyflink.datastream.functions import MapFunction, SinkFunction


class FlinkEnvironment:
    def __init__(
        self, env: StreamExecutionEnvironment, kafka_conf: KafkaConfig
    ) -> None:
        self._env = env
        self._kafka_conf = {
            "bootstrap.servers": kafka_conf.broker,
            "group.id": "flink-group",
        }
        self._consumer_topic = kafka_conf.src_topic
        self._producer_topic = kafka_conf.sink_topic

    def add_kafka_source(self) -> DataStream:
        kafka_consumer = FlinkKafkaConsumer(
            topics=self._consumer_topic,
            deserialization_schema=SimpleStringSchema(),
            properties=self._kafka_conf,
        )
        kafka_consumer.set_start_from_earliest()

        return self._env.add_source(kafka_consumer).set_parallelism(1)

    def kafka_sink(self) -> FlinkKafkaProducer:
        return FlinkKafkaProducer(
            topic=self._producer_topic,
            serialization_schema=SimpleStringSchema(),
            producer_config=self._kafka_conf,
        )

    @property
    def env(self) -> StreamExecutionEnvironment:
        return self._env


class FlinkEnvironmentBuilder:
    def __init__(self, evaluation: bool) -> None:
        self.kafka_config = KafkaConfig()
        self.evaluation = evaluation

    def build(self) -> FlinkEnvironment:
        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
        env.set_parallelism(1)
        env.set_stream_time_characteristic(TimeCharacteristic.EventTime)
        if self.evaluation:
            env.get_config().set_latency_tracking_interval(100)

        return FlinkEnvironment(env, self.kafka_config)


class JsonEventToRow(MapFunction):
    def map(self, value: str) -> Row:
        json_data = json.loads(value)

        return Row(
            timestamp=json_data["timestamp"],
            serial_number=json_data["serial_number"],
            model=json_data["model"],
            failure=json_data["failure"],
            vault_id=json_data["vault_id"],
            s9_power_on_hours=json_data["s9_power_on_hours"],
            s194_temperature_celsius=json_data["s194_temperature_celsius"],
        )


class CustomTimestampAssigner(TimestampAssigner):

    def extract_timestamp(self, value: Row, record_timestamp: int) -> int:
        return value.timestamp  # type: ignore


from typing import Any
from pyflink.datastream.functions import MapFunction, RuntimeContext
import datetime
from typing import Callable


class ThroughputEvaluator(MapFunction):
    def __init__(self):
        # Number of records processed.
        self._count = 0
        # tuples per second
        self._throughput = 0
        # Start time of the evaluation.
        self._start = 0

    def open(self, runtime_context: RuntimeContext):
        self._count = 0
        self._throughput = 0
        self._start = datetime.datetime.now().timestamp()
        runtime_context.get_metrics_group().gauge(
            "throughput", lambda: self._throughput * 100000
        )

    def map(self, value):
        self._count += 1
        current_timestamp = datetime.datetime.now().timestamp()
        elapsed_time = current_timestamp - self._start

        self.throughput = self._count / elapsed_time

        return value
