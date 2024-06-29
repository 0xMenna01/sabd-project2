from __future__ import annotations
from enum import Enum

from pyflink.common.typeinfo import Types
from pyflink.common import WatermarkStrategy
from pyflink.common import Row
from utils.flink_utils import (
    JsonEventToRow,
    CustomTimestampAssigner,
)
from utils.flink_utils import FlinkEnvironmentBuilder, ThroughputEvaluator
from pyflink.datastream import DataStream
from queries import query1


class QueryNum(Enum):
    ONE = 1
    TWO = 2
    THREE = 3


class StreamingApi:

    def __init__(
        self,
        query: QueryNum,
        evaluation: bool,
    ):
        flink_env = FlinkEnvironmentBuilder(evaluation).build()
        self._stream = flink_env.add_kafka_source()
        self._kafka_sink = flink_env.kafka_sink()
        self._env = flink_env.env
        self._query = query
        self._evaluation = evaluation
        self.is_stream_prepared = False
        self._result_streams = None

    def prepare_stream(self) -> StreamingApi:
        self._stream = self._stream.map(
            JsonEventToRow(),
        ).assign_timestamps_and_watermarks(
            WatermarkStrategy.for_monotonous_timestamps().with_timestamp_assigner(
                CustomTimestampAssigner()
            )
        )

        self.is_stream_prepared = True

        return self

    def query(self) -> StreamingApi:
        assert self.is_stream_prepared, "Stream not prepared"
        if self._query == QueryNum.ONE:
            self._result_streams = query1.query(self._stream)
        elif self._query == QueryNum.TWO:
            pass
        elif self._query == QueryNum.THREE:
            pass

    def execute(self):
        assert self._result_streams is not None, "Query not executed"
        for name, stream in self._result_streams:
            stream.map(ThroughputEvaluator()).name(name).add_sink(self._kafka_sink)

            if self._evaluation:
                self._env.execute_async(name)

        if not self._evaluation:
            self._env.execute(f"Query_{self._query.name}")
