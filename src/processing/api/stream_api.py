from __future__ import annotations
from enum import Enum
import json
from loguru import logger
from pyflink.common.typeinfo import Types
from pyflink.common import WatermarkStrategy
from pyflink.common import Row
from utils.flink_utils import (
    CustomTimestampAssigner,
)
from utils.flink_utils import (
    FlinkEnvironmentBuilder,
    ThroughputEvaluator,
    build_query1_local_sink,
)
from preprocessing import preprocess
from utils.flink_utils import JsonEventToRowFromFaust
from queries import query1


class QueryNum(Enum):
    ONE = 1
    TWO = 2
    THREE = 3


class StreamingApi:

    def __init__(
        self,
        query: QueryNum,
        is_preprocessed: bool,
        evaluation: bool,
        write_locally: bool,
    ):
        flink_env = FlinkEnvironmentBuilder(is_preprocessed, evaluation).build()

        self._stream = flink_env.add_kafka_source()
        self._kafka_sink = flink_env.kafka_sink()
        self._env = flink_env.env
        self._query = query
        self._evaluation = evaluation
        self._write_locally = write_locally
        self.is_stream_prepared = False
        self._result_streams = None
        self._is_preprocessed = is_preprocessed

    def prepare_stream(self) -> StreamingApi:
        if self._is_preprocessed:
            # Stream is being preprocessed by Faust
            stream = self._stream.map(JsonEventToRowFromFaust())
        else:
            # Stream is preprocessed using Flink
            stream = preprocess.execute(self._stream)

        self._stream = stream.assign_timestamps_and_watermarks(
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

        return self

    def sink_and_execute(self) -> None:
        assert self._result_streams is not None, "Query not executed"

        for stream in self._result_streams:
            query_name = stream.get_name()
            if self._evaluation:
                stream = stream.map(ThroughputEvaluator())

            if self._write_locally:
                # Write results to a csv local file
                local_sink = build_query1_local_sink(query_name)
                stream.sink_to(local_sink)

            # Convert to JSON string for Kafka
            json_stream = stream.map(lambda x: (query_name, x)).map(
                lambda x: json.dumps(x), output_type=Types.STRING()
            )
            json_stream.add_sink(self._kafka_sink)

            if self._evaluation:
                self._env.execute_async(query_name)
        if not self._evaluation:
            self._env.execute(f"Query_{self._query.name}")