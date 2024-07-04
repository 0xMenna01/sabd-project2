from __future__ import annotations
from enum import Enum
import json
from typing import List, Tuple
from pyflink.common.typeinfo import Types
from pyflink.common import WatermarkStrategy
from pyflink.common import Row
from utils.flink_utils import CustomTimestampAssigner
from utils.flink_utils import (
    FlinkEnvironmentBuilder,
    ThroughputEvaluator,
)
from pyflink.datastream.window import (
    Time,
    TumblingEventTimeWindows,
    WindowAssigner,
)
from preprocessing import preprocess
from utils.flink_utils import JsonEventToRowFromFaust
from queries.query1 import QueryOneExecutor
from queries.query2 import QueryTwoExecutor
from queries.executor import QueryExecutor


class QueryNum(Enum):
    ONE = 1
    TWO = 2


WindowData = List[Tuple[Tuple[str, str], WindowAssigner]]


WINDOWS: WindowData = [
    (("query1_day", "query2_day"), TumblingEventTimeWindows.of(Time.days(1))),
    (("query1_3days", "query2_3day"), TumblingEventTimeWindows.of(Time.days(3))),
    (
        ("query1_from_start", "query2_from_start"),
        TumblingEventTimeWindows.of(Time.days(23)),
    ),
]


class StreamingApi:

    def __init__(
        self,
        query: QueryNum,
        is_preprocessed: bool,
        evaluation: bool,
    ):
        flink_env = FlinkEnvironmentBuilder(is_preprocessed, evaluation).build()

        self._stream = flink_env.add_kafka_source()
        self._flink_env = flink_env
        self._query = query
        self._evaluation = evaluation
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

    def execute_query(self) -> StreamingApi:
        assert self.is_stream_prepared, "Stream not prepared"

        window_index = 0
        query_exec: QueryExecutor

        if self._query == QueryNum.ONE:
            query_exec = QueryOneExecutor(self._stream)
        elif self._query == QueryNum.TWO:
            query_exec = QueryTwoExecutor(self._stream)
            window_index = 1

        for query_names, window in WINDOWS:
            name = query_names[window_index]
            # Assign window and execute query
            res_stream = query_exec.window_assigner(window).execute().name(name)

            kafka_sink = self._flink_env.kafka_sink(name)
            if self._evaluation:
                # Evaluate throughput
                res_stream = res_stream.map(ThroughputEvaluator())
            # Convert into JSON for Kafka and add sink
            res_stream.map(
                lambda x: json.dumps(x), output_type=Types.STRING()
            ).add_sink(kafka_sink)

            if self._evaluation:
                self._flink_env.env.execute_async(name)

        if not self._evaluation:
            self._flink_env.env.execute(f"query_{self._query.name}")

        return self
