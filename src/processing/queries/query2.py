import json
import math
from typing import Iterable
from pyflink.datastream import DataStream, StreamExecutionEnvironment
from pyflink.datastream.functions import (
    AggregateFunction,
    ProcessWindowFunction,
    ReduceFunction,
    MapFunction,
)
from pyflink.common import Row, Types
from pyflink.datastream.window import (
    Time,
    TumblingEventTimeWindows,
    WindowAssigner,
    TimeWindow,
)
from pyflink.datastream.functions import (
    ProcessFunction,
    RuntimeContext,
    KeyedProcessFunction,
)
from pyflink.datastream.state import ValueStateDescriptor
from typing import List, Tuple


TOP_FAILURES = 10


WINDOWS: List[Tuple[str, WindowAssigner]] = [
    ("query2_day", TumblingEventTimeWindows.of(Time.days(1))),
    ("query2_3days", TumblingEventTimeWindows.of(Time.days(3))),
    (
        "query1_from_start",
        TumblingEventTimeWindows.of(Time.days(23)),
    ),
]


class FailuresRankingReduceFunction(ReduceFunction):

    def reduce(
        self,
        acc: Tuple[int, int, List[Tuple[int, int, List[str]]]],
        value: Tuple[int, int, List[Tuple[int, int, List[str]]]],
    ) -> Tuple[int, int, List[Tuple[int, int, List[str]]]]:
        ts, lower_bound, rank_list = acc
        _, failures_count, vault_failures = value

        if failures_count < lower_bound and len(rank_list) == TOP_FAILURES:
            return acc

        rank_list += vault_failures
        rank_list.sort(key=lambda x: x[1], reverse=True)

        lower_bound = min(lower_bound, failures_count)
        if len(rank_list) > TOP_FAILURES:
            rank_list.pop()

        return (ts, lower_bound, rank_list)


class WindowTimestamp(ProcessWindowFunction):

    def process(
        self,
        key: int,
        context: ProcessWindowFunction.Context,
        values: Iterable[tuple[int, int, List[str]]],
    ) -> Iterable[Row]:

        value = next(iter(values))

        window: TimeWindow = context.window()
        yield Row(
            start=window.start,
            vault_id=value[0],
            failures_count=value[1],
            disks=value[2],
        )


def query(data: DataStream) -> List[DataStream]:
    """
    Query 2.

    param data: stream of Row [timestamp, serial_number, model, failure, vault_id, s9_power_on_hours, s194_temperature_celsius]
    return: List containing the three windowed resulting streams along with their names.
    """
    vaults_failures_stream = (
        data.filter(lambda x: x.failure == True)
        .map(lambda x: (x.vault_id, 1, list([x.model, x.serial_number])))
        .key_by(lambda x: x[0])
    )

    result_windowed_streams: List[DataStream] = []
    for name, window in WINDOWS:
        windowed_result = (
            vaults_failures_stream.window(window)
            .reduce(
                lambda x, y: (
                    x[0],  # vault_id
                    x[1] + y[1],  # failures_count
                    x[2] + y[2],  # list of models and serial numbers
                ),
                WindowTimestamp(),  # Add window start timestamp
            )
            .map(
                lambda x: (
                    x.start,
                    x.failures_count,
                    [(x.vault_id, x.failures_count, x.disks)],
                )
            )
            .key_by(lambda x: x.start)
            .reduce(FailuresRankingReduceFunction())
            # (timestamp, [(vault_id, failures_count, list_of_models_and_serial_numbers)])
            .map(lambda x: (x[0], x[2]))
            .name(name)
        )

        result_windowed_streams.append(windowed_result)

    return result_windowed_streams
