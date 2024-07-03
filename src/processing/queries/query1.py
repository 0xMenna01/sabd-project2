import json
import math
from typing import Iterable
from pyflink.datastream import DataStream, StreamExecutionEnvironment
from pyflink.datastream.functions import (
    AggregateFunction,
    ProcessWindowFunction,
    ReduceFunction,
)
from pyflink.common import Row, Types
from pyflink.datastream.window import (
    Time,
    TumblingEventTimeWindows,
    TimeWindow,
    WindowAssigner,
)
from pyflink.datastream.window import GlobalWindows
from pyflink.datastream.connectors.kafka import FlinkKafkaProducer
from typing import List, Tuple
from loguru import logger

WINDOWS: List[Tuple[str, WindowAssigner]] = [
    ("query1_day", TumblingEventTimeWindows.of(Time.days(1))),
    ("query1_3days", TumblingEventTimeWindows.of(Time.days(3))),
    (
        "query1_from_start",
        TumblingEventTimeWindows.of(Time.days(23)),
    ),
]


class WelfordOnlineAggregateFunction(AggregateFunction):

    def create_accumulator(self):
        return (0, 0.0, 0.0)  # count, mean, M2

    def add(
        self, value: tuple[int, int], accumulator: tuple[int, float, float]
    ) -> tuple[int, float, float]:
        count, mean, M2 = accumulator
        count += 1
        delta = value[1] - mean
        mean += delta / count
        delta2 = value[1] - mean
        M2 += delta * delta2

        return (count, mean, M2)

    def merge(
        self, a: tuple[int, float, float], b: tuple[int, float, float]
    ) -> tuple[int, float, float]:
        count_a, mean_a, M2_a = a
        count_b, mean_b, M2_b = b

        count = count_a + count_b
        delta = mean_b - mean_a
        mean = (mean_a + mean_b) / 2
        M2 = M2_a + M2_b + delta**2 * count_a * count_b / count

        return (count, mean, M2)

    def get_result(self, accumulator):
        return accumulator


class ComputeStddev(ProcessWindowFunction):

    def process(
        self,
        key: int,
        context: ProcessWindowFunction.Context,
        stats: Iterable[tuple[int, float, float]],
    ):
        count, mean, M2 = next(iter(stats))
        if count < 2:
            stddev = float("nan")
        else:
            variance = M2 / (count - 1)  # sample variance
            stddev = math.sqrt(variance)

        window: TimeWindow = context.window()

        yield (window.start, key, count, mean, stddev)


def query(data: DataStream) -> List[DataStream]:
    """
    Query 1.

    param data: stream of Row [timestamp, serial_number, model, failure, vault_id, s9_power_on_hours, s194_temperature_celsius]
    return: List containing the three windowed resulting streams along with their names.
    """
    vaults_stream = (
        data.filter(lambda x: 1000 <= x.vault_id <= 1020)
        .map(lambda x: (x.vault_id, x.s194_temperature_celsius, 1))
        .key_by(lambda x: x[0])
    )

    result_windowed_streams: List[DataStream] = []
    for name, window in WINDOWS:
        windowed_result = (
            vaults_stream.window(window)
            .aggregate(WelfordOnlineAggregateFunction(), ComputeStddev())
            .name(name)
        )

        result_windowed_streams.append(windowed_result)

    return result_windowed_streams
