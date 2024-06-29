import json
import math
from typing import Iterable
from pyflink.datastream import DataStream, StreamExecutionEnvironment
from pyflink.datastream.functions import AggregateFunction, ProcessWindowFunction
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


WINDOWS: List[Tuple[str, WindowAssigner]] = [
    ("query1_day", TumblingEventTimeWindows.of(Time.days(1))),
    ("query1_3days", TumblingEventTimeWindows.of(Time.days(3))),
    (
        "query1_global",
        GlobalWindows(),
    ),
]


class TemperatureAggregate(AggregateFunction):
    def create_accumulator(self):
        return (0, 0.0, 0.0)  # count, sum, sum of squares

    def add(self, value, accumulator):
        count, sum, sum_of_squares = accumulator
        count += 1
        sum += value
        sum_of_squares += value * value
        return (count, sum, sum_of_squares)

    def get_result(self, accumulator):
        return accumulator

    def merge(self, a, b):
        return (a[0] + b[0], a[1] + b[1], a[2] + b[2])


class ComputeStats(ProcessWindowFunction):

    def process(
        self,
        vault_id: int,
        context: ProcessWindowFunction.Context,
        stats: Iterable[tuple[int, int, int]],
    ):
        count, sum, sum_of_squares = next(iter(stats))
        mean = sum / count
        variance = (sum_of_squares / count) - (mean * mean)
        stddev = math.sqrt(variance) if count > 1 else 0.0

        window: TimeWindow = context.window()

        yield Row(window.start, vault_id, count, mean, stddev)


def query(data: DataStream) -> List[DataStream]:
    """
    Query 1.

    param data: stream of Row [timestamp, serial_number, model, failure, vault_id, s9_power_on_hours, s194_temperature_celsius]
    return: List containing the three windowed resulting streams along with their names.
    """
    vaults_stream = (
        data.filter(lambda x: 1000 <= x.vault_id <= 1020)
        .map(lambda x: Row(x.timestamp, x.vault_id, x.s194_temperature_celsius))
        .key_by(lambda x: x.vault_id)
    )

    result_windowed_streams: List[DataStream] = []
    for name, window in WINDOWS:
        windowed_result = (
            vaults_stream.window(window)
            .aggregate(TemperatureAggregate(), ComputeStats())
            .name(name)
        )

        result_windowed_streams.append(windowed_result)

    return result_windowed_streams
