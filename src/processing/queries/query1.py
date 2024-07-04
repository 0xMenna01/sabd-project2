from __future__ import annotations
import math
from typing import Iterable
from pyflink.datastream import DataStream
from pyflink.datastream.functions import (
    AggregateFunction,
    ProcessWindowFunction,
)
from pyflink.datastream.window import (
    TimeWindow,
    WindowAssigner,
)
from typing import Tuple
from .executor import QueryExecutor


# (count, mean, M2)
WelfordStats = Tuple[int, float, float]
# (vault_id, temperature)
VaultTemperature = Tuple[int, int]
# (timestamp, vault_id, count, mean, stddev)
VaultTemperatureStats = Tuple[int, int, int, float, float]


class QueryOneExecutor(QueryExecutor):
    def __init__(self, data: DataStream):
        """
        Query 1.

        param data: stream of Row [timestamp, serial_number, model, failure, vault_id, s9_power_on_hours, s194_temperature_celsius]
        return: List containing the three windowed resulting streams along with their names.
        """
        self.partial_vaults_stream = (
            data.filter(lambda x: 1000 <= x.vault_id <= 1020)
            .map(lambda x: (x.vault_id, x.s194_temperature_celsius))
            .key_by(lambda x: x[0])
        )
        self.window: WindowAssigner | None = None

    def window_assigner(self, window: WindowAssigner) -> QueryOneExecutor:
        self.window = window
        return self

    def execute(self) -> DataStream:
        if self.window is None:
            raise ValueError("Window assigner not set")

        return self.partial_vaults_stream.window(self.window).aggregate(
            WelfordOnlineAggregateFunction(), ComputeStddev()
        )


class WelfordOnlineAggregateFunction(AggregateFunction):

    def create_accumulator(self) -> WelfordStats:
        return (0, 0.0, 0.0)  # count, mean, M2

    def add(
        self, value: VaultTemperature, accumulator: WelfordStats
    ) -> tuple[int, float, float]:
        count, mean, M2 = accumulator
        temperature = value[1]

        count += 1
        delta = temperature - mean
        mean += delta / count
        delta2 = temperature - mean
        M2 += delta * delta2

        return (count, mean, M2)

    def merge(self, a: WelfordStats, b: WelfordStats) -> WelfordStats:
        count_a, mean_a, M2_a = a
        count_b, mean_b, M2_b = b

        count = count_a + count_b
        delta = mean_b - mean_a
        mean = (mean_a + mean_b) / 2
        M2 = M2_a + M2_b + delta**2 * count_a * count_b / count

        return (count, mean, M2)

    def get_result(self, accumulator: WelfordStats) -> WelfordStats:
        return accumulator


class ComputeStddev(ProcessWindowFunction):

    def process(
        self,
        key: int,
        context: ProcessWindowFunction.Context,
        stats: Iterable[WelfordStats],
    ) -> Iterable[VaultTemperatureStats]:
        count, mean, M2 = next(iter(stats))
        if count < 2:
            stddev = float("nan")
        else:
            variance = M2 / (count - 1)  # sample variance
            stddev = math.sqrt(variance)

        window: TimeWindow = context.window()

        yield (window.start, key, count, mean, stddev)
