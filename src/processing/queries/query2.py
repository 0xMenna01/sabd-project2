from __future__ import annotations
from typing import Iterable, List, Tuple
from pyflink.datastream import DataStream
from pyflink.datastream.functions import (
    ProcessAllWindowFunction,
    AggregateFunction,
)
from pyflink.datastream.window import (
    Time,
    TumblingEventTimeWindows,
    WindowAssigner,
    TimeWindow,
)
from .executor import QueryExecutor


TOP_FAILURES = 10


# (vault_id, failures_count_per_day, list_of_models_and_serial_numbers)
DailyVaultFailures = Tuple[int, int, List[str]]

# List[(vault_id, failures_count, list_of_models_and_serial_numbers)])
# Used for accumulating the top 10 vaults with the most failures within the same day.
VaultsRankingFailures = List[Tuple[int, int, List[str]]]

TimestampedRankingFailures = Tuple[int, VaultsRankingFailures]


class QueryTwoExecutor(QueryExecutor):
    def __init__(self, data: DataStream):
        """
        Query 1.

        param data: stream of Row [timestamp, serial_number, model, failure, vault_id, s9_power_on_hours, s194_temperature_celsius]
        return: List containing the three windowed resulting streams along with their names.
        """
        self.partial_vaults_failures_stream = (
            data.filter(lambda x: x.failure == True)
            .map(
                lambda x: (
                    x.vault_id,
                    1,
                    list([x.model, x.serial_number]),
                )
            )
            .key_by(lambda x: x[0])
            .window(TumblingEventTimeWindows.of(Time.days(1)))
            # Count the number of vault's failures per day and accumulate the models and serial numbers.
            .reduce(
                lambda x, y: (
                    x[0],  # vault_id
                    x[1] + y[1],  # failures_count
                    x[2] + y[2],  # list of models and serial numbers
                ),
            )
        )

        self.window: WindowAssigner | None = None

    def window_assigner(self, window: WindowAssigner) -> QueryTwoExecutor:
        self.window = window
        return self

    def execute(self) -> DataStream:
        if self.window is None:
            raise ValueError("Window assigner not set")

        return (
            # (vault_id, failures_count_in_one_day, list_of_models_and_serial_numbers)
            self.partial_vaults_failures_stream.window_all(self.window).aggregate(
                DailyFailuresRankingAggregateFunction(), TimestampForVaultsRanking()
            )
        )


class DailyFailuresRankingAggregateFunction(AggregateFunction):

    def create_accumulator(self) -> VaultsRankingFailures:
        return list([])

    def add(
        self, value: DailyVaultFailures, ranking: VaultsRankingFailures
    ) -> VaultsRankingFailures:
        vault_id, failures_count, faulty_disks = value

        ranking.append((vault_id, failures_count, faulty_disks))
        ranking.sort(key=lambda x: x[1], reverse=True)
        ranking = ranking[:TOP_FAILURES]

        return ranking

    def merge(
        self, ranking_a: VaultsRankingFailures, ranking_b: VaultsRankingFailures
    ) -> VaultsRankingFailures:

        ranking = ranking_a + ranking_b
        ranking.sort(key=lambda x: x[1], reverse=True)
        ranking = ranking[:TOP_FAILURES]

        return ranking

    def get_result(self, accumulator: VaultsRankingFailures) -> VaultsRankingFailures:
        return accumulator


class TimestampForVaultsRanking(ProcessAllWindowFunction):

    def process(
        self,
        context: ProcessAllWindowFunction.Context,
        elements: Iterable[VaultsRankingFailures],
    ) -> Iterable[TimestampedRankingFailures]:
        ranking = next(iter(elements))
        window: TimeWindow = context.window()

        yield (window.start, ranking)
