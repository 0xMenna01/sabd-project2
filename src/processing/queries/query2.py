from __future__ import annotations
from typing import Iterable, List, Tuple
from pyflink.datastream import DataStream
from pyflink.datastream.functions import (
    ProcessWindowFunction,
    ReduceFunction,
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

# (lowerboud_failures_count, [(vault_id, failures_count, list_of_models_and_serial_numbers)])
# Used for accumulating the top 10 vaults with the most failures within the same day.
VaultsRankingFailuresAcc = Tuple[int, List[Tuple[int, int, List[str]]]]

TimestampedRankingFailures = Tuple[int, List[Tuple[int, int, List[str]]]]


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
                    (x.timestamp, x.vault_id),
                    1,
                    list([x.model, x.serial_number]),
                )
            )
            .key_by(lambda x: x[0])
            # Count the number of failures per vault per day and accumulate the models and serial numbers.
            .reduce(
                lambda x, y: (
                    x[0],  # (timestamp, vault_id)
                    x[1] + y[1],  # failures_count
                    x[2] + y[2],  # list of models and serial numbers
                ),
            )
            # (vault_id, failures_count_per_day, list_of_models_and_serial_numbers)
            .map(lambda x: (x[0][1], x[1], x[2]))
        )

        self.window: WindowAssigner | None = None

    def window_assigner(self, window: WindowAssigner) -> QueryTwoExecutor:
        self.window = window
        return self

    def execute(self) -> DataStream:
        if self.window is None:
            raise ValueError("Window assigner not set")

        return (
            # (vault_id, failures_count_per_day, list_of_models_and_serial_numbers)
            self.partial_vaults_failures_stream.window_all(self.window).aggregate(
                DailyFailuresRankingAggregateFunction()
            )
        )


class DailyFailuresRankingAggregateFunction(AggregateFunction):

    def create_accumulator(self) -> VaultsRankingFailuresAcc:
        return (0, list([]))

    def add(
        self, value: DailyVaultFailures, acc: VaultsRankingFailuresAcc
    ) -> VaultsRankingFailuresAcc:
        vault_id, failures_count, faulty_disks = value
        failures_lowerbound, vaults_ranking = acc

        if failures_count < failures_lowerbound and len(vaults_ranking) == TOP_FAILURES:
            return acc

        vaults_ranking += list([(vault_id, failures_count, faulty_disks)])
        vaults_ranking.sort(key=lambda x: x[1], reverse=True)

        failures_lowerbound = min(failures_lowerbound, failures_count)
        if len(vaults_ranking) > TOP_FAILURES:
            vaults_ranking.pop()

        return (failures_lowerbound, vaults_ranking)

    def merge(
        self, a: VaultsRankingFailuresAcc, b: VaultsRankingFailuresAcc
    ) -> VaultsRankingFailuresAcc:
        failures_lowerbound_a, vaults_ranking_a = a
        failures_lowerbound_b, vaults_ranking_b = b

        vaults_ranking = vaults_ranking_a + vaults_ranking_b
        vaults_ranking.sort(key=lambda x: x[1], reverse=True)
        vaults_ranking = vaults_ranking[:TOP_FAILURES]

        failures_lowerbound = min(failures_lowerbound_a, failures_lowerbound_b)

        return (failures_lowerbound, vaults_ranking)

    def get_result(
        self, accumulator: VaultsRankingFailuresAcc
    ) -> VaultsRankingFailuresAcc:
        return accumulator


class ProcessVaultsRanking(ProcessWindowFunction):

    def process(
        self,
        key,
        context: ProcessWindowFunction.Context,
        elements: Iterable[VaultsRankingFailuresAcc],
    ) -> Iterable[TimestampedRankingFailures]:
        _, ranking = next(iter(elements))
        window: TimeWindow = context.window()

        yield (window.start, ranking)
