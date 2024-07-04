from __future__ import annotations
from typing import Iterable, List, Tuple
from pyflink.datastream import DataStream
from pyflink.datastream.functions import (
    ProcessWindowFunction,
    ReduceFunction,
)
from pyflink.datastream.window import (
    Time,
    TumblingEventTimeWindows,
    WindowAssigner,
    TimeWindow,
)
from .executor import QueryExecutor


TOP_FAILURES = 10


# ((day, vault_id), failures_count, list_of_models_and_serial_numbers)
DailyVaultFailures = Tuple[Tuple[int, int], int, List[str]]

# (ts, lowerboud_failures_count, [(vault_id, failures_count, list_of_models_and_serial_numbers)])
# Used for accumulating the top 10 vaults with the most failures within the same day.
RankingDailyVaultsFailures = Tuple[int, int, List[Tuple[int, int, List[str]]]]


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
                    list([]),
                )
            )
            .key_by(lambda x: x[0])
        )
        self.window: WindowAssigner | None = None

    def window_assigner(self, window: WindowAssigner) -> QueryTwoExecutor:
        self.window = window
        return self

    def execute(self) -> DataStream:
        if self.window is None:
            raise ValueError("Window assigner not set")

        return (
            self.partial_vaults_failures_stream.window(self.window)
            .reduce(
                lambda x, y: (
                    x[0],  # (timestamp, vault_id)
                    x[1] + y[1],  # failures_count
                    x[2] + y[2],  # list of models and serial numbers
                ),
                ProcessDailyVaultFailures(),
            )
            .key_by(lambda x: x[0])  # timestamp
            .reduce(DailyFailuresRankingReduceFunction())
            .map(
                lambda x: (x[0], x[2])
            )  # (ts, List[(vault_id, failures_count, list_of_models_and_serial_numbers)])
        )


class DailyFailuresRankingReduceFunction(ReduceFunction):

    def reduce(
        self,
        acc: RankingDailyVaultsFailures,
        value: RankingDailyVaultsFailures,
    ) -> RankingDailyVaultsFailures:
        ts, failures_lowerbound, vaults_ranking = acc
        _, failures_count, vault_failures = value

        # the accumulator's `failures_count` is the lowerbound within the `rank_list`
        if failures_count < failures_lowerbound and len(vaults_ranking) == TOP_FAILURES:
            return acc

        vaults_ranking += vault_failures
        vaults_ranking.sort(key=lambda x: x[1], reverse=True)

        failures_lowerbound = min(failures_lowerbound, failures_count)
        if len(vaults_ranking) > TOP_FAILURES:
            vaults_ranking.pop()

        return (ts, failures_lowerbound, vaults_ranking)


class ProcessDailyVaultFailures(ProcessWindowFunction):

    def process(
        self,
        key: int,
        context: ProcessWindowFunction.Context,
        values: Iterable[DailyVaultFailures],
    ) -> Iterable[RankingDailyVaultsFailures]:

        value = next(iter(values))

        window: TimeWindow = context.window()
        # (ts, failures_count, [(vault_id, failures_count, list_of_models_and_serial_numbers)])
        yield (
            window.start,
            value[1],
            list([(value[0][1], value[1], value[2])]),
        )
