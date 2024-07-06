from __future__ import annotations
from typing import Iterable, List, Tuple, Dict
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
import bisect
from .executor import QueryExecutor


TOP_FAILURES = 10

# (vault_id, failures_count_in_one_day, list_of_models_and_serial_numbers)
DailyVaultFailures = Tuple[int, int, List[Tuple[str, str]]]
# List[(vault_id, failures_count_in_one_day, list_of_models_and_serial_numbers)])
# Used for accumulating the top 10 vaults with the most failures within the same day.
VaultsRankingFailures = List[DailyVaultFailures]
# (timestamp, failures_ranking)
TimestampedRankingFailures = Tuple[int, VaultsRankingFailures]


class QueryTwoExecutor(QueryExecutor):
    def __init__(self, data: DataStream):
        """
        Query 2.

        param data: stream of Row [timestamp, serial_number, model, failure, vault_id, s9_power_on_hours, s194_temperature_celsius]
        """
        self.partial_vaults_failures_stream = (
            data.filter(lambda x: x.failure == True)
            .map(
                lambda x: (
                    x.vault_id,
                    1,
                    list([(x.model, x.serial_number)]),
                )
            )
            .key_by(lambda x: x[0])
            .window(TumblingEventTimeWindows.of(Time.days(1)))
            # Count the number of vault's failures per day and accumulate the models and serial numbers.
            .reduce(
                lambda x, y: (
                    x[0],  # vault_id
                    x[1] + y[1],  # failures_count
                    list(
                        set(x[2] + y[2])
                    ),  # list of models and serial numbers (avoid duplicate pairs)
                ),
            )
        )

        self.window: WindowAssigner | None = None

    def window_assigner(self, window: WindowAssigner) -> QueryTwoExecutor:
        self.window = window
        return self

    def query(self) -> DataStream:
        if self.window is None:
            raise ValueError("Window assigner not set")

        return self.partial_vaults_failures_stream.window_all(self.window).aggregate(
            DailyFailuresRankingAggregateFunction(), TimestampForVaultsRanking()
        )


class DailyFailuresRankingAggregateFunction(AggregateFunction):

    def create_accumulator(self) -> VaultsRankingFailures:
        return list([])

    def add(
        self, value: DailyVaultFailures, ranking: VaultsRankingFailures
    ) -> VaultsRankingFailures:
        vault_id = value[0]

        # Negligible overhead - O(log10)
        idx = binary_search_reverse(ranking, vault_id)
        if idx:
            # Vault already in the ranking, keep the max value
            ranking[idx] = max(ranking[idx], value, key=lambda x: x[1])
        else:
            # Vault not in the ranking
            ranking.append(value)

        ranking.sort(key=lambda x: x[1], reverse=True)
        ranking = ranking[:TOP_FAILURES]

        return ranking

    def merge(
        self, ranking_a: VaultsRankingFailures, ranking_b: VaultsRankingFailures
    ) -> VaultsRankingFailures:
        ranking = []

        # vault_id -> position in list
        registered_vaults_pos: Dict[int, int] = {}
        # The loop does not add much overhead since the elements are at most 20, and the merge operation is not frequent.
        for vault_failures in ranking_a + ranking_b:
            vault_id = vault_failures[1]

            i = registered_vaults_pos.get(vault_id)
            if i is not None:
                # Vault already in the list, keep the max value
                vault_failures = max(ranking[i], vault_failures, key=lambda x: x[1])
                ranking[i] = vault_failures
            else:
                ranking.append(vault_failures)
                i = len(ranking) - 1
                registered_vaults_pos[vault_id] = i

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


def binary_search_reverse(ranking: VaultsRankingFailures, vault_id: int) -> int | None:
    low, high = 0, len(ranking) - 1
    while low <= high:
        mid = (low + high) // 2
        if ranking[mid][0] == vault_id:
            return mid
        elif ranking[mid][0] < vault_id:
            high = mid - 1
        else:
            low = mid + 1
    return None
