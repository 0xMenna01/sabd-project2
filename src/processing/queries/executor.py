from __future__ import annotations
from abc import ABC, abstractmethod
from pyflink.datastream.window import (
    WindowAssigner,
)
from pyflink.datastream import DataStream


class QueryExecutor(ABC):
    @abstractmethod
    def window_assigner(self, window: WindowAssigner) -> QueryExecutor:
        pass

    @abstractmethod
    def execute(self) -> DataStream:
        pass
