from enum import Enum
from api import QueryNum, StreamingApi
from loguru import logger


class Controller:
    def __init__(self, query: QueryNum, evaluation: bool = False) -> None:
        self._query = query
        self._evaluation = evaluation

    def run(self):
        # api to handle the datastream
        api = StreamingApi(query=self._query, evaluation=self._evaluation)

        stream = api.prepare_stream()
