from enum import Enum
from api.stream_api import QueryNum, StreamingApi
import argparse
from loguru import logger


class StreamCli:
    def __init__(self):
        parser = argparse.ArgumentParser(description="StreamCli Command Line Interface")
        parser.add_argument(
            "query",
            type=int,
            choices=[1, 2],
            help="Query number for the Streaming API (1, or 2)",
        )
        parser.add_argument(
            "--faust-preprocess",
            action="store_true",
            default=False,
            help="If the stream has been already preprocessed by Faust",
        )
        parser.add_argument(
            "--evaluation",
            action="store_true",
            default=False,
            help="Enable evaluation mode",
        )

        args = parser.parse_args()

        self._query = QueryNum(args.query)
        self._is_preprocessed = args.faust_preprocess
        self._evaluation = args.evaluation

    def run(self):
        api = StreamingApi(
            query=self._query,
            is_preprocessed=self._is_preprocessed,
            evaluation=self._evaluation,
        )

        api.prepare_stream().execute_query()
