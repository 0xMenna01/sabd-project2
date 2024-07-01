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
            choices=[1, 2, 3],
            help="Query number for the Streaming API (1, 2, or 3)",
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
        parser.add_argument(
            "--local-write",
            action="store_true",
            default=False,
            help="Enable writing locally",
        )

        args = parser.parse_args()

        self._query = QueryNum(args.query)
        self._is_preprocessed = args.faust_preprocess
        self._evaluation = args.evaluation
        self._local_write = args.local_write

    def run(self):
        api = StreamingApi(
            query=self._query,
            is_preprocessed=self._is_preprocessed,
            evaluation=self._evaluation,
            write_locally=self._local_write,
        )

        api.prepare_stream().query().sink_and_execute()
