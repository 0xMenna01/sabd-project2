from enum import Enum
from api import QueryNum, StreamingApi
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
        self._evaluation = args.evaluation
        self._local_write = args.local_write

    def run(self):
        api = StreamingApi(
            query=self._query,
            evaluation=self._evaluation,
            write_locally=self._local_write,
        )
        logger.info(f"Streaming API initialized for {self._query}.")

        api.prepare_stream().query()
