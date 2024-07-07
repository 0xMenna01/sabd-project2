import config
import csv
import json
from loguru import logger
import time
import random
from datetime import datetime
from .producer import KafkaProducer
from . import utils


class StreamEmulator:
    def __init__(self, is_fast: bool = True):
        self.producer = KafkaProducer()
        logger.info("Kafka producer initialized successfully.")
        # Wheter to produce events at fast speed (without delays between events of the same day) and with a higher scaling factor.
        self.is_fast = is_fast

    def start(self):
        logger.info(f"Starting event production, fast mode: {self.is_fast}.")
        starting_time = time.time()

        s_factor = utils.scale_factor(self.is_fast)
        f_interval = utils.flush_interval(self.is_fast)
        with open(config.DATASET_PATH, "r") as f:
            event_reader = csv.reader(f)

            next(event_reader)  # Skip header

            prev_timestamp = None
            # Since no events have been produced yet, we set the latest_flush to the current time
            latest_flush = time.time()

            logger.info("Producing events..")

            for event in event_reader:
                event = tuple(event)
                try:
                    dt = datetime.strptime(event[0], "%Y-%m-%dT%H:%M:%S.%f")
                except ValueError:
                    continue

                event_time = dt.timestamp()

                if prev_timestamp:
                    time_interval = (event_time - prev_timestamp) / s_factor
                    if time_interval > 0:
                        time.sleep(time_interval)
                    if not self.is_fast and time_interval == 0:
                        time.sleep(random.uniform(0, 0.1))

                json_event = json.dumps(event)
                self.producer.produce_event(json_event.encode())

                prev_timestamp = event_time

                if time.time() - latest_flush > f_interval:
                    self.producer.flush()
                    latest_flush = time.time()

        trigger_event = utils.tuple_for_last_window_triggering(event)
        self.producer.produce_event(trigger_event.encode())
        self.producer.flush()

        ending_time = time.time()
        logger.info(
            f"Finished producing events. Time elapsed: {ending_time - starting_time} seconds."
        )
