import config
import csv
import json
from loguru import logger
import time
from datetime import datetime
from .producer import KafkaProducer


# Factor to scale the emulation time.
# 1 hour of real-time is emulated in 0.1 seconds.
SPEED_FACTOR = 36000


# Interval for flushing the queue to ensure it doesn't get overloaded.
# This helps manage bursts of events that have the same timestamp.
FLUSHING_INTERVAL = 0.5  # seconds


class StreamEmulator:
    def __init__(self):
        self.producer = KafkaProducer()
        logger.info("Kafka producer initialized successfully.")

    def start(self):
        starting_time = time.time()

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
                    time_interval = (event_time - prev_timestamp) / SPEED_FACTOR
                    if time_interval > 0:
                        time.sleep(time_interval)

                json_event = json.dumps(event)
                self.producer.produce_event(json_event.encode())

                prev_timestamp = event_time

                if time.time() - latest_flush > FLUSHING_INTERVAL:
                    self.producer.flush()
                    latest_flush = time.time()

        trigger_event = tuple_for_last_window_triggering(event)
        self.producer.produce_event(trigger_event.encode())
        self.producer.flush()

        ending_time = time.time()
        logger.info(
            f"Finished producing events. Time elapsed: {ending_time - starting_time} seconds."
        )


def tuple_for_last_window_triggering(event: tuple) -> str:
    l_event = list(event)
    # Set the timestamp to the day after the last event
    l_event[0] = "2023-04-24T00:00:00.000000"

    return json.dumps(tuple(l_event))
