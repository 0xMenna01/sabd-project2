import pandas as pd
from producer import KafkaProducer
import time
import utils


SPEED_FACTOR = 3600  # 1 hour in seconds
FLUSHING_INTERVAL = 5  # seconds


class StreamEmulator:
    def __init__(self):
        self.producer = KafkaProducer()

    def start(self):
        df = utils.read_df_from_csv()

        prev_timestamp = None
        # Since no events have been produced yet, we set the latest_flush to the current time
        latest_flush = time.time()
        for _, row in df.iterrows():
            event_time = pd.Timestamp(row['date']).timestamp()

            if prev_timestamp:
                time_interval = (event_time - prev_timestamp) / SPEED_FACTOR
                if time_interval > 0:
                    time.sleep(time_interval)

            event = row.to_json()
            self.producer.produce_event(event)

            prev_timestamp = event_time

            if time.time() - latest_flush > FLUSHING_INTERVAL:
                self.producer.flush()
                latest_flush = time.time()

        self.producer.free()
