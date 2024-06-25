from datetime import datetime
import re
import faust
from faust.types.app import AppT
from config import KafkaConfig
from typing import Tuple, Optional
from loguru import logger


class FilteredDiskEvent(faust.Record):
    timestamp: int
    serial_number: str
    model: str
    failure: bool
    vault_id: int
    s9_power_on_hours: int
    s194_temperature_celsius: Optional[int]


class IngestionApp:
    def __init__(self, app: AppT):
        self.app = app

    def start(self):
        self.app.main()


class AppBuilder:
    def __init__(self, app_name):
        kafka_config = KafkaConfig()
        self.app = faust.App(app_name, broker=kafka_config.broker_url)
        self.src_topic = self.app.topic(
            kafka_config.topic_src)
        self.dest_topic = self.app.topic(
            kafka_config.topic_dest, value_type=FilteredDiskEvent)

        logger.info(f"AppBuilder initialized with app_name: {app_name}")

    def build(self) -> IngestionApp:
        @self.app.agent(self.src_topic)
        async def ingest_disk_record(disk_events):
            async for event in disk_events:
                filtered_data = filter_event(event)
                if filtered_data:
                    await self.dest_topic.send(value=filtered_data)

        return IngestionApp(self.app)


def filter_event(event: Tuple) -> Optional[FilteredDiskEvent]:
    """Filter an event by taking only fields of interest.

    Returns: Optional[FilteredDiskEvent]: A filtered event if the input event is valid, None otherwise.
    """

    date = event[0]
    serial_number = event[1]
    model = event[2]
    failure = event[3]
    vault_id = event[4]
    s9_power_on_hours = event[12]
    s194_temperature_celsius = event[25]

    serial_number_pattern = r'^[A-Z0-9_-]+$'
    model_pattern = r'^[A-Z0-9 ]+$'

    if not re.match(serial_number_pattern, serial_number):
        return None
    if not re.match(model_pattern, model):
        return None
    # At this point we know that date, serial_number and model are valid
    if failure == '' or vault_id == '' or s9_power_on_hours == '':
        return None

    timestamp = int(datetime.fromisoformat(date).timestamp())

    return FilteredDiskEvent(
        timestamp,
        serial_number,
        model,
        failure=bool(failure),
        vault_id=int(vault_id),
        # removes the .0 from the string
        s9_power_on_hours=int(s9_power_on_hours[:-2]),
        # Could be null, it will be handled by the consumer (Flink)
        s194_temperature_celsius=s194_temperature_celsius
    )
