from datetime import datetime
import re
from pyflink.datastream import (
    DataStream,
)
import json
from pyflink.common import Row
from pyflink.datastream.functions import MapFunction


EMPTY_STRING = ""
SERIAL_NUMBER_PATTERN = r"^[A-Z0-9_-]+$"
MODEL_PATTERN = r"^[A-Za-z0-9 _.-]+$"


class JsonEventToRow(MapFunction):
    def map(self, value: str) -> Row:
        event = json.loads(value)

        timestamp = datetime.fromisoformat(event[0]).timestamp()
        return Row(
            timestamp=timestamp * 1000,
            serial_number=event[1],
            model=event[2],
            failure=event[3],
            vault_id=event[4],
            s9_power_on_hours=event[12],
            s194_temperature_celsius=event[25],
        )


def execute(data: DataStream) -> DataStream:

    return (
        data.map(JsonEventToRow())
        .filter(
            lambda x: re.match(SERIAL_NUMBER_PATTERN, x.serial_number)
            and re.match(MODEL_PATTERN, x.model)
            and x.failure is not EMPTY_STRING
            and x.vault_id is not EMPTY_STRING
            and x.s9_power_on_hours is not EMPTY_STRING
        )
        .map(
            lambda x: Row(
                timestamp=x.timestamp,
                serial_number=str(x.serial_number),
                model=str(x.model),
                failure=bool(int(x.failure)),
                vault_id=int(x.vault_id),
                s9_power_on_hours=int(x.s9_power_on_hours[:-2]),
                s194_temperature_celsius=int(x.s194_temperature_celsius[:-2]),
            )
        )
    )
