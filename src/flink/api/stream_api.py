from pyflink.datastream import StreamExecutionEnvironment, RuntimeExecutionMode, TimeCharacteristic
from pyflink.common.watermark_strategy import WatermarkStrategy
from utils import utils
from pyflink.common.typeinfo import Types
import json


class StreamingApi:

    def __init__(self):
        env = StreamExecutionEnvironment.get_execution_environment()
        env.set_runtime_mode(RuntimeExecutionMode.STREAMING)
        env.set_parallelism(1)
        env.set_stream_time_characteristic(TimeCharacteristic.EventTime)

        # Build the Kafka source
        kafka_src = utils.build_kafka_source()

        self._stream = env.from_source(
            kafka_src, WatermarkStrategy.no_watermarks(), source_name="Ingestion_S")
        self._env = env

    def preperate_stream(self):
        # Convert the JSON event to a Row
        self._stream = self._stream.map(
            lambda msg: utils.json_event_to_row,
            output_type=Types.ROW([
                Types.STRING(), Types.STRING(),
                Types.STRING(), Types.BOOLEAN(),
                Types.INT(), Types.INT(), Types.INT()
            ])
        )

    @property
    def stream(self):
        return self._stream

    @property
    def env(self):
        return self._env
