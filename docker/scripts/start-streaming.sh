#!/bin/bash

STREAM_PRODUCER_HOME="/home/kafka-producer"
CONTAINER_NAME="event-producer"

docker exec -it --workdir $STREAM_PRODUCER_HOME $CONTAINER_NAME python $STREAM_PRODUCER_HOME/src/main.py