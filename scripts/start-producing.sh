#!/bin/bash

STREAM_PRODUCER_HOME="/home/kafka-producer"
CONTAINER_NAME="event-producer"

# Check for arguments
if [ "$#" -eq 1 ] && [ "$1" == "--fast" ]; then
    FAST_MODE="--fast"
else
    FAST_MODE=""
    if [ "$#" -ne 0 ]; then
        echo "Usage: $0 [--fast]"
        exit 1
    fi
fi

docker exec -it --workdir $STREAM_PRODUCER_HOME $CONTAINER_NAME python $STREAM_PRODUCER_HOME/src/main.py $FAST_MODE
