#!/bin/bash

cd ../docker

if [ "$1" == "start" ]; then
    docker compose up -d

    ../scripts/setup-topics.sh create

    if [[ "$@" =~ "--faust-preprocessing" ]]; then
        echo "Starting Faust app to listen for events to ingest..."
        sleep 5
        # Prepare to consume events from Kafka. Faust will then ingest them into a new topic for the Flink job to consume.
        docker compose exec -d faust_preprocessing bash -c "python /home/faust/src/main.py worker -l info"
        echo "Faust app started."
    fi

elif [ "$1" == "stop" ]; then
    docker compose down
else
    echo "Usage: $0 [start|stop] [--faust]"
    exit 1
fi
