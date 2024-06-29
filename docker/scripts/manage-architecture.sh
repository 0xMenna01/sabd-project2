#!/bin/bash

if [ "$1" == "start" ]; then
    docker compose up -d
    ../scripts/manage-topics.sh create
    # Prepare to consume events from Kafka. Faust will then ingest them into a new topic for the Flink job to consume.
    docker-compose exec -d faust_ingestion bash -c "python /home/faust/src/app.py worker -l info"

elif [ "$1" == "stop" ]; then
    docker compose down
else
    echo "Usage: $0 [start|stop]"
    exit 1
fi