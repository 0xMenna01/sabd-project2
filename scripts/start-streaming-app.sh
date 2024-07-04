#!/bin/bash

STREAM_PRODUCER_HOME="/home/kafka-producer"
CONTAINER_NAME="event-producer"

if [ $# -ne 1 ]; then
    echo "Usage: $0 <query1|query2|query3>"
    exit 1
fi

QUERY="$1"

case "$QUERY" in
    "query1")
        QUERY_NUM=1
        ;;
    "query2")
        QUERY_NUM=2
        ;;
    "query3")
        QUERY_NUM=3
        ;;
    *)
        echo "Invalid query. Please use query1, query2, or query3."
        exit 1
        ;;
esac

cd ../docker/flink_submit

docker build -t jobsubmit-app .
cd ../..
# Start the Flink job. This will run the input query, evaluate metrics, and write results to a local csv file.
docker run -d --network project2_network --name flink-app --volume ./src/processing:/opt/flink/job --volume ./Results:/opt/flink/results jobsubmit-app bash -c "flink run -m jobmanager:8081 --jarfile /KafkaConnectorDependencies.jar --python /opt/flink/job/main.py $QUERY_NUM --evaluation"

# Start producing events
#./scripts/start-producing.sh
