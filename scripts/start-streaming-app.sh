#!/bin/bash

STREAM_PRODUCER_HOME="/home/kafka-producer"
CONTAINER_NAME="event-producer"

usage() {
    echo "Usage: $0 <query1|query2> [--evaluation] [--faust-preprocess]"
    exit 1
}

if [ $# -lt 1 ]; then
    usage
fi

QUERY=""
EVALUATION=""
FAUST_PREPROCESS=""

# Parse the first argument as the query
case "$1" in
    "query1")
        QUERY="1"
        ;;
    "query2")
        QUERY="2"
        ;;
    *)
        usage
        ;;
esac

# Shift to the next argument
shift

# Check for optional parameters
while [[ $# -gt 0 ]]; do
    case "$1" in
        --evaluation)
            EVALUATION="--evaluation"
            shift
            ;;
        --faust-preprocess)
            FAUST_PREPROCESS="--faust-preprocess"
            shift
            ;;
        *)
            usage
            ;;
    esac
done

cd ../docker/flink_submit

docker build -t jobsubmit-app .
cd ../..
# Start the Flink job. This will run the input query, evaluate metrics, and write results to a local csv file.
docker run -d --network project2_network --name flink-app --volume ./src/processing:/opt/flink/job --volume ./Results:/opt/flink/results jobsubmit-app bash -c "flink run -m jobmanager:8081 --python /opt/flink/job/main.py $QUERY $EVALUATION $FAUST_PREPROCESS"