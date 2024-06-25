#!/bin/bash

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



docker build -f ./dockerfiles/jobsumit.Dockerfile -t jobsubmit-app .
docker run -d --network project2_network --name flink-app --volume ./src/flink:/opt/flink/job jobsubmit-app

docker exec -it flink-app bash -c "flink run --jobmanager jobmanager:8081 --python /opt/flink/job/main.py $QUERY_NUM"