#!/bin/bash


KAFKA_CONTAINER_NAME="kafka-broker"
KAFKA_BROKER="localhost:9092"
TOPICS=("disk-monitoring" "filtered-disk-monitoring")
PARTITIONS=1
REPLICATION_FACTOR=1

if [ "$1" == "create" ]; then
  for topic in "${TOPICS[@]}"; do
    docker exec -it $KAFKA_CONTAINER_NAME kafka-topics.sh --create --topic "$topic" --bootstrap-server "$KAFKA_BROKER" --partitions "$PARTITIONS" --replication-factor "$REPLICATION_FACTOR"
  done
elif [ "$1" == "delete" ]; then
  for topic in "${TOPICS[@]}"; do
    docker exec -it $KAFKA_CONTAINER_NAME kafka-topics.sh --delete --topic "$topic" --bootstrap-server "$KAFKA_BROKER"
  done
else
  echo "Invalid action. Use 'create' or 'delete'"
  exit 1
fi




