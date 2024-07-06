#!/bin/bash

DOCKER_COMPOSE_DIR="../docker"
SETUP_TOPICS_SCRIPT="../scripts/setup-topics.sh"
FAUST_CONTAINER_NAME="faust_preprocessing"
FAUST_SCRIPT_PATH="/home/faust/src/main.py"
PROMETHEUS_CONTAINER_NAME="docker-prometheus-1"
PROMETHEUS_COMMAND="prometheus --config.file=./prometheus.yml --web.listen-address=:9010"

start_containers() {
    docker compose up -d
    $SETUP_TOPICS_SCRIPT create

    if [[ "$*" =~ "--faust-preprocessing" ]]; then
        echo "Starting Faust app to listen for events to ingest..."
        sleep 3
        docker compose exec -d "$FAUST_CONTAINER_NAME" bash -c "python $FAUST_SCRIPT_PATH worker -l info"
        echo "Faust app started."
    fi

    if [[ "$*" =~ "--monitor" ]]; then
        echo "Starting Prometheus monitoring..."
        sleep 3
        docker exec -t -i -d "$PROMETHEUS_CONTAINER_NAME" $PROMETHEUS_COMMAND
        echo "Prometheus monitoring started."
    fi
}


stop_containers() {
    docker compose down
}


case "$1" in
    start)
        cd "$DOCKER_COMPOSE_DIR" || { echo "Directory $DOCKER_COMPOSE_DIR not found."; exit 1; }
        start_containers "$@"
        ;;
    stop)
        cd "$DOCKER_COMPOSE_DIR" || { echo "Directory $DOCKER_COMPOSE_DIR not found."; exit 1; }
        stop_containers
        ;;
    *)
        echo "Usage: $0 [start|stop] [--faust-preprocessing] [--monitor]"
        exit 1
        ;;
esac