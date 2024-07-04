#!/bin/bash

DOCKER_COMPOSE_DIR="../docker"
SETUP_TOPICS_SCRIPT="../scripts/setup-topics.sh"
FAUST_CONTAINER_NAME="faust_preprocessing"
FAUST_SCRIPT_PATH="/home/faust/src/main.py"


start_containers() {
    docker compose up -d
    $SETUP_TOPICS_SCRIPT create

    if [[ "$*" =~ "--faust-preprocessing" ]]; then
        echo "Starting Faust app to listen for events to ingest..."
        sleep 5
        docker compose exec -d "$FAUST_CONTAINER_NAME" bash -c "python $FAUST_SCRIPT_PATH worker -l info"
        echo "Faust app started."
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
        echo "Usage: $0 [start|stop] [--faust-preprocessing]"
        exit 1
        ;;
esac