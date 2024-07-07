#!/bin/bash

DOCKER_COMPOSE_DIR="../docker"
DATASET_DIR="../dataset"
SETUP_TOPICS_SCRIPT="../scripts/setup-topics.sh"
FAUST_CONTAINER_NAME="faust_preprocessing"
FAUST_SCRIPT_PATH="/home/faust/src/main.py"
PROMETHEUS_CONTAINER_NAME="docker-prometheus-1"
PROMETHEUS_COMMAND="prometheus --config.file=./prometheus.yml --web.listen-address=:9010"

# Function to show usage
usage() {
    echo "Usage: $0 start --dataset <path> [--faust-preprocessing] [--monitor]"
    echo "       $0 stop"
    exit 1
}

start_containers() {
    docker compose up -d
    $SETUP_TOPICS_SCRIPT create
        
    if [[ "$*" =~ "--faust-preprocessing" ]]; then
        echo "Starting Faust app to listen for events to ingest..."
        sleep 10
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

# Check if start or stop command is given
if [[ "$1" == "start" ]]; then
    shift
    if [[ "$1" != "--dataset" ]]; then
        usage
    fi
    
    DATASET_PATH="$2"
    if [[ -z "$DATASET_PATH" ]]; then
        usage
    fi

    # Check if the dataset path exists
    if [[ ! -f "$DATASET_PATH" ]]; then
        echo "Dataset file $DATASET_PATH not found."
        exit 1
    fi

    # Copy the dataset to the dataset directory
    mkdir -p "$DATASET_DIR"
    cp "$DATASET_PATH" "$DATASET_DIR"

    # Remove --dataset and the path from the arguments list
    shift 2

    cd "$DOCKER_COMPOSE_DIR" || { echo "Directory $DOCKER_COMPOSE_DIR not found."; exit 1; }
    start_containers "$@"
elif [[ "$1" == "stop" ]]; then
    cd "$DOCKER_COMPOSE_DIR" || { echo "Directory $DOCKER_COMPOSE_DIR not found."; exit 1; }
    stop_containers
else
    usage
fi
