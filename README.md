# Project 2: Real-time Analysis of Disk Monitoring Events with Apache Flink

## Getting Started

### Prerequisites

Ensure you have the following installed:

- Docker
- Docker Compose

### Starting the Architecture

1. **Navigate to the scripts directory:**

   ```sh
   cd scripts
   ```

2. **Start the architecture:**

   ```sh
   ./manage-architecture.sh start --dataset <dataset_path>
   ```

   **If you want to enable monitoring using Prometheus run:**

   ```sh
   ./manage-architecture.sh start --dataset <dataset_path> --monitor
   ```

3. **To Enable Faust (optional), for preprocessing and ingesting events, run:**

   ```sh
   ./manage-architecture.sh start --dataset <dataset_path> --faust-preprocessing
   ```

   **Note:** If Faust is enabled, the overall query execution is slower due to the added level of indirection.

### Executing Queries

Once the architecture is running, you can execute queries as follows:

1. **To run the first query, type the following command:**

   ```sh
   ./start-streaming-app.sh query1
   ```

2. **Query metrics evaluation:**

   ```sh
   ./start-streaming-app.sh query2 --evaluation
   ```

   **Note:** To execute two queries simultaneously, you must use the `--evaluation` flag because it utilizes the `execute_async` function to schedule the job execution. If you want to use the preprocessed data by faust, use the `--faust-preprocess` flag.

3. **Stopping a query:**
   Before starting a new query, stop the current flink client container:
   ```sh
   ./stop-flink-app.sh
   ```

### Real-time Simulation of Events

Start simulating events in real-time:

1. **Fast mode execution:**

   ```sh
   ./start-producing.sh --fast
   ```

2. **Realistic event generation:**

   ```sh
   ./start-producing.sh
   ```

   In this mode, tuples of the same day are produced within a randomly distributed interval to simulate real-world conditions.

### Stopping the Architecture

To stop the entire architecture, run:

```sh
./manage-architecture.sh stop
```
