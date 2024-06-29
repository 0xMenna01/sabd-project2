# Use the Flink base image
FROM flink:latest

# Install dependencies and update package list
RUN apt-get update -y && \
    apt-get install -y \
    ca-certificates \
    python3 \
    python3-pip \
    openjdk-11-jdk-headless && \
    ln -s /usr/bin/python3 /usr/bin/python && \
    rm -rf /var/lib/apt/lists/*

RUN curl -o /KafkaConnectorDependencies.jar https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.1/flink-sql-connector-kafka-1.17.1.jar


# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
ENV PATH $JAVA_HOME/bin:$PATH
ENV CONF_PATH=/opt/flink/conf/kafka-config.json

COPY conf/kafka-config.json /opt/flink/conf/kafka-config.json

# Install apache-flink
RUN pip3 install apache-flink

CMD ["flink", "run", "-m", "jobmanager:8081", "--jarfile", "/KafkaConnectorDependencies.jar", "--python", "/opt/flink/job/main.py"]