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

ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64
ENV PATH $JAVA_HOME/bin:$PATH
ENV CONF_PATH=/opt/flink/conf/kafka-config.json

RUN curl -o /opt/flink/lib/flink-sql-connector-kafka-1.19.1.jar https://repo.maven.apache.org/maven2/org/apache/flink/flink-sql-connector-kafka/1.17.1/flink-sql-connector-kafka-1.17.1.jar


COPY conf/requirements.txt /opt/flink/conf/requirements.txt
COPY conf/kafka-config.json /opt/flink/conf/kafka-config.json

WORKDIR /opt/flink/job

RUN pip3 install -r /opt/flink/conf/requirements.txt
