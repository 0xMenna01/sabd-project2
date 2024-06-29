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

# Set JAVA_HOME environment variable
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-arm64
ENV PATH $JAVA_HOME/bin:$PATH

# Install apache-flink
RUN pip3 install apache-flink loguru
