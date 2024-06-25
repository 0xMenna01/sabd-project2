FROM flink:latest

RUN apt-get update -y \
    && apt-get install -y python3 python3-pip \
    && ln -s /usr/bin/python3 /usr/bin/python \
    && apt-get clean


RUN pip3 install apache-flink apache-flink-connector-kafka

CMD ["/bin/bash"]
