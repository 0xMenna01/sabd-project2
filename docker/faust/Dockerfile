FROM python:3.9-slim

RUN apt-get update 

# Add faust user
RUN useradd -m faust -s /bin/bash

WORKDIR /home/faust

USER faust

ENV CONF_PATH=/home/faust/conf.json

COPY conf/requirements.txt requirements.txt
COPY conf/kafka-config.json conf.json

RUN pip install -r requirements.txt
