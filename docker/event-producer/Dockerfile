FROM python:3.11

RUN apt-get update 

# Add spark user
RUN useradd -m kafka-producer -s /bin/bash

WORKDIR /home/kafka-producer

USER kafka-producer

ENV DATASET_PATH=/home/kafka-producer/dataset/raw_data_medium-utv_sorted.csv
ENV CONF_PATH=/home/kafka-producer/conf.json

COPY conf/requirements.txt requirements.txt
COPY conf/kafka-config.json conf.json

# Install dependencies
RUN pip install -r requirements.txt
