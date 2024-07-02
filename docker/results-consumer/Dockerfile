FROM python:3.9-slim

RUN apt-get update 

# Add faust user
RUN useradd -m consumer-csv -s /bin/bash

WORKDIR /home/consumer-csv

USER consumer-csv

ENV RESULTS_PATH=/home/consumer-csv/results

COPY conf/requirements.txt requirements.txt

RUN pip install -r requirements.txt

CMD [ "/bin/bash" ]