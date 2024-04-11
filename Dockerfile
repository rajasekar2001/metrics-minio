FROM python:3.9-alpine3.19 

RUN mkdir /app

WORKDIR /app

COPY main.sh .
COPY main.py .
COPY requirements.txt .
COPY kafka_certs ./kafka_certs

RUN chmod +x main.sh

RUN pip install -r requirements.txt

CMD [ "/bin/sh", "-c", "./main.sh" ]