FROM python:3.7-alpine

COPY consumer.py .
COPY requirements.txt .

RUN apk add openjdk8
RUN pip install -r requirements.txt

ENTRYPOINT python consumer.py