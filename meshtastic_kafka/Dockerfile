FROM python:3.11-slim

WORKDIR /app

RUN apt-get update && apt-get install -y curl
COPY . .
RUN pip install .

ENV PYTHONUNBUFFERED=1