FROM python:3.11-slim

RUN apt-get update && apt-get install -y --no-install-recommends \
    default-jre-headless \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Pre-download Spark-Kafka connector into Ivy cache so container startup doesn't fetch it
RUN python -c "\
from pyspark.sql import SparkSession; \
spark = SparkSession.builder.master('local') \
    .config('spark.jars.packages', 'org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1') \
    .getOrCreate(); \
spark.stop()"

COPY . .
