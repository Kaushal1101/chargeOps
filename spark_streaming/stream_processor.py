from pyspark.sql import SparkSession


def run():
    spark = (
        SparkSession.builder.appName("LogiShield-StreamProcessor")
        .master("local[*]")
        .config("spark.ui.port", "4040")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9093")
        .option("subscribe", "fleet-telemetry")
        .option("startingOffsets", "earliest")
        .load()
    )

    projected = raw_stream.selectExpr(
        "CAST(key AS STRING) AS key",
        "CAST(value AS STRING) AS value",
        "topic",
        "partition",
        "offset",
        "timestamp",
    )

    query = (
        projected.writeStream.format("console")
        .outputMode("append")
        .option("truncate", "false")
        .option("numRows", "10")
        .trigger(processingTime="5 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    run()
