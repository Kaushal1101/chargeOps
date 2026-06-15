import uuid

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    avg,
    col,
    concat,
    first,
    from_json,
    lit,
    max as spark_max,
    struct,
    to_json,
    udf,
    when,
    window,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

TELEMETRY_SCHEMA = StructType([
    StructField("event_id", StringType(), True),
    StructField("event_ts", StringType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("cargo_temperature", DoubleType(), True),
    StructField("time_left_to_destination", IntegerType(), True),
    StructField("sla_time_remaining", IntegerType(), True),
    StructField("scenario_state", StringType(), True),
    StructField("sla_buffer_threshold", IntegerType(), True),
    StructField("cargo_temp_threshold", DoubleType(), True),
])


def run():
    spark = (
        SparkSession.builder.appName("LogiShield-StreamProcessor")
        .master("local[*]")
        .config("spark.ui.port", "4040")
        .config("spark.sql.shuffle.partitions", "8")
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

    parsed = raw_stream.select(
        from_json(col("value").cast("string"), TELEMETRY_SCHEMA).alias("data"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp"),
    )

    structured = parsed.select(
        col("data.event_id"),
        col("data.event_ts").cast(TimestampType()).alias("event_ts"),
        col("data.vehicle_id"),
        col("data.cargo_temperature"),
        col("data.time_left_to_destination"),
        col("data.sla_time_remaining"),
        col("data.scenario_state"),
        col("data.sla_buffer_threshold"),
        col("data.cargo_temp_threshold"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp"),
    )

    with_metrics = structured.withColumn(
        "delivery_buffer",
        col("sla_time_remaining") - col("time_left_to_destination"),
    )

    watermarked = with_metrics.withWatermark("event_ts", "5 minutes")

    windowed = (
        watermarked
        .groupBy(
            window(col("event_ts"), "10 minutes", "30 seconds"),
            col("vehicle_id"),
        )
        .agg(
            avg(col("delivery_buffer")).alias("avg_delivery_buffer"),
            spark_max(col("cargo_temperature")).alias("max_cargo_temperature"),
            first(col("sla_buffer_threshold")).alias("sla_buffer_threshold"),
            first(col("cargo_temp_threshold")).alias("cargo_temp_threshold"),
        )
    )

    tiered = windowed.withColumn(
        "risk_tier",
        when(
            (col("avg_delivery_buffer") < 0)
            | (col("max_cargo_temperature") > col("cargo_temp_threshold")),
            "RED",
        )
        .when(
            (col("avg_delivery_buffer") < col("sla_buffer_threshold"))
            | (col("max_cargo_temperature") > col("cargo_temp_threshold") * 0.9),
            "YELLOW",
        )
        .otherwise("GREEN"),
    )

    alerts = tiered.filter(col("risk_tier") != "GREEN")

    gen_id = udf(lambda: str(uuid.uuid4()), StringType()).asNondeterministic()

    alert_records = alerts.select(
        gen_id().alias("event_id"),
        col("window.end").cast("string").alias("event_ts"),
        col("vehicle_id"),
        col("risk_tier"),
        col("avg_delivery_buffer").cast(IntegerType()).alias("delivery_buffer"),
        col("max_cargo_temperature").alias("cargo_temperature"),
        when(
            col("risk_tier") == "RED",
            when(
                col("avg_delivery_buffer") < 0,
                concat(
                    lit("Delivery buffer breached SLA: "),
                    col("avg_delivery_buffer").cast("integer").cast("string"),
                    lit("min"),
                ),
            ).otherwise(
                concat(
                    lit("Cargo temperature exceeded threshold: "),
                    col("max_cargo_temperature").cast("string"),
                    lit("C"),
                )
            ),
        )
        .otherwise(
            when(
                col("avg_delivery_buffer") < col("sla_buffer_threshold"),
                concat(
                    lit("Delivery buffer below threshold: "),
                    col("avg_delivery_buffer").cast("integer").cast("string"),
                    lit("min"),
                ),
            ).otherwise(
                concat(
                    lit("Cargo temperature approaching threshold: "),
                    col("max_cargo_temperature").cast("string"),
                    lit("C"),
                )
            )
        )
        .alias("reason"),
    )

    kafka_payload = alert_records.select(
        col("vehicle_id").cast(StringType()).alias("key"),
        to_json(struct(*[col(c) for c in alert_records.columns])).alias("value"),
    )

    query = (
        kafka_payload.writeStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9093")
        .option("topic", "risk-alerts")
        .option("checkpointLocation", "/tmp/logishield-checkpoints/risk-alerts")
        .outputMode("update")
        .trigger(processingTime="5 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    run()
