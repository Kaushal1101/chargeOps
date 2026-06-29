import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.functions import (
    avg,
    col,
    concat,
    first,
    from_json,
    lit,
    max,
    min,
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

_PROGRESS_FILE = Path("/tmp/logishield-spark-progress.jsonl")


class _ProgressWriter(StreamingQueryListener):
    def onQueryStarted(self, event):
        _PROGRESS_FILE.write_text("")

    def onQueryProgress(self, event):
        p = event.progress
        record = {
            "timestamp": p.timestamp,
            "batchId": p.batchId,
            "inputRowsPerSecond": p.inputRowsPerSecond,
            "processedRowsPerSecond": p.processedRowsPerSecond,
            "numInputRows": p.numInputRows,
            "durationMs": p.durationMs,
        }
        with _PROGRESS_FILE.open("a", encoding="utf-8") as f:
            f.write(json.dumps(record) + "\n")

    def onQueryTerminated(self, event):
        pass


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
    StructField("trip_state", StringType(), True),
    StructField("trip_id", StringType(), True),
    StructField("cargo_type", StringType(), True),
    StructField("cargo_value", DoubleType(), True),
    StructField("customer_priority", StringType(), True),
    StructField("service_level", StringType(), True),
    StructField("destination_region", StringType(), True),
    StructField("route_progress", DoubleType(), True),
    StructField("estimated_arrival_minutes", IntegerType(), True),
    StructField("remaining_stops", IntegerType(), True),
    StructField("driver_hours_remaining", DoubleType(), True),
])


def run():
    spark = (
        SparkSession.builder.appName("LogiShield-StreamProcessor")
        .master("local[*]")
        .config("spark.ui.port", "4040")
        .config("spark.sql.shuffle.partitions", "8")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    spark.streams.addListener(_ProgressWriter())

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", "localhost:9093")
        .option("subscribe", "fleet-telemetry")
        .option("startingOffsets", "latest")
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
        col("data.trip_state"),
        col("data.trip_id"),
        col("data.cargo_type"),
        col("data.cargo_value"),
        col("data.customer_priority"),
        col("data.service_level"),
        col("data.destination_region"),
        col("data.route_progress"),
        col("data.estimated_arrival_minutes"),
        col("data.remaining_stops"),
        col("data.driver_hours_remaining"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp"),
    )

    with_metrics = structured.withColumn(
        "delivery_buffer",
        col("sla_time_remaining") - col("time_left_to_destination"),
    )

    in_transit = with_metrics.filter(col("trip_state") == "IN_TRANSIT")
    watermarked = in_transit.withWatermark("event_ts", "1 minute")

    windowed = (
        watermarked
        .groupBy(
            window(col("event_ts"), "5 minutes", "30 seconds"),
            col("vehicle_id"),
        )
        .agg(
            avg(col("delivery_buffer")).alias("avg_delivery_buffer"),
            avg(col("cargo_temperature")).alias("avg_cargo_temperature"),
            first(col("sla_buffer_threshold")).alias("sla_buffer_threshold"),
            first(col("cargo_temp_threshold")).alias("cargo_temp_threshold"),
            first(col("trip_id")).alias("trip_id"),
            first(col("cargo_type")).alias("cargo_type"),
            first(col("cargo_value")).alias("cargo_value"),
            first(col("customer_priority")).alias("customer_priority"),
            first(col("service_level")).alias("service_level"),
            first(col("destination_region")).alias("destination_region"),
            max(col("route_progress")).alias("max_route_progress"),
            min(col("remaining_stops")).alias("min_remaining_stops"),
            min(col("driver_hours_remaining")).alias("min_driver_hours_remaining"),
            min(col("estimated_arrival_minutes")).alias("min_estimated_arrival_minutes"),
        )
    )

    tiered = windowed.withColumn(
        "risk_tier",
        when(
            (col("avg_delivery_buffer") < 0)
            | (col("avg_cargo_temperature") > col("cargo_temp_threshold")),
            "RED",
        )
        .when(
            (col("avg_delivery_buffer") < col("sla_buffer_threshold"))
            | (col("avg_cargo_temperature") > col("cargo_temp_threshold") * 0.9),
            "YELLOW",
        )
        .otherwise("GREEN"),
    )

    alerts = tiered.filter(col("risk_tier") != "GREEN")

    gen_id = udf(lambda: str(uuid.uuid4()), StringType()).asNondeterministic()

    alert_records = alerts.select(
        gen_id().alias("event_id"),
        col("window.start").cast("string").alias("window_start"),
        col("window.end").cast("string").alias("window_end"),
        col("vehicle_id"),
        col("risk_tier"),
        col("avg_delivery_buffer").cast(IntegerType()).alias("delivery_buffer"),
        col("avg_cargo_temperature").alias("avg_temperature"),
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
                    col("avg_cargo_temperature").cast("string"),
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
                    col("avg_cargo_temperature").cast("string"),
                    lit("C"),
                )
            )
        )
        .alias("reason"),
        col("trip_id"),
        col("cargo_type"),
        col("cargo_value"),
        col("customer_priority"),
        col("service_level"),
        col("destination_region"),
        col("max_route_progress").alias("route_progress"),
        col("min_remaining_stops").alias("remaining_stops"),
        col("min_driver_hours_remaining").alias("driver_hours_remaining"),
        col("min_estimated_arrival_minutes").alias("estimated_arrival_minutes"),
    )

    last_tiers: dict[tuple[str, str], str] = {}

    def write_on_transition(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return

        rows = batch_df.collect()
        new_alerts = []
        for row in rows:
            vid = row["vehicle_id"]
            trip_id = row["trip_id"]
            tier = row["risk_tier"]
            key = (vid, trip_id)
            if last_tiers.get(key) != tier:
                last_tiers[key] = tier
                new_alerts.append(row)

        if not new_alerts:
            return

        spark_session = SparkSession.getActiveSession()
        new_df = spark_session.createDataFrame(new_alerts, batch_df.schema)

        alert_ts_str = datetime.now(timezone.utc).isoformat()
        new_df = new_df.withColumn("alert_ts", lit(alert_ts_str))

        kafka_output = new_df.select(
            col("vehicle_id").cast(StringType()).alias("key"),
            to_json(struct(*[col(c) for c in new_df.columns])).alias("value"),
        )

        kafka_output.write.format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9093") \
            .option("topic", "risk-alerts") \
            .save()

    query = (
        alert_records.writeStream
        .foreachBatch(write_on_transition)
        .option("checkpointLocation", "/tmp/logishield-checkpoints/risk-alerts")
        .outputMode("append")
        .trigger(processingTime="5 seconds")
        .start()
    )

    query.awaitTermination()


if __name__ == "__main__":
    run()
