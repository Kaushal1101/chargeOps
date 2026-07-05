import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

import redis as redis_client
from pyspark.sql import SparkSession
from pyspark.sql.streaming import StreamingQueryListener
from pyspark.sql.functions import (
    avg,
    col,
    concat,
    count,
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
    StructField("charger_id", StringType(), True),
    StructField("charger_temperature", DoubleType(), True),
    StructField("estimated_completion_minutes", IntegerType(), True),
    StructField("session_time_remaining", IntegerType(), True),
    StructField("scenario_state", StringType(), True),
    StructField("session_buffer_threshold", IntegerType(), True),
    StructField("temp_threshold", DoubleType(), True),
    StructField("session_state", StringType(), True),
    StructField("session_id", StringType(), True),
    StructField("connector_type", StringType(), True),
    StructField("energy_requested_kwh", DoubleType(), True),
    StructField("user_tier", StringType(), True),
    StructField("charging_speed", StringType(), True),
    StructField("site_region", StringType(), True),
    StructField("session_progress", DoubleType(), True),
    StructField("power_output_kw", DoubleType(), True),
    StructField("energy_delivered_kwh", DoubleType(), True),
    StructField("charger_lat", DoubleType(), True),
    StructField("charger_lng", DoubleType(), True),
    StructField("rated_power_kw", DoubleType(), True),
    StructField("site_id", StringType(), True),
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
        .option("subscribe", "charger-telemetry")
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
        col("data.charger_id"),
        col("data.charger_temperature"),
        col("data.estimated_completion_minutes"),
        col("data.session_time_remaining"),
        col("data.scenario_state"),
        col("data.session_buffer_threshold"),
        col("data.temp_threshold"),
        col("data.session_state"),
        col("data.session_id"),
        col("data.connector_type"),
        col("data.energy_requested_kwh"),
        col("data.user_tier"),
        col("data.charging_speed"),
        col("data.site_region"),
        col("data.session_progress"),
        col("data.power_output_kw"),
        col("data.energy_delivered_kwh"),
        col("data.charger_lat"),
        col("data.charger_lng"),
        col("data.rated_power_kw"),
        col("data.site_id"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp"),
    )

    with_metrics = structured.withColumn(
        "session_buffer",
        col("session_time_remaining") - col("estimated_completion_minutes"),
    )

    charging = with_metrics.filter(col("session_state") == "CHARGING")
    watermarked = charging.withWatermark("event_ts", "1 minute")

    windowed = (
        watermarked
        .groupBy(
            window(col("event_ts"), "5 minutes", "30 seconds"),
            col("charger_id"),
        )
        .agg(
            avg(col("session_buffer")).alias("avg_session_buffer"),
            avg(col("charger_temperature")).alias("avg_charger_temperature"),
            first(col("session_buffer_threshold")).alias("session_buffer_threshold"),
            first(col("temp_threshold")).alias("temp_threshold"),
            first(col("session_id")).alias("session_id"),
            first(col("connector_type")).alias("connector_type"),
            first(col("energy_requested_kwh")).alias("energy_requested_kwh"),
            first(col("user_tier")).alias("user_tier"),
            first(col("charging_speed")).alias("charging_speed"),
            first(col("site_region")).alias("site_region"),
            first(col("charger_lat")).alias("charger_lat"),
            first(col("charger_lng")).alias("charger_lng"),
            first(col("rated_power_kw")).alias("rated_power_kw"),
            first(col("site_id")).alias("site_id"),
            max(col("session_progress")).alias("max_session_progress"),
            max(col("power_output_kw")).alias("max_power_output_kw"),
            max(col("energy_delivered_kwh")).alias("max_energy_delivered_kwh"),
            min(col("estimated_completion_minutes")).alias("min_estimated_completion_minutes"),
        )
    )

    tiered = windowed.withColumn(
        "risk_tier",
        when(
            (col("avg_session_buffer") < 0)
            | (col("avg_charger_temperature") > col("temp_threshold")),
            "RED",
        )
        .when(
            (col("avg_session_buffer") < col("session_buffer_threshold"))
            | (col("avg_charger_temperature") > col("temp_threshold") * 0.9),
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
        col("charger_id"),
        col("risk_tier"),
        col("avg_session_buffer").cast(IntegerType()).alias("session_buffer"),
        col("avg_charger_temperature").alias("avg_temperature"),
        when(
            col("risk_tier") == "RED",
            when(
                col("avg_session_buffer") < 0,
                concat(
                    lit("Session projected to overrun by "),
                    (col("avg_session_buffer") * -1).cast("integer").cast("string"),
                    lit("min"),
                ),
            ).otherwise(
                concat(
                    lit("Charger temperature exceeded threshold: "),
                    col("avg_charger_temperature").cast("string"),
                    lit("C"),
                )
            ),
        )
        .otherwise(
            when(
                col("avg_session_buffer") < col("session_buffer_threshold"),
                concat(
                    lit("Session buffer below threshold: "),
                    col("avg_session_buffer").cast("integer").cast("string"),
                    lit("min"),
                ),
            ).otherwise(
                concat(
                    lit("Charger temperature approaching threshold: "),
                    col("avg_charger_temperature").cast("string"),
                    lit("C"),
                )
            )
        )
        .alias("reason"),
        lit("CHARGING").alias("session_state"),
        col("session_id"),
        col("connector_type"),
        col("energy_requested_kwh"),
        col("user_tier"),
        col("charging_speed"),
        col("site_region"),
        col("charger_lat"),
        col("charger_lng"),
        col("rated_power_kw"),
        col("site_id"),
        col("max_session_progress").alias("session_progress"),
        col("max_power_output_kw").alias("power_output_kw"),
        col("max_energy_delivered_kwh").alias("energy_delivered_kwh"),
        col("min_estimated_completion_minutes").alias("estimated_completion_minutes"),
    )

    last_tiers: dict[tuple[str, str], str] = {}

    def write_on_transition(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return

        rows = batch_df.collect()
        new_alerts = []
        for row in rows:
            charger_id = row["charger_id"]
            session_id = row["session_id"]
            tier = row["risk_tier"]
            key = (charger_id, session_id)
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
            col("charger_id").cast(StringType()).alias("key"),
            to_json(struct(*[col(c) for c in new_df.columns])).alias("value"),
        )

        kafka_output.write.format("kafka") \
            .option("kafka.bootstrap.servers", "localhost:9093") \
            .option("topic", "risk-alerts") \
            .save()

    def write_stats(batch_df, batch_id):
        if batch_df.rdd.isEmpty():
            return

        r = redis_client.Redis(host="localhost", port=6379, decode_responses=True)
        now = datetime.now(timezone.utc).isoformat()

        # By region
        for row in batch_df.groupBy("site_region").agg(
            count("*").alias("active_sessions"),
            avg("charger_temperature").alias("avg_temperature"),
            avg("session_progress").alias("avg_session_progress"),
            avg("session_buffer").alias("avg_session_buffer"),
            avg("power_output_kw").alias("avg_power_kw"),
        ).collect():
            r.hset(f"stats:region:{row.site_region}", mapping={
                "active_sessions": int(row.active_sessions),
                "avg_temperature": round(float(row.avg_temperature), 2),
                "avg_session_progress": round(float(row.avg_session_progress), 4),
                "avg_session_buffer": round(float(row.avg_session_buffer), 2),
                "avg_power_kw": round(float(row.avg_power_kw), 2),
                "last_update": now,
            })

        # By connector type
        for row in batch_df.groupBy("connector_type").agg(
            count("*").alias("active_sessions"),
            avg("power_output_kw").alias("avg_power_kw"),
            avg("rated_power_kw").alias("avg_rated_power_kw"),
            avg("energy_delivered_kwh").alias("avg_energy_kwh"),
        ).collect():
            rated = float(row.avg_rated_power_kw) if row.avg_rated_power_kw else 0
            actual = float(row.avg_power_kw) if row.avg_power_kw else 0
            utilization = round((actual / rated * 100), 2) if rated else 0
            r.hset(f"stats:connector:{row.connector_type}", mapping={
                "active_sessions": int(row.active_sessions),
                "avg_power_kw": round(actual, 2),
                "avg_rated_power_kw": round(rated, 2),
                "avg_utilization_pct": utilization,
                "avg_energy_kwh": round(float(row.avg_energy_kwh), 2),
                "last_update": now,
            })

        # Network-wide
        row = batch_df.agg(
            count("*").alias("total_active_sessions"),
            avg("charger_temperature").alias("avg_temperature"),
            avg("session_buffer").alias("avg_session_buffer"),
            avg("power_output_kw").alias("avg_power_kw"),
        ).collect()[0]
        r.hset("stats:network", mapping={
            "total_active_sessions": int(row.total_active_sessions),
            "avg_temperature": round(float(row.avg_temperature), 2),
            "avg_session_buffer": round(float(row.avg_session_buffer), 2),
            "avg_power_kw": round(float(row.avg_power_kw), 2),
            "last_update": now,
        })

    query = (
        alert_records.writeStream
        .foreachBatch(write_on_transition)
        .option("checkpointLocation", "/tmp/logishield-checkpoints/risk-alerts")
        .outputMode("append")
        .trigger(processingTime="5 seconds")
        .start()
    )

    stats_query = (
        charging.writeStream
        .foreachBatch(write_stats)
        .option("checkpointLocation", "/tmp/logishield-checkpoints/charger-stats")
        .outputMode("update")
        .trigger(processingTime="5 seconds")
        .start()
    )

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    run()
