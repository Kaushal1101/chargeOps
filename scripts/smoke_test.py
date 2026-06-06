import sys

from pyspark.sql import SparkSession


def main() -> int:
    spark = (
        SparkSession.builder.appName("fleet-telemetry-smoke-test")
        .config(
            "spark.jars.packages",
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1",
        )
        .getOrCreate()
    )

    try:
        df = (
            spark.read.format("kafka")
            .option("kafka.bootstrap.servers", "localhost:9093")
            .option("subscribe", "fleet-telemetry")
            .option("startingOffsets", "earliest")
            .load()
        )
        row_count = df.count()
        print(f"[SMOKE TEST] Row count: {row_count}")
        print(
            "[SMOKE TEST] PASSED \u2014 Spark connected to Kafka and accessed fleet-telemetry"
        )
        return 0
    except Exception as e:
        print(f"[SMOKE TEST] FAILED \u2014 {e}")
        return 1
    finally:
        spark.stop()


if __name__ == "__main__":
    sys.exit(main())
