"""
Job Spark Structured Streaming: seismes USGS, file source -> Parquet + Postgres.

Architecture (sans Kafka):

    earthquake-producer (long-running)
            |
            v
    /data/incoming/eq_<timestamp>.json        (JSONL, ecriture atomique)
            |
            v
    Spark Structured Streaming (ce fichier)
      - readStream.format("json") sur /data/incoming
      - enrichissement (depth_category, severity, is_significant)
            |
            +---> writeStream Parquet -> /data/clean/earthquakes/ (partitionne par event_date)
            |
            +---> foreachBatch -> Postgres (table earthquake_enriched via JDBC)

Une seule requete writeStream avec foreachBatch ecrit dans les DEUX sinks
(Parquet puis Postgres) pour chaque micro-batch. On NE peut PAS utiliser
deux writeStream distincts sur la meme readStream file source: ils
partageraient le sourceLog du FileStreamSource et se battraient pour le
mettre a jour, ce qui crashe la stream avec:
    IllegalStateException: Concurrent update to the log.
    Multiple streaming jobs detected for N
"""

from __future__ import annotations

import os

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
)

INCOMING_DIR = os.environ.get("INCOMING_DIR", "/data/incoming")
CLEAN_DIR = os.environ.get("CLEAN_DIR", "/data/clean/earthquakes")
CHECKPOINT_ROOT = os.environ.get("CHECKPOINT_ROOT", "/data/checkpoints/earthquake")

PG_URL = os.environ.get("PG_URL", "jdbc:postgresql://postgres:5432/earthquakes")
PG_USER = os.environ.get("PG_USER", "airflow")
PG_PASSWORD = os.environ.get("PG_PASSWORD", "airflow")
PG_TABLE = os.environ.get("PG_TABLE", "earthquake_enriched")
PG_STAGING_TABLE = f"{PG_TABLE}_stg"

EVENT_SCHEMA = StructType([
    StructField("event_id", StringType()),
    StructField("magnitude", DoubleType()),
    StructField("mag_type", StringType()),
    StructField("place", StringType()),
    StructField("event_time", StringType()),
    StructField("latitude", DoubleType()),
    StructField("longitude", DoubleType()),
    StructField("depth_km", DoubleType()),
    StructField("significance", IntegerType()),
    StructField("tsunami", IntegerType()),
    StructField("status", StringType()),
    StructField("type", StringType()),
    StructField("title", StringType()),
    StructField("ingested_at", StringType()),
])


def build_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("earthquake-structured-streaming")
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def read_incoming(spark: SparkSession) -> DataFrame:
    return (
        spark.readStream
        .schema(EVENT_SCHEMA)
        .option("maxFilesPerTrigger", 5)
        .option("cleanSource", "off")
        .json(INCOMING_DIR)
    )


def enrich(events: DataFrame) -> DataFrame:
    return (
        events
        .withColumn(
            "depth_category",
            F
            .when(F.col("depth_km") < 70, F.lit("Superficiel (0-70 km)"))
            .when(F.col("depth_km") < 300, F.lit("Intermediaire (70-300 km)"))
            .otherwise(F.lit("Profond (300+ km)")),
        )
        .withColumn(
            "severity",
            F
            .when(F.col("tsunami") == 1, F.lit("TSUNAMI"))
            .when(F.col("magnitude") >= 8.0, F.lit("Devastateur"))
            .when(F.col("magnitude") >= 7.0, F.lit("Majeur"))
            .when(F.col("magnitude") >= 6.0, F.lit("Fort"))
            .when(F.col("magnitude") >= 5.0, F.lit("Modere"))
            .otherwise(F.lit("Mineur")),
        )
        .withColumn("is_significant", (F.col("significance") > 500).cast("int"))
        .withColumn("event_ts", F.to_timestamp("event_time"))
        .withColumn("event_date", F.to_date("event_ts"))
        .withColumn("processed_at", F.current_timestamp())
    )


UPSERT_COLUMNS = [
    "event_id",
    "magnitude",
    "mag_type",
    "place",
    "event_ts",
    "latitude",
    "longitude",
    "depth_km",
    "significance",
    "tsunami",
    "status",
    "type",
    "title",
    "depth_category",
    "severity",
    "is_significant",
    "processed_at",
]


def _merge_staging_into_main(spark) -> None:
    """INSERT ... SELECT ... ON CONFLICT DO NOTHING via py4j JDBC.

    foreachBatch tourne cote driver donc on peut acquerir une connexion
    JDBC Java directement via spark._jvm. On merge puis tronque la
    staging pour garder son volume petit.
    """
    col_list = ", ".join(UPSERT_COLUMNS)
    merge_sql = (
        f"INSERT INTO {PG_TABLE} ({col_list}) "
        f"SELECT {col_list} FROM {PG_STAGING_TABLE} "
        f"ON CONFLICT (event_id) DO NOTHING"
    )
    truncate_sql = f"TRUNCATE TABLE {PG_STAGING_TABLE}"

    driver_manager = spark._jvm.java.sql.DriverManager
    conn = driver_manager.getConnection(PG_URL, PG_USER, PG_PASSWORD)
    try:
        conn.setAutoCommit(False)
        stmt = conn.createStatement()
        stmt.execute(merge_sql)
        stmt.execute(truncate_sql)
        conn.commit()
        stmt.close()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


def write_both(df: DataFrame):
    """Une seule writeStream qui ecrit dans Parquet ET Postgres.

    Pourquoi pas deux writeStream distincts: elles partageraient le
    sourceLog du FileStreamSource et se battraient pour le mettre a
    jour (IllegalStateException: Concurrent update to the log).

    Postgres via staging + ON CONFLICT DO NOTHING pour l'idempotence:
    les micro-batches peuvent etre rejoues (at-least-once cote sink)
    apres un crash sans crasher sur une PK violation.

    Ne PAS appeler batch_df.rdd.* ici: cloudpickle 2.0 bundled avec
    pyspark 3.3.0 n'est pas compatible avec le bytecode de Python 3.11.
    """

    def _batch(batch_df: DataFrame, batch_id: int) -> None:
        del batch_id
        batch_df.persist()
        try:
            (batch_df.write.mode("append").partitionBy("event_date").parquet(CLEAN_DIR))

            (
                batch_df
                .select(*UPSERT_COLUMNS)
                .write.format("jdbc")
                .option("url", PG_URL)
                .option("dbtable", PG_STAGING_TABLE)
                .option("user", PG_USER)
                .option("password", PG_PASSWORD)
                .option("driver", "org.postgresql.Driver")
                .mode("overwrite")
                .option("truncate", "true")
                .save()
            )

            _merge_staging_into_main(batch_df.sparkSession)
        finally:
            batch_df.unpersist()

    return (
        df.writeStream
        .foreachBatch(_batch)
        .option("checkpointLocation", f"{CHECKPOINT_ROOT}/combined")
        .outputMode("append")
        .trigger(processingTime="10 seconds")
        .queryName("sink-combined")
        .start()
    )


def main() -> None:
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    events = read_incoming(spark)
    enriched = enrich(events)

    q = write_both(enriched)
    print(f"[earthquake-streaming] combined query id = {q.id}")

    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
