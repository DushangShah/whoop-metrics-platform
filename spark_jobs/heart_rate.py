"""Flatten PMData heart_rate.json (about 21 million readings) into one tidy table.

Keeps every reading exactly as in the file, including repeated timestamps.
Time stays as text (local clock, no time zone). dbt parses it and removes repeats.
source_file is not kept: it is always <participant>/fitbit/heart_rate.json.
Writes Parquet first, then reads it back to run the checks.
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import LongType, StringType, StructField, StructType

DATA_DIR = os.path.expanduser("~/local-data/pmdata")
OUT_DIR = os.path.expanduser("~/local-data/processed/heart_rate")

SCHEMA = StructType([
    StructField("dateTime", StringType()),
    StructField("value", StructType([
        StructField("bpm", LongType()),
        StructField("confidence", LongType()),
    ])),
])


def main():
    spark = (
        SparkSession.builder.master("local[2]")
        .appName("heart_rate")
        .config("spark.driver.memory", "4g")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.schema(SCHEMA)
        .option("multiLine", True)      # each file is one JSON list
        .option("mode", "FAILFAST")     # stop loudly on a broken file
        .json(os.path.join(DATA_DIR, "p*", "fitbit", "heart_rate.json"))
    )
    flat = raw.select(
        F.regexp_extract(F.col("_metadata.file_path"), r"/(p\d+)/fitbit/", 1).alias("participant_id"),
        F.col("dateTime").alias("reading_time_raw"),
        F.col("value.bpm").cast("int").alias("bpm"),
        F.col("value.confidence").cast("int").alias("confidence"),
    )
    flat.coalesce(4).write.mode("overwrite").parquet(OUT_DIR)
    print("wrote Parquet to", OUT_DIR)

    chk = spark.read.parquet(OUT_DIR)
    print("rows:", chk.count())
    print("participants:", chk.select("participant_id").distinct().count())
    per = chk.groupBy("participant_id").count().collect()
    print("rows per participant:", dict(sorted((r["participant_id"], r["count"]) for r in per)))
    print("rows with a null time, bpm or confidence:", chk.filter(
        F.col("reading_time_raw").isNull() | F.col("bpm").isNull() | F.col("confidence").isNull()).count())
    print("bpm min/max:", chk.agg(F.min("bpm"), F.max("bpm")).first())
    print("bpm = 30:", chk.filter("bpm = 30").count(), "| bpm > 200:", chk.filter("bpm > 200").count())
    conf = chk.groupBy("confidence").count().collect()
    print("rows per confidence:", dict(sorted((r["confidence"], r["count"]) for r in conf)))

    dups = (
        chk.groupBy("participant_id", "reading_time_raw")
        .agg(F.count("*").alias("n"), F.countDistinct("bpm").alias("bpms"),
             F.countDistinct("confidence").alias("confs"))
        .filter("n > 1").cache()
    )
    print("repeated (participant, time) pairs:", dups.count(),
          "| extra rows:", dups.agg(F.sum(F.col("n") - 1)).first()[0])
    print("repeated pairs with different bpm:", dups.filter("bpms > 1").count(),
          "| with different confidence:", dups.filter("confs > 1").count())
    spark.stop()


if __name__ == "__main__":
    main()
