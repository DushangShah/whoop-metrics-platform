"""Flatten PMData sleep.json into one row per sleep record.

Keeps every record exactly as in the file, including the 63 repeated logIds.
Duplicates are resolved later in dbt, where a test can prove the rule.
Not read: levels.data and levels.shortData (stage-by-stage timelines).
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (BooleanType, DoubleType, LongType, MapType,
                               StringType, StructField, StructType)

DATA_DIR = os.path.expanduser("~/local-data/pmdata")
OUT_DIR = os.path.expanduser("~/local-data/processed/sleep_logs")

STAGE_GROUPS = ["deep", "light", "rem", "wake"]       # records of type "stages"
CLASSIC_GROUPS = ["asleep", "restless", "awake"]      # records of type "classic"

SCHEMA = StructType([
    StructField("logId", LongType()),
    StructField("dateOfSleep", StringType()),
    StructField("startTime", StringType()),
    StructField("endTime", StringType()),
    StructField("duration", DoubleType()),
    StructField("minutesToFallAsleep", DoubleType()),
    StructField("minutesAsleep", DoubleType()),
    StructField("minutesAwake", DoubleType()),
    StructField("minutesAfterWakeup", DoubleType()),
    StructField("timeInBed", DoubleType()),
    StructField("efficiency", DoubleType()),
    StructField("type", StringType()),
    StructField("infoCode", DoubleType()),
    StructField("mainSleep", BooleanType()),
    StructField("levels", StructType([
        # stage name -> {count, minutes, thirtyDayAvgMinutes}; works for both record types
        StructField("summary", MapType(StringType(), MapType(StringType(), DoubleType()))),
    ])),
])


def main():
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("sleep_logs")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.schema(SCHEMA)
        .option("multiLine", True)
        .option("mode", "FAILFAST")
        .json(os.path.join(DATA_DIR, "p*", "fitbit", "sleep.json"))
    )

    summ = F.col("levels.summary")
    stage_cols = []
    for g in STAGE_GROUPS:
        stage_cols += [
            summ[g]["count"].alias(f"{g}_count"),
            summ[g]["minutes"].alias(f"{g}_minutes"),
            summ[g]["thirtyDayAvgMinutes"].alias(f"{g}_30d_avg"),
        ]
    classic_cols = []
    for g in CLASSIC_GROUPS:
        classic_cols += [
            summ[g]["count"].alias(f"{g}_count"),
            summ[g]["minutes"].alias(f"{g}_minutes"),
        ]

    flat = raw.select(
        F.regexp_extract(F.col("_metadata.file_path"), r"/(p\d+)/fitbit/", 1).alias("participant_id"),
        F.col("logId").cast("string").alias("log_id"),
        F.col("dateOfSleep").alias("date_of_sleep"),
        F.col("startTime").alias("start_time_raw"),
        F.col("endTime").alias("end_time_raw"),
        F.col("duration").alias("duration_ms"),
        F.col("minutesToFallAsleep").alias("minutes_to_fall_asleep"),
        F.col("minutesAsleep").alias("minutes_asleep"),
        F.col("minutesAwake").alias("minutes_awake"),
        F.col("minutesAfterWakeup").alias("minutes_after_wakeup"),
        F.col("timeInBed").alias("time_in_bed"),
        F.col("efficiency"),
        F.col("type").alias("sleep_type"),
        F.col("infoCode").alias("info_code"),
        F.col("mainSleep").alias("main_sleep"),
        *stage_cols,
        *classic_cols,
        F.to_json(summ).alias("levels_summary_json"),
        F.col("_metadata.file_path").alias("source_file"),
    ).cache()

    stages = F.col("sleep_type") == "stages"
    classic = F.col("sleep_type") == "classic"
    print("rows:", flat.count())
    print("participants:", flat.select("participant_id").distinct().count())
    print("unique log ids:", flat.select("log_id").distinct().count())
    print("rows with null log id:", flat.filter(F.col("log_id").isNull()).count())
    flat.groupBy("sleep_type").count().orderBy("sleep_type").show()
    flat.groupBy("main_sleep").count().orderBy("main_sleep").show()
    print("stages rows with classic columns filled:", flat.filter(stages & F.col("asleep_minutes").isNotNull()).count())
    print("classic rows with stage columns filled:", flat.filter(classic & F.col("deep_minutes").isNotNull()).count())
    print("stages rows missing a stage minutes value:", flat.filter(
        stages & (F.col("deep_minutes").isNull() | F.col("light_minutes").isNull()
                  | F.col("rem_minutes").isNull() | F.col("wake_minutes").isNull())).count())
    print("classic rows missing a classic minutes value:", flat.filter(
        classic & (F.col("asleep_minutes").isNull() | F.col("restless_minutes").isNull()
                   | F.col("awake_minutes").isNull())).count())

    flat.coalesce(1).write.mode("overwrite").parquet(OUT_DIR)
    print("wrote Parquet to", OUT_DIR)
    spark.stop()


if __name__ == "__main__":
    main()
