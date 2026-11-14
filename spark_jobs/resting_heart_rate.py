"""Flatten PMData resting_heart_rate.json into one tidy table.

One row per participant per day. Values stay as they are in the file (including 0.0
placeholders); cleaning happens later in dbt, where tests can prove it.
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

DATA_DIR = os.path.expanduser("~/local-data/pmdata")
OUT_DIR = os.path.expanduser("~/local-data/processed/resting_heart_rate")

SCHEMA = StructType([
    StructField("dateTime", StringType()),
    StructField("value", StructType([
        StructField("date", StringType()),
        StructField("value", DoubleType()),
        StructField("error", DoubleType()),
    ])),
])


def main():
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("resting_heart_rate")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.schema(SCHEMA)
        .option("multiLine", True)
        .option("mode", "FAILFAST")
        .json(os.path.join(DATA_DIR, "p*", "fitbit", "resting_heart_rate.json"))
    )

    flat = raw.select(
        F.regexp_extract(F.col("_metadata.file_path"), r"/(p\d+)/fitbit/", 1).alias("participant_id"),
        F.col("dateTime").alias("reading_datetime_raw"),
        F.col("value.date").alias("inner_date_raw"),
        F.col("value.value").alias("resting_hr_raw"),
        F.col("value.error").alias("error_raw"),
        F.col("_metadata.file_path").alias("source_file"),
    ).cache()

    hr = F.col("resting_hr_raw")
    print("rows:", flat.count())
    print("participants:", flat.select("participant_id").distinct().count())
    print("rows with null dateTime:", flat.filter(F.col("reading_datetime_raw").isNull()).count())
    print("rows with heart rate = 0:", flat.filter(hr == 0).count())
    print("rows with null inner date:", flat.filter(F.col("inner_date_raw").isNull()).count())
    print("rows with heart rate = 0 AND date present:", flat.filter((hr == 0) & F.col("inner_date_raw").isNotNull()).count())
    print("rows with heart rate > 0 AND date missing:", flat.filter((hr > 0) & F.col("inner_date_raw").isNull()).count())
    flat.filter(hr > 0).agg(
        F.min(hr).alias("min_hr_nonzero"),
        F.expr("percentile_approx(resting_hr_raw, 0.5)").alias("median_hr_nonzero"),
        F.max(hr).alias("max_hr_nonzero"),
        F.min("error_raw").alias("min_error"),
        F.max("error_raw").alias("max_error"),
    ).show()

    flat.coalesce(1).write.mode("overwrite").parquet(OUT_DIR)
    print("wrote Parquet to", OUT_DIR)
    spark.stop()


if __name__ == "__main__":
    main()
