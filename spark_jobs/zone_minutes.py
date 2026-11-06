"""Flatten PMData time_in_heart_rate_zones.json into one tidy table.

Reads every participant's file, turns the nested JSON into columns, fills missing zone
keys with 0 (remembering that they were missing), and writes Parquet outside the repo.
"""
import os

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import DoubleType, StringType, StructField, StructType

DATA_DIR = os.path.expanduser("~/local-data/pmdata")
OUT_DIR = os.path.expanduser("~/local-data/processed/zone_minutes")

# output column name -> key name in the JSON
ZONE_KEYS = {
    "minutes_below_zone_1": "BELOW_DEFAULT_ZONE_1",
    "minutes_zone_1": "IN_DEFAULT_ZONE_1",
    "minutes_zone_2": "IN_DEFAULT_ZONE_2",
    "minutes_zone_3": "IN_DEFAULT_ZONE_3",
}

# The shape we expect. A zone key that is missing in a file becomes NULL.
SCHEMA = StructType([
    StructField("dateTime", StringType()),
    StructField("value", StructType([
        StructField("valuesInZones", StructType(
            [StructField(key, DoubleType()) for key in ZONE_KEYS.values()]
        )),
    ])),
])


def main():
    spark = (
        SparkSession.builder.master("local[*]")
        .appName("zone_minutes")
        .config("spark.driver.bindAddress", "127.0.0.1")
        .config("spark.driver.host", "127.0.0.1")
        .config("spark.sql.session.timeZone", "UTC")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")

    raw = (
        spark.read.schema(SCHEMA)
        .option("multiLine", True)      # each file is one JSON list, not one object per line
        .option("mode", "FAILFAST")     # stop loudly on a broken file
        .json(os.path.join(DATA_DIR, "p*", "fitbit", "time_in_heart_rate_zones.json"))
    )

    zones = F.col("value.valuesInZones")
    names = list(ZONE_KEYS)
    flat = raw.select(
        F.regexp_extract(F.col("_metadata.file_path"), r"/(p\d+)/fitbit/", 1).alias("participant_id"),
        F.to_date(F.col("dateTime"), "yyyy-MM-dd HH:mm:ss").alias("zone_date"),
        *[zones[key].alias(name) for name, key in ZONE_KEYS.items()],
        F.col("_metadata.file_path").alias("source_file"),
    )

    # Remember which rows had a missing zone key BEFORE we fill them.
    flat = flat.withColumn(
        "any_zone_missing",
        F.greatest(*[F.col(n).isNull().cast("int") for n in names]) == 1,
    )
    for n in names:
        flat = flat.withColumn(n, F.coalesce(F.col(n), F.lit(0.0)))
    flat = flat.withColumn("total_minutes", sum(F.col(n) for n in names))
    flat.cache()

    print("rows:", flat.count())
    print("participants:", flat.select("participant_id").distinct().count())
    print("rows with a null date:", flat.filter(F.col("zone_date").isNull()).count())
    print("rows with a missing zone key:", flat.filter("any_zone_missing").count())
    print("rows summing to exactly 1440 minutes:", flat.filter(F.col("total_minutes") == 1440).count())
    print("rows over 1440 minutes (impossible):", flat.filter(F.col("total_minutes") > 1440).count())
    flat.agg(
        F.min("total_minutes").alias("min_total"),
        F.expr("percentile_approx(total_minutes, 0.5)").alias("median_total"),
        F.max("total_minutes").alias("max_total"),
    ).show()

    # The date goes out as text (yyyy-MM-dd). dbt converts it to a real date later.
    out = flat.withColumn("zone_date", F.date_format("zone_date", "yyyy-MM-dd"))
    out.coalesce(1).write.mode("overwrite").parquet(OUT_DIR)
    print("wrote Parquet to", OUT_DIR)
    spark.stop()


if __name__ == "__main__":
    main()
