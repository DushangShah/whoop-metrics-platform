-- Raw table for heart rate readings produced by spark_jobs/heart_rate.py.
-- Every reading is kept, including repeated timestamps. dbt removes repeats.
-- Safe to re-run: CREATE OR REPLACE rebuilds the table and resets load history.
USE ROLE TRANSFORMER;
USE WAREHOUSE DEV_WH;
USE SCHEMA WHOOP_METRICS.RAW;

CREATE FILE FORMAT IF NOT EXISTS PARQUET_FORMAT TYPE = PARQUET;

CREATE OR REPLACE TABLE RAW_HEART_RATE (
  PARTICIPANT_ID STRING,
  READING_TIME_RAW STRING,
  BPM INTEGER,
  CONFIDENCE INTEGER,
  LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COPY INTO RAW_HEART_RATE (PARTICIPANT_ID, READING_TIME_RAW, BPM, CONFIDENCE)
FROM (
  SELECT
    $1:participant_id::STRING,
    $1:reading_time_raw::STRING,
    $1:bpm::INTEGER,
    $1:confidence::INTEGER
  FROM @PMDATA_STAGE/heart_rate/
)
FILE_FORMAT = (FORMAT_NAME = PARQUET_FORMAT);
