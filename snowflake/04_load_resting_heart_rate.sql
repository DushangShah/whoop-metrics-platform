-- Raw table for resting heart rate produced by spark_jobs/resting_heart_rate.py.
-- Values are kept exactly as in the file, including 0.0 placeholders. dbt cleans them.
-- Safe to re-run: CREATE OR REPLACE rebuilds the table and resets load history.
USE ROLE TRANSFORMER;
USE WAREHOUSE DEV_WH;
USE SCHEMA WHOOP_METRICS.RAW;

CREATE FILE FORMAT IF NOT EXISTS PARQUET_FORMAT TYPE = PARQUET;

CREATE OR REPLACE TABLE RAW_RESTING_HEART_RATE (
  PARTICIPANT_ID STRING,
  READING_DATETIME_RAW STRING,
  INNER_DATE_RAW STRING,
  RESTING_HR_RAW FLOAT,
  ERROR_RAW FLOAT,
  SOURCE_FILE STRING,
  LOADED_AT TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP()
);

COPY INTO RAW_RESTING_HEART_RATE (
  PARTICIPANT_ID, READING_DATETIME_RAW, INNER_DATE_RAW, RESTING_HR_RAW, ERROR_RAW, SOURCE_FILE)
FROM (
  SELECT
    $1:participant_id::STRING,
    $1:reading_datetime_raw::STRING,
    $1:inner_date_raw::STRING,
    $1:resting_hr_raw::FLOAT,
    $1:error_raw::FLOAT,
    $1:source_file::STRING
  FROM @PMDATA_STAGE/resting_heart_rate/
)
FILE_FORMAT = (FORMAT_NAME = PARQUET_FORMAT);
