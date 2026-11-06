# Progress log

## 2026-11-02
- Downloaded PMData (16 participants, about 3 GB), kept it outside git and iCloud, linked at data/raw/pmdata
- Inspected layouts and formats, wrote docs/dataset.md
- Found quirks: missing zone keys, placeholder zero resting heart rates, mixed time formats, nested sleep JSON
- Learned: join on IDs when time zones are unclear, JSON lists need multiLine in Spark
- Next: load the first files into Snowflake

## 2026-11-03
- Installed Java 17, verified local Spark, pinned pandas below 3 for PySpark
- Created Snowflake file format CSV_FORMAT and internal stage PMDATA_STAGE under the TRANSFORMER role
- Wrote ingestion/upload_csvs_to_stage.py (key-pair login, one stage folder per participant), uploaded 32 CSV files
- Loaded RAW_SLEEP_SCORE (1,836 rows) and RAW_WELLNESS (1,747 rows) with COPY INTO, raw values kept as text, with source file and participant recorded
- Checked: 16 participants in both tables, and row counts match the published dataset description
- Learned: a stage is a loading dock, COPY INTO moves staged files into tables, METADATA$FILENAME records where each row came from, and every new Snowflake SQL file needs USE WAREHOUSE before querying tables

## 2026-11-05
- dbt build found 5 wellness rows with zeros on a 1 to 5 scale (all other tests passed)
- Inspected the rows, decided zeros mean not answered, updated the staging model, added a blank-entry guard test, documented the decision
- Learned: a failing test is information, so look at the rows before changing the test or the data
