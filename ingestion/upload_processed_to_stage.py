"""Upload Spark's Parquet output to the Snowflake internal stage.

Usage: python ingestion/upload_processed_to_stage.py zone_minutes resting_heart_rate
Needs SNOWFLAKE_ACCOUNT and SNOWFLAKE_PRIVATE_KEY_PATH set in the environment.
Safe to re-run: each stage folder is emptied first, so old part files never get loaded twice.
"""
import glob
import os
import sys

import snowflake.connector

PROCESSED_DIR = os.path.expanduser("~/local-data/processed")


def main(folders):
    if not folders:
        raise SystemExit("Name at least one folder, for example: resting_heart_rate")
    conn = snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user="DBT_USER",
        private_key_file=os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"],
        role="TRANSFORMER",
        warehouse="DEV_WH",
        database="WHOOP_METRICS",
        schema="RAW",
    )
    cur = conn.cursor()
    try:
        for folder in folders:
            paths = sorted(glob.glob(os.path.join(PROCESSED_DIR, folder, "*.parquet")))
            if not paths:
                raise SystemExit(f"No Parquet files in {folder}. Run the Spark job first.")
            cur.execute(f"REMOVE @PMDATA_STAGE/{folder}/")
            for path in paths:
                cur.execute(
                    f"PUT 'file://{path}' @PMDATA_STAGE/{folder}/ "
                    "AUTO_COMPRESS=FALSE OVERWRITE=TRUE"
                )
                print(f"uploaded {folder}/{os.path.basename(path)}")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main(sys.argv[1:])
