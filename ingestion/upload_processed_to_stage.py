"""Upload Spark's Parquet output to the Snowflake internal stage.

Needs SNOWFLAKE_ACCOUNT and SNOWFLAKE_PRIVATE_KEY_PATH set in the environment.
Safe to re-run: the stage folder is emptied first, so old part files never get loaded twice.
"""
import glob
import os

import snowflake.connector

PROCESSED_DIR = os.path.expanduser("~/local-data/processed")
FOLDERS = ["zone_minutes"]  # each folder becomes @PMDATA_STAGE/<folder>/


def main():
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
        for folder in FOLDERS:
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
    main()
