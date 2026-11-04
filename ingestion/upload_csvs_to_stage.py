"""Upload PMData CSV files to the Snowflake internal stage, one folder per participant.

Needs SNOWFLAKE_ACCOUNT and SNOWFLAKE_PRIVATE_KEY_PATH set in the environment.
Safe to re-run: OVERWRITE=TRUE replaces files that already exist on the stage.
"""
import glob
import os

import snowflake.connector

DATA_DIR = os.path.expanduser("~/local-data/pmdata")

# (glob pattern under each participant, folder name on the stage)
UPLOADS = [
    ("fitbit/sleep_score.csv", "sleep_score"),
    ("pmsys/wellness.csv", "wellness"),
]


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
        for pattern, stage_folder in UPLOADS:
            paths = sorted(glob.glob(os.path.join(DATA_DIR, "p*", pattern)))
            print(f"{stage_folder}: found {len(paths)} files")
            for path in paths:
                participant = path.split(os.sep)[-3]
                cur.execute(
                    f"PUT 'file://{path}' @PMDATA_STAGE/{stage_folder}/{participant}/ "
                    "AUTO_COMPRESS=TRUE OVERWRITE=TRUE"
                )
                print(f"  uploaded {participant}/{os.path.basename(path)}")
    finally:
        cur.close()
        conn.close()


if __name__ == "__main__":
    main()