from __future__ import annotations

import duckdb

from common import DWH_DB_PATH, ensure_runtime_dir, read_plan_sheet


def main() -> None:
    ensure_runtime_dir()
    df = read_plan_sheet()

    with duckdb.connect(str(DWH_DB_PATH)) as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")
        con.register("plan_daily", df)
        con.execute("CREATE OR REPLACE TABLE raw.stg_plan_daily AS SELECT * FROM plan_daily")
        row_count = con.execute("SELECT COUNT(*) FROM raw.stg_plan_daily").fetchone()[0]

    print(f"DWH DB: {DWH_DB_PATH}")
    print(f"Rows loaded into raw.stg_plan_daily: {row_count}")


if __name__ == "__main__":
    main()
