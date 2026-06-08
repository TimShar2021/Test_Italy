from __future__ import annotations

import pandas as pd
import duckdb

from common import FACT_SHEETS, POS_SOURCE_DB_PATH, ensure_runtime_dir, read_fact_sheet


def main() -> None:
    ensure_runtime_dir()
    frames: list[pd.DataFrame] = []
    source_row_id = 1

    for sheet_name, period in FACT_SHEETS.items():
        df = read_fact_sheet(sheet_name, period)
        df.insert(0, "source_row_id", range(source_row_id, source_row_id + len(df)))
        source_row_id += len(df)
        frames.append(df)

    all_facts = pd.concat(frames, ignore_index=True)
    all_facts["source_loaded_at"] = pd.Timestamp.utcnow().tz_localize(None)

    with duckdb.connect(str(POS_SOURCE_DB_PATH)) as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS pos")
        con.register("all_facts", all_facts)
        con.execute("CREATE OR REPLACE TABLE pos.check_items AS SELECT * FROM all_facts")
        row_count = con.execute("SELECT COUNT(*) FROM pos.check_items").fetchone()[0]
        period_stats = con.execute(
            """
            SELECT source_period, COUNT(*) AS rows_count, COUNT(DISTINCT check_id) AS checks_count
            FROM pos.check_items
            GROUP BY 1
            ORDER BY 1
            """
        ).fetchall()

    print(f"POS source DB created: {POS_SOURCE_DB_PATH}")
    print(f"Rows loaded into pos.check_items: {row_count}")
    print("Period stats:")
    for row in period_stats:
        print(row)


if __name__ == "__main__":
    main()
