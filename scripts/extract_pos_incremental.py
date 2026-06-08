from __future__ import annotations

import os
import duckdb

from common import DWH_DB_PATH, POS_SOURCE_DB_PATH, ensure_runtime_dir

SOURCE_NAME = "pos.check_items"
RAW_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS raw.stg_raw_checks (
    source_row_id BIGINT,
    accounting_date DATE,
    check_id BIGINT,
    open_ts TIMESTAMP,
    open_hour INTEGER,
    close_ts TIMESTAMP,
    close_hour INTEGER,
    dish_name VARCHAR,
    dish_category VARCHAR,
    order_type VARCHAR,
    qty DOUBLE,
    guest_count INTEGER,
    amount_discount_rub DOUBLE,
    source_system VARCHAR,
    source_file VARCHAR,
    source_sheet VARCHAR,
    source_period VARCHAR,
    source_row_number BIGINT,
    source_loaded_at TIMESTAMP,
    extracted_at TIMESTAMP
)
"""

PLAN_TABLE_DDL = """
CREATE TABLE IF NOT EXISTS raw.stg_plan_daily (
    plan_date DATE,
    plan_total_amount DOUBLE,
    plan_restaurant_amount DOUBLE,
    plan_banquet_own_amount DOUBLE,
    plan_banquet_cb_amount DOUBLE,
    plan_aggregator_amount DOUBLE,
    plan_pickup_amount DOUBLE,
    plan_delivery_amount DOUBLE,
    plan_avg_check_restaurant DOUBLE,
    plan_avg_check_delivery DOUBLE,
    plan_avg_guest_restaurant DOUBLE,
    plan_avg_guest_delivery DOUBLE,
    plan_avg_guest_banquet DOUBLE,
    source_file VARCHAR,
    source_sheet VARCHAR,
    loaded_at TIMESTAMP
)
"""


def main() -> None:
    ensure_runtime_dir()
    if not POS_SOURCE_DB_PATH.exists():
        raise FileNotFoundError(
            f"POS source DB does not exist: {POS_SOURCE_DB_PATH}. Run scripts/bootstrap_sources.py first."
        )

    full_refresh = os.getenv("FULL_REFRESH", "0") == "1"

    with duckdb.connect(str(DWH_DB_PATH)) as con:
        con.execute("CREATE SCHEMA IF NOT EXISTS raw")
        con.execute(
            """
            CREATE TABLE IF NOT EXISTS raw.etl_checkpoints (
                source_name VARCHAR PRIMARY KEY,
                watermark BIGINT,
                updated_at TIMESTAMP
            )
            """
        )
        con.execute(RAW_TABLE_DDL)
        con.execute(PLAN_TABLE_DDL)

        if full_refresh:
            con.execute("DELETE FROM raw.stg_raw_checks")
            con.execute("DELETE FROM raw.etl_checkpoints WHERE source_name = ?", [SOURCE_NAME])

        checkpoint = con.execute(
            "SELECT COALESCE(MAX(watermark), 0) FROM raw.etl_checkpoints WHERE source_name = ?",
            [SOURCE_NAME],
        ).fetchone()[0]

        con.execute(f"ATTACH '{POS_SOURCE_DB_PATH.as_posix()}' AS pos_src (READ_ONLY)")
        max_source_row_id = con.execute(
            "SELECT COALESCE(MAX(source_row_id), 0) FROM pos_src.pos.check_items"
        ).fetchone()[0]

        inserted = con.execute(
            f"""
            INSERT INTO raw.stg_raw_checks
            SELECT
                source_row_id,
                accounting_date,
                check_id,
                open_ts,
                open_hour,
                close_ts,
                close_hour,
                dish_name,
                dish_category,
                order_type,
                qty,
                guest_count,
                amount_discount_rub,
                source_system,
                source_file,
                source_sheet,
                source_period,
                source_row_number,
                source_loaded_at,
                CURRENT_TIMESTAMP AS extracted_at
            FROM pos_src.pos.check_items
            WHERE source_row_id > {int(checkpoint)}
            """
        ).rowcount

        con.execute(
            """
            INSERT OR REPLACE INTO raw.etl_checkpoints (source_name, watermark, updated_at)
            VALUES (?, ?, CURRENT_TIMESTAMP)
            """,
            [SOURCE_NAME, max_source_row_id],
        )

    print(f"DWH DB: {DWH_DB_PATH}")
    print(f"Previous watermark: {checkpoint}")
    print(f"Current source watermark: {max_source_row_id}")
    print(f"Rows inserted into raw.stg_raw_checks: {inserted}")


if __name__ == "__main__":
    main()
