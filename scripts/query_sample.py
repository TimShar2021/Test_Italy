from __future__ import annotations

import duckdb

from common import DWH_DB_PATH


def main() -> None:
    with duckdb.connect(str(DWH_DB_PATH), read_only=True) as con:
        for sql in [
            "SELECT * FROM mart.mart_plan_fact_daily ORDER BY revenue_date LIMIT 10",
            "SELECT * FROM mart.mart_channel_daily ORDER BY revenue_date, channel_group LIMIT 10",
            "SELECT * FROM mart.mart_avg_check_by_order_type ORDER BY revenue_date, order_type LIMIT 10",
        ]:
            print("\n" + sql)
            print(con.execute(sql).fetchdf())


if __name__ == "__main__":
    main()
