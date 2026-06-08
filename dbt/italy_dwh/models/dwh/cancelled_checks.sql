WITH check_versions AS (
    SELECT
        check_id,
        COUNT(*) AS raw_rows_count,
        SUM(CASE WHEN COALESCE(qty, 0) > 0 THEN 1 ELSE 0 END) AS positive_qty_rows_count,
        MAX(close_ts) AS last_close_ts,
        MIN(accounting_date) AS first_accounting_date,
        MAX(accounting_date) AS last_accounting_date
    FROM {{ ref('stg_checks') }}
    GROUP BY 1
)

SELECT
    check_id,
    raw_rows_count,
    last_close_ts,
    first_accounting_date,
    last_accounting_date,
    TRUE AS is_cancelled
FROM check_versions
WHERE positive_qty_rows_count = 0
