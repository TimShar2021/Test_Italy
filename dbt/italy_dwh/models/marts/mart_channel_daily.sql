WITH check_level AS (
    SELECT
        revenue_date,
        channel_group,
        check_id,
        MAX(guest_count) AS guest_count,
        SUM(CASE WHEN is_turnover_item THEN amount_discount_rub ELSE 0 END) AS check_revenue
    FROM {{ ref('fact_check_items') }}
    GROUP BY 1, 2, 3
)

SELECT
    revenue_date,
    channel_group,
    COUNT(*) AS checks_count,
    SUM(guest_count) AS guests_count,
    ROUND(SUM(check_revenue), 2) AS revenue_amount,
    ROUND(SUM(check_revenue) / NULLIF(COUNT(*), 0), 2) AS avg_check_per_order,
    ROUND(SUM(check_revenue) / NULLIF(SUM(guest_count), 0), 2) AS avg_revenue_per_guest
FROM check_level
GROUP BY 1, 2
