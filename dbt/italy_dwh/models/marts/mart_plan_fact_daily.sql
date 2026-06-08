WITH check_level AS (
    SELECT
        revenue_date,
        check_id,
        MAX(guest_count) AS guest_count,
        SUM(CASE WHEN is_turnover_item THEN amount_discount_rub ELSE 0 END) AS check_revenue,
        SUM(CASE WHEN channel_group = 'restaurant' AND is_turnover_item THEN amount_discount_rub ELSE 0 END) AS restaurant_revenue,
        SUM(CASE WHEN channel_group = 'delivery' AND is_turnover_item THEN amount_discount_rub ELSE 0 END) AS delivery_revenue,
        SUM(CASE WHEN channel_group = 'pickup' AND is_turnover_item THEN amount_discount_rub ELSE 0 END) AS pickup_revenue,
        SUM(CASE WHEN channel_group = 'banquet' AND is_turnover_item THEN amount_discount_rub ELSE 0 END) AS banquet_revenue,
        SUM(CASE WHEN channel_group = 'other' AND is_turnover_item THEN amount_discount_rub ELSE 0 END) AS other_revenue
    FROM {{ ref('fact_check_items') }}
    GROUP BY 1, 2
),

actual_daily AS (
    SELECT
        revenue_date,
        COUNT(*) AS checks_count,
        SUM(guest_count) AS guests_count,
        SUM(check_revenue) AS fact_total_amount,
        SUM(restaurant_revenue) AS fact_restaurant_amount,
        SUM(delivery_revenue) AS fact_delivery_amount,
        SUM(pickup_revenue) AS fact_pickup_amount,
        SUM(banquet_revenue) AS fact_banquet_amount,
        SUM(other_revenue) AS fact_other_amount,
        SUM(check_revenue) / NULLIF(COUNT(*), 0) AS avg_check_per_order,
        SUM(check_revenue) / NULLIF(SUM(guest_count), 0) AS avg_check_per_guest
    FROM check_level
    GROUP BY 1
),

final AS (
    SELECT
        COALESCE(a.revenue_date, p.plan_date) AS revenue_date,
        a.checks_count,
        a.guests_count,
        ROUND(a.fact_total_amount, 2) AS fact_total_amount,
        ROUND(p.plan_total_amount, 2) AS plan_total_amount,
        ROUND(a.fact_total_amount - p.plan_total_amount, 2) AS plan_fact_delta_amount,
        ROUND(a.fact_total_amount / NULLIF(p.plan_total_amount, 0), 4) AS plan_completion_rate,
        ROUND(a.fact_restaurant_amount, 2) AS fact_restaurant_amount,
        ROUND(p.plan_restaurant_amount, 2) AS plan_restaurant_amount,
        ROUND(a.fact_delivery_amount, 2) AS fact_delivery_amount,
        ROUND(p.plan_delivery_amount + p.plan_aggregator_amount, 2) AS plan_delivery_and_aggregator_amount,
        ROUND(a.fact_pickup_amount, 2) AS fact_pickup_amount,
        ROUND(p.plan_pickup_amount, 2) AS plan_pickup_amount,
        ROUND(a.fact_banquet_amount, 2) AS fact_banquet_amount,
        ROUND(p.plan_banquet_own_amount + p.plan_banquet_cb_amount, 2) AS plan_banquet_amount,
        ROUND(a.fact_other_amount, 2) AS fact_other_amount,
        ROUND(a.avg_check_per_order, 2) AS avg_check_per_order,
        ROUND(a.avg_check_per_guest, 2) AS avg_check_per_guest,
        p.plan_avg_check_restaurant,
        p.plan_avg_check_delivery,
        p.plan_avg_guest_restaurant,
        p.plan_avg_guest_delivery,
        p.plan_avg_guest_banquet
    FROM actual_daily AS a
    FULL OUTER JOIN {{ ref('stg_plan_daily') }} AS p
        ON a.revenue_date = p.plan_date
)

SELECT *
FROM final
