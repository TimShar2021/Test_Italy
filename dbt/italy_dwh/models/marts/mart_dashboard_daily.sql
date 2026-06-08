WITH current_daily AS (
    SELECT *
    FROM {{ ref('mart_plan_fact_daily') }}
),

previous_year_daily AS (
    SELECT
        revenue_date + INTERVAL 1 YEAR AS revenue_date,
        fact_total_amount AS prev_year_fact_total_amount,
        checks_count AS prev_year_checks_count,
        guests_count AS prev_year_guests_count,
        avg_check_per_order AS prev_year_avg_check_per_order,
        avg_check_per_guest AS prev_year_avg_check_per_guest
    FROM {{ ref('mart_plan_fact_daily') }}
),

final AS (
    SELECT
        c.revenue_date,
        c.checks_count,
        c.guests_count,
        c.fact_total_amount,
        c.plan_total_amount,
        c.plan_fact_delta_amount,
        c.plan_completion_rate,
        c.fact_restaurant_amount,
        c.plan_restaurant_amount,
        c.fact_delivery_amount,
        c.plan_delivery_and_aggregator_amount,
        c.fact_pickup_amount,
        c.plan_pickup_amount,
        c.fact_banquet_amount,
        c.plan_banquet_amount,
        c.fact_other_amount,
        c.avg_check_per_order,
        c.avg_check_per_guest,
        py.prev_year_fact_total_amount,
        py.prev_year_checks_count,
        py.prev_year_guests_count,
        py.prev_year_avg_check_per_order,
        py.prev_year_avg_check_per_guest,
        ROUND(
            (c.fact_total_amount - py.prev_year_fact_total_amount)
            / NULLIF(py.prev_year_fact_total_amount, 0),
            4
        ) AS yoy_growth_rate,
        ROUND(c.checks_count - py.prev_year_checks_count, 2) AS yoy_checks_delta,
        ROUND(c.avg_check_per_order - py.prev_year_avg_check_per_order, 2) AS yoy_avg_check_delta
    FROM current_daily AS c
    LEFT JOIN previous_year_daily AS py
        ON c.revenue_date = py.revenue_date
)

SELECT *
FROM final
