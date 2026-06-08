SELECT
    CAST(plan_date AS DATE) AS plan_date,
    CAST(plan_total_amount AS DOUBLE) AS plan_total_amount,
    CAST(plan_restaurant_amount AS DOUBLE) AS plan_restaurant_amount,
    CAST(plan_banquet_own_amount AS DOUBLE) AS plan_banquet_own_amount,
    CAST(plan_banquet_cb_amount AS DOUBLE) AS plan_banquet_cb_amount,
    CAST(plan_aggregator_amount AS DOUBLE) AS plan_aggregator_amount,
    CAST(plan_pickup_amount AS DOUBLE) AS plan_pickup_amount,
    CAST(plan_delivery_amount AS DOUBLE) AS plan_delivery_amount,
    CAST(plan_avg_check_restaurant AS DOUBLE) AS plan_avg_check_restaurant,
    CAST(plan_avg_check_delivery AS DOUBLE) AS plan_avg_check_delivery,
    CAST(plan_avg_guest_restaurant AS DOUBLE) AS plan_avg_guest_restaurant,
    CAST(plan_avg_guest_delivery AS DOUBLE) AS plan_avg_guest_delivery,
    CAST(plan_avg_guest_banquet AS DOUBLE) AS plan_avg_guest_banquet,
    source_file,
    source_sheet,
    CAST(loaded_at AS TIMESTAMP) AS loaded_at
FROM {{ source('raw', 'stg_plan_daily') }}
