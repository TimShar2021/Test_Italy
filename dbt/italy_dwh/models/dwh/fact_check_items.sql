WITH checks_with_positive_qty AS (
    SELECT
        check_id,
        MAX(close_ts) AS final_close_ts
    FROM {{ ref('stg_checks') }}
    WHERE COALESCE(qty, 0) > 0
    GROUP BY 1
),

final_version_rows AS (
    SELECT s.*
    FROM {{ ref('stg_checks') }} AS s
    INNER JOIN checks_with_positive_qty AS f
        ON s.check_id = f.check_id
        AND s.close_ts = f.final_close_ts
    WHERE COALESCE(s.qty, 0) > 0
),

enriched AS (
    SELECT
        MD5(
            COALESCE(CAST(source_row_id AS VARCHAR), '') || '|' ||
            COALESCE(CAST(check_id AS VARCHAR), '') || '|' ||
            COALESCE(CAST(close_ts AS VARCHAR), '') || '|' ||
            COALESCE(dish_name, '') || '|' ||
            COALESCE(dish_category, '') || '|' ||
            COALESCE(order_type, '')
        ) AS check_item_key,
        source_row_id,
        accounting_date AS revenue_date,
        check_id,
        open_ts,
        close_ts,
        open_hour,
        close_hour,
        MD5(COALESCE(dish_name, '') || '|' || COALESCE(category_clean, '')) AS dish_key,
        MD5(COALESCE(order_type, '') || '|' || COALESCE(channel_group, '')) AS order_type_key,
        dish_name,
        dish_category,
        category_clean,
        is_delivery_category,
        order_type,
        channel_group,
        qty,
        guest_count,
        amount_discount_rub,
        is_service_item,
        is_zero_amount_modifier,
        CASE
            WHEN is_zero_amount_modifier THEN FALSE
            ELSE TRUE
        END AS is_turnover_item,
        CASE
            WHEN is_service_item OR is_zero_amount_modifier THEN FALSE
            ELSE TRUE
        END AS is_dish_sales_item,
        check_duration_minutes,
        is_long_duration_check,
        is_suspicious_open_date,
        source_period,
        source_sheet,
        extracted_at
    FROM final_version_rows
)

SELECT *
FROM enriched
