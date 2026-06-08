WITH source AS (
    SELECT *
    FROM {{ source('raw', 'stg_raw_checks') }}
),

renamed AS (
    SELECT
        CAST(source_row_id AS BIGINT) AS source_row_id,
        CAST(accounting_date AS DATE) AS accounting_date,
        CAST(check_id AS BIGINT) AS check_id,
        CAST(open_ts AS TIMESTAMP) AS open_ts,
        CAST(open_hour AS INTEGER) AS open_hour,
        CAST(close_ts AS TIMESTAMP) AS close_ts,
        CAST(close_hour AS INTEGER) AS close_hour,
        NULLIF(TRIM(dish_name), '') AS dish_name,
        NULLIF(TRIM(dish_category), '') AS dish_category,
        NULLIF(TRIM(order_type), '') AS order_type,
        CAST(qty AS DOUBLE) AS qty,
        CAST(guest_count AS INTEGER) AS guest_count,
        CAST(amount_discount_rub AS DOUBLE) AS amount_discount_rub,
        source_system,
        source_file,
        source_sheet,
        source_period,
        CAST(source_row_number AS BIGINT) AS source_row_number,
        CAST(source_loaded_at AS TIMESTAMP) AS source_loaded_at,
        CAST(extracted_at AS TIMESTAMP) AS extracted_at
    FROM source
),

final AS (
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
        REGEXP_REPLACE(dish_category, '^Д\\.', '') AS category_clean,
        CASE WHEN dish_category LIKE 'Д.%' THEN TRUE ELSE FALSE END AS is_delivery_category,
        order_type,
        {{ channel_group('order_type') }} AS channel_group,
        qty,
        guest_count,
        amount_discount_rub,
        {{ is_service_item('dish_name') }} AS is_service_item,
        CASE
            WHEN dish_category = 'МОДИФИКАТОРЫ' AND COALESCE(amount_discount_rub, 0) = 0 THEN TRUE
            ELSE FALSE
        END AS is_zero_amount_modifier,
        DATE_DIFF('minute', open_ts, close_ts) AS check_duration_minutes,
        CASE
            WHEN DATE_DIFF('hour', open_ts, close_ts) > 12 THEN TRUE
            ELSE FALSE
        END AS is_long_duration_check,
        CASE
            WHEN open_ts < CAST(accounting_date AS TIMESTAMP) - INTERVAL 3 DAY THEN TRUE
            ELSE FALSE
        END AS is_suspicious_open_date,
        CASE
            WHEN COALESCE(qty, 0) = 0 THEN TRUE
            ELSE FALSE
        END AS is_zero_qty_row,
        source_system,
        source_file,
        source_sheet,
        source_period,
        source_row_number,
        source_loaded_at,
        extracted_at
    FROM renamed
)

SELECT *
FROM final
