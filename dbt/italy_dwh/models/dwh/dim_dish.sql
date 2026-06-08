WITH dishes AS (
    SELECT DISTINCT
        dish_name,
        category_clean,
        BOOL_OR(is_delivery_category) AS has_delivery_category,
        BOOL_OR(is_service_item) AS is_service_item
    FROM {{ ref('stg_checks') }}
    WHERE dish_name IS NOT NULL
    GROUP BY 1, 2
)

SELECT
    MD5(COALESCE(dish_name, '') || '|' || COALESCE(category_clean, '')) AS dish_key,
    dish_name,
    category_clean,
    has_delivery_category,
    is_service_item
FROM dishes
