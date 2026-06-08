SELECT
    revenue_date,
    category_clean,
    dish_name,
    SUM(qty) AS qty_sold,
    ROUND(SUM(amount_discount_rub), 2) AS dish_revenue_amount,
    COUNT(DISTINCT check_id) AS checks_count
FROM {{ ref('fact_check_items') }}
WHERE is_dish_sales_item
GROUP BY 1, 2, 3
