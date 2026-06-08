WITH bounds AS (
    SELECT
        MIN(date_value) AS min_date,
        MAX(date_value) AS max_date
    FROM (
        SELECT accounting_date AS date_value FROM {{ ref('stg_checks') }}
        UNION ALL
        SELECT plan_date AS date_value FROM {{ ref('stg_plan_daily') }}
    ) AS dates
),

date_spine AS (
    SELECT generated_date::DATE AS date_key
    FROM bounds,
    GENERATE_SERIES(min_date, max_date, INTERVAL 1 DAY) AS t(generated_date)
)

SELECT
    date_key,
    EXTRACT(year FROM date_key) AS year,
    EXTRACT(month FROM date_key) AS month,
    EXTRACT(day FROM date_key) AS day,
    STRFTIME(date_key, '%Y-%m') AS year_month,
    STRFTIME(date_key, '%A') AS weekday_name,
    EXTRACT(isodow FROM date_key) AS iso_weekday,
    CASE WHEN EXTRACT(isodow FROM date_key) IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend
FROM date_spine
