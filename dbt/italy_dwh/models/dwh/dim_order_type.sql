SELECT DISTINCT
    MD5(COALESCE(order_type, '') || '|' || COALESCE(channel_group, '')) AS order_type_key,
    order_type,
    channel_group
FROM {{ ref('stg_checks') }}
WHERE order_type IS NOT NULL
