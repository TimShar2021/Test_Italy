{% macro channel_group(order_type_column) -%}
CASE
    WHEN {{ order_type_column }} IN (
        'Приложение (доставка курьером)',
        'Сайт (доставка курьером)',
        'Доставка курьером',
        'Агрегатор (Курьером)',
        'Яндекс.ультима'
    ) THEN 'delivery'
    WHEN {{ order_type_column }} IN (
        'Приложение (самовывоз гостем)',
        'Сайт (самовывоз гостем)',
        'Самовывоз гостем',
        'Агрегатор (Самовывоз)'
    ) THEN 'pickup'
    WHEN {{ order_type_column }} IN (
        'Заказ в ресторане',
        'Заказ с собой',
        'Веранда'
    ) THEN 'restaurant'
    WHEN {{ order_type_column }} IN ('Банкет', 'Банкет C&B') THEN 'banquet'
    ELSE 'other'
END
{%- endmacro %}
