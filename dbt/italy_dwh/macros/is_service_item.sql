{% macro is_service_item(dish_column) -%}
CASE
    WHEN {{ dish_column }} IN (
        'Округление в пользу гостя',
        'Сервисный сбор',
        'Доставка 199',
        'Доставка 299',
        'CHEESE/ S Маргарита',
        'PET/вкусняшка + ваучер на груминг'
    ) THEN TRUE
    ELSE FALSE
END
{%- endmacro %}
