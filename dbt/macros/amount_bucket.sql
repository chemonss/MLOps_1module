{% macro amount_bucket(amount_column) %}
    CASE
        WHEN {{ amount_column }} < 50 THEN 'LOW'
        WHEN {{ amount_column }} < 200 THEN 'MEDIUM'
        WHEN {{ amount_column }} < 1000 THEN 'HIGH'
        ELSE 'VERY_HIGH'
    END
{% endmacro %}
