-- macros/data_quality.sql
{% macro test_data_freshness(table_name, timestamp_column, max_age_hours=24) %}
    {% set test_sql %}
        SELECT COUNT(*) as stale_records
        FROM {{ table_name }}
        WHERE {{ timestamp_column }} < CURRENT_TIMESTAMP() - INTERVAL {{ max_age_hours }} HOUR
    {% endset %}
    
    {{ return(test_sql) }}
{% endmacro %}

{% macro test_completeness(table_name, required_columns) %}
    {% set test_sql %}
        SELECT 
            {% for column in required_columns %}
            COUNT(CASE WHEN {{ column }} IS NULL THEN 1 END) as {{ column }}_null_count
            {%- if not loop.last -%},{%- endif %}
            {% endfor %}
        FROM {{ table_name }}
    {% endset %}
    
    {{ return(test_sql) }}
{% endmacro %}