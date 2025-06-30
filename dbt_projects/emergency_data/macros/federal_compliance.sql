-- macros/federal_compliance.sql
{% macro validate_federal_data_retention(table_name) %}
    {% set validation_sql %}
        SELECT 
            COUNT(*) as total_records,
            COUNT(CASE WHEN retention_date IS NULL THEN 1 END) as missing_retention_dates,
            COUNT(CASE WHEN data_classification IS NULL THEN 1 END) as missing_classification,
            COUNT(CASE WHEN data_classification NOT IN ('PUBLIC', 'INTERNAL', 'RESTRICTED', 'CONFIDENTIAL') THEN 1 END) as invalid_classification
        FROM {{ table_name }}
    {% endset %}
    
    {{ return(validation_sql) }}
{% endmacro %}

{% macro mask_sensitive_data(column_name, data_classification) %}
    {% if data_classification in ('RESTRICTED', 'CONFIDENTIAL') %}
        CASE 
            WHEN '{{ target.name }}' = 'prod' AND CURRENT_USER() NOT LIKE '%_admin' THEN 
                CONCAT(LEFT({{ column_name }}, 2), REPEAT('*', LENGTH({{ column_name }}) - 2))
            ELSE {{ column_name }}
        END
    {% else %}
        {{ column_name }}
    {% endif %}
{% endmacro %}
