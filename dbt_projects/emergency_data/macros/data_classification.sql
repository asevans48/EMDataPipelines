-- macros/data_classification.sql
{% macro get_data_classification(classification_level) %}
    {% if classification_level %}
        '{{ classification_level }}'
    {% else %}
        '{{ var("classification_levels")["public"] }}'
    {% endif %}
{% endmacro %}

-- macros/audit_columns.sql  
{% macro get_retention_date(data_type) %}
    {% set retention_days = var("retention_periods")[data_type] %}
    {% if retention_days %}
        DATE_ADD(CURRENT_DATE(), INTERVAL {{ retention_days }} DAY)
    {% else %}
        DATE_ADD(CURRENT_DATE(), INTERVAL 2555 DAY)  -- Default 7 years
    {% endif %}
{% endmacro %}

{% macro add_audit_columns() %}
    ,CURRENT_TIMESTAMP() AS created_at
    ,CURRENT_TIMESTAMP() AS updated_at
    ,'{{ var("dbt_version", "unknown") }}' AS dbt_version
    ,'{{ target.name }}' AS target_environment
    ,'{{ invocation_id }}' AS dbt_run_id
{% endmacro %}
