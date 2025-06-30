
{% macro log_federal_compliance_event(event_type, table_name, details=none) %}
    INSERT INTO federal_compliance_log (
        event_type,
        table_name,
        event_details,
        compliance_frameworks,
        event_timestamp,
        dbt_run_id,
        user_name,
        environment
    ) VALUES (
        '{{ event_type }}',
        '{{ table_name }}',
        {% if details %}'{{ details }}'{% else %}NULL{% endif %},
        '{{ var("compliance_frameworks") | join(",") }}',
        CURRENT_TIMESTAMP(),
        '{{ invocation_id }}',
        CURRENT_USER(),
        '{{ target.name }}'
    )
{% endmacro %}
