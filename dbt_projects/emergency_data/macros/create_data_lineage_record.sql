{% macro create_data_lineage_record(source_table, target_table, transformation_type) %}
    INSERT INTO data_lineage_audit (
        source_table,
        target_table, 
        transformation_type,
        dbt_run_id,
        transformation_timestamp,
        dbt_version,
        user_name,
        compliance_frameworks
    ) VALUES (
        '{{ source_table }}',
        '{{ target_table }}',
        '{{ transformation_type }}',
        '{{ invocation_id }}',
        CURRENT_TIMESTAMP(),
        '{{ var("dbt_version", "unknown") }}',
        CURRENT_USER(),
        '{{ var("compliance_frameworks") | join(",") }}'
    )
{% endmacro %}
