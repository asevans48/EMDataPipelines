{% macro optimize_starrocks_table(table_name) %}
    {% set optimize_sql %}
        -- Refresh materialized view if it exists
        REFRESH MATERIALIZED VIEW IF EXISTS {{ table_name }}_mv;
        
        -- Update table statistics for query optimization
        ANALYZE TABLE {{ table_name }} UPDATE HISTOGRAM ON *;
        
        -- Compact small files for better performance
        ALTER TABLE {{ table_name }} COMPACT;
    {% endset %}
    
    {{ return(optimize_sql) }}
{% endmacro %}
