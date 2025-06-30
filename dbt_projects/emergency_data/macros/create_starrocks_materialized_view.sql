
{% macro create_starrocks_materialized_view(base_table, view_name, select_columns) %}
    {% set mv_sql %}
        CREATE MATERIALIZED VIEW {{ view_name }}
        DISTRIBUTED BY HASH({{ select_columns[0] }})
        REFRESH ASYNC
        AS SELECT 
            {% for column in select_columns %}
            {{ column }}
            {%- if not loop.last -%},{%- endif %}
            {% endfor %}
        FROM {{ base_table }}
    {% endset %}
    
    {{ return(mv_sql) }}
{% endmacro %}
