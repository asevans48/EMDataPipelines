-- macros/public_data_helpers.sql
{% macro create_starrocks_table(table_name, columns, partition_column=none, distributed_column=none) %}
    {% set create_sql %}
        CREATE TABLE IF NOT EXISTS {{ table_name }} (
            {% for column_name, column_type in columns.items() %}
                {{ column_name }} {{ column_type }}
                {%- if not loop.last -%},{%- endif %}
            {% endfor %}
        )
        ENGINE=OLAP
        {% if partition_column %}
        PARTITION BY RANGE({{ partition_column }})()
        {% endif %}
        {% if distributed_column %}
        DISTRIBUTED BY HASH({{ distributed_column }})
        {% endif %}
        PROPERTIES (
            "replication_num" = "{{ var('starrocks')['replication_num'] }}",
            "storage_format" = "DEFAULT",
            "compression" = "{{ var('starrocks')['compression'] }}"
        )
    {% endset %}
    
    {{ return(create_sql) }}
{% endmacro %}
