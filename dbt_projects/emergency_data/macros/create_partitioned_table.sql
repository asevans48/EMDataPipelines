{% macro create_partitioned_table(table_name, partition_column, partition_type='RANGE') %}
    {% set create_sql %}
        CREATE TABLE IF NOT EXISTS {{ table_name }} 
        PARTITION BY {{ partition_type }}({{ partition_column }})
        (
            {% if partition_type == 'RANGE' and partition_column in ['event_date', 'declaration_date', 'observation_date'] %}
            PARTITION p_2020 VALUES LESS THAN ('2021-01-01'),
            PARTITION p_2021 VALUES LESS THAN ('2022-01-01'),
            PARTITION p_2022 VALUES LESS THAN ('2023-01-01'),
            PARTITION p_2023 VALUES LESS THAN ('2024-01-01'),
            PARTITION p_2024 VALUES LESS THAN ('2025-01-01'),
            PARTITION p_2025 VALUES LESS THAN ('2026-01-01'),
            PARTITION p_future VALUES LESS THAN (MAXVALUE)
            {% endif %}
        )
        ENGINE=OLAP
        DISTRIBUTED BY HASH({{ partition_column }})
        PROPERTIES (
            "replication_num" = "{{ var('starrocks')['replication_num'] }}",
            "storage_format" = "DEFAULT",
            "compression" = "{{ var('starrocks')['compression'] }}",
            "bucket_size" = "{{ var('starrocks')['bucket_size'] }}"
        )
    {% endset %}
    
    {{ return(create_sql) }}
{% endmacro %}