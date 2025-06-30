
{% macro get_optimal_bucket_count(expected_row_count) %}
    {% set bucket_count %}
        CASE 
            WHEN {{ expected_row_count }} < 100000 THEN 1
            WHEN {{ expected_row_count }} < 1000000 THEN 4
            WHEN {{ expected_row_count }} < 10000000 THEN 8
            WHEN {{ expected_row_count }} < 100000000 THEN 16
            ELSE 32
        END
    {% endset %}
    
    {{ return(bucket_count) }}
{% endmacro %}