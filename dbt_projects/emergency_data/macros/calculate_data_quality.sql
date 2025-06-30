{% macro calculate_data_quality_score(table_name) %}
    {% set quality_sql %}
        WITH quality_metrics AS (
            SELECT 
                COUNT(*) as total_records,
                
                -- Completeness metrics
                {% for column in ['state_code', 'event_date', 'data_classification'] %}
                SUM(CASE WHEN {{ column }} IS NOT NULL THEN 1 ELSE 0 END) / COUNT(*) as {{ column }}_completeness,
                {% endfor %}
                
                -- Timeliness metrics  
                SUM(CASE WHEN processed_at >= CURRENT_TIMESTAMP() - INTERVAL 24 HOUR THEN 1 ELSE 0 END) / COUNT(*) as timeliness_score,
                
                -- Validity metrics
                SUM(CASE WHEN data_classification IN ('PUBLIC', 'INTERNAL', 'RESTRICTED', 'CONFIDENTIAL') THEN 1 ELSE 0 END) / COUNT(*) as classification_validity
        FROM {{ table_name }}
        )
        SELECT 
            total_records,
            ROUND(
                (state_code_completeness + event_date_completeness + data_classification_completeness + 
                 timeliness_score + classification_validity) / 5 * 100, 2
            ) as overall_quality_score
        FROM quality_metrics
    {% endset %}
    
    {{ return(quality_sql) }}
{% endmacro %}