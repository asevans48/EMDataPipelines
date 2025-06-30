
-- models/metrics/usage_metrics.sql
{{ config(
    materialized='view'
) }}

WITH api_usage AS (
    SELECT 
        'PUBLIC_DISASTERS' AS api_endpoint,
        COUNT(*) AS total_requests,
        COUNT(DISTINCT api_hour_bucket) AS unique_hours_accessed,
        MIN(last_updated) AS first_access,
        MAX(last_updated) AS last_access,
        AVG(CASE WHEN disaster_id IS NOT NULL THEN 1 ELSE 0 END) AS success_rate
    FROM {{ ref('public_disasters') }}
    WHERE last_updated >= CURRENT_DATE() - INTERVAL 7 DAY
    
    UNION ALL
    
    SELECT 
        'PUBLIC_WEATHER_ALERTS' AS api_endpoint,
        COUNT(*) AS total_requests,
        COUNT(DISTINCT api_hour_bucket) AS unique_hours_accessed,
        MIN(last_updated) AS first_access,
        MAX(last_updated) AS last_access,
        AVG(CASE WHEN alert_api_id IS NOT NULL THEN 1 ELSE 0 END) AS success_rate
    FROM {{ ref('public_weather_alerts') }}
    WHERE last_updated >= CURRENT_DATE() - INTERVAL 7 DAY
    
    UNION ALL
    
    SELECT 
        'PUBLIC_AGRICULTURAL' AS api_endpoint,
        COUNT(*) AS total_requests,
        COUNT(DISTINCT api_hour_bucket) AS unique_hours_accessed,
        MIN(last_updated) AS first_access,
        MAX(last_updated) AS last_access,
        AVG(CASE WHEN ag_data_id IS NOT NULL THEN 1 ELSE 0 END) AS success_rate
    FROM {{ ref('public_agricultural_data') }}
    WHERE last_updated >= CURRENT_DATE() - INTERVAL 7 DAY
),

system_performance AS (
    SELECT 
        api_endpoint,
        total_requests,
        unique_hours_accessed,
        first_access,
        last_access,
        ROUND(success_rate * 100, 2) AS success_rate_percent,
        
        -- Performance indicators
        ROUND(total_requests / 7.0, 0) AS avg_daily_requests,
        ROUND(total_requests / (unique_hours_accessed * 1.0), 1) AS avg_requests_per_hour,
        
        -- System health
        CASE 
            WHEN success_rate >= 0.99 THEN 'EXCELLENT'
            WHEN success_rate >= 0.95 THEN 'GOOD'
            WHEN success_rate >= 0.90 THEN 'ACCEPTABLE'
            ELSE 'DEGRADED'
        END AS system_health_status,
        
        -- Usage trend
        CASE 
            WHEN total_requests > 1000 THEN 'HIGH_USAGE'
            WHEN total_requests > 100 THEN 'MODERATE_USAGE'
            WHEN total_requests > 10 THEN 'LOW_USAGE'
            ELSE 'MINIMAL_USAGE'
        END AS usage_level,
        
        CURRENT_TIMESTAMP() AS metrics_generated_at
    FROM api_usage
)

SELECT * FROM system_performance
