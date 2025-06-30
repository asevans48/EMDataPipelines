
-- models/metrics/api_performance.sql
{{ config(
    materialized='view'
) }}

WITH emergency_events_performance AS (
    SELECT 
        'EMERGENCY_EVENTS' AS dataset_name,
        COUNT(*) AS total_events,
        COUNT(CASE WHEN event_date >= CURRENT_DATE() - INTERVAL 30 DAY THEN 1 END) AS recent_events,
        COUNT(CASE WHEN risk_level IN ('HIGH', 'CRITICAL') THEN 1 END) AS high_risk_events,
        COUNT(DISTINCT state_code) AS states_with_events,
        COUNT(DISTINCT event_category) AS event_categories,
        AVG(CASE WHEN event_duration_days IS NOT NULL THEN event_duration_days END) AS avg_event_duration_days,
        MAX(last_updated) AS last_dataset_update
    FROM {{ ref('emergency_events') }}
    WHERE event_date >= CURRENT_DATE() - INTERVAL 365 DAY
),

weather_impacts_performance AS (
    SELECT 
        'WEATHER_IMPACTS' AS dataset_name,
        COUNT(*) AS total_events,
        COUNT(CASE WHEN impact_date >= CURRENT_DATE() - INTERVAL 7 DAY THEN 1 END) AS recent_events,
        COUNT(CASE WHEN impact_category IN ('MODERATE', 'SEVERE') THEN 1 END) AS high_risk_events,
        COUNT(DISTINCT state_code) AS states_with_events,
        COUNT(DISTINCT agricultural_impact_assessment) AS event_categories,
        AVG(total_impact_score) AS avg_event_duration_days,  -- Reusing field for impact score
        MAX(analysis_timestamp) AS last_dataset_update
    FROM {{ ref('weather_impacts') }}
    WHERE impact_date >= CURRENT_DATE() - INTERVAL 90 DAY
),

disaster_analytics_performance AS (
    SELECT 
        'DISASTER_ANALYTICS' AS dataset_name,
        COUNT(*) AS total_events,
        COUNT(CASE WHEN disaster_year = YEAR(CURRENT_DATE()) THEN 1 END) AS recent_events,
        COUNT(CASE WHEN annual_disaster_risk_rating IN ('HIGH', 'VERY_HIGH') THEN 1 END) AS high_risk_events,
        COUNT(DISTINCT state_code) AS states_with_events,
        COUNT(DISTINCT event_category) AS event_categories,
        AVG(event_count) AS avg_event_duration_days,  -- Reusing field for avg event count
        MAX(analytics_generated_at) AS last_dataset_update
    FROM {{ ref('disaster_analytics') }}
    WHERE disaster_year >= YEAR(CURRENT_DATE()) - 5
),

combined_performance AS (
    SELECT * FROM emergency_events_performance
    UNION ALL
    SELECT * FROM weather_impacts_performance  
    UNION ALL
    SELECT * FROM disaster_analytics_performance
),

performance_analysis AS (
    SELECT 
        dataset_name,
        total_events,
        recent_events,
        high_risk_events,
        states_with_events,
        event_categories,
        ROUND(avg_event_duration_days, 2) AS avg_metric_value,
        last_dataset_update,
        
        -- Performance indicators
        ROUND(recent_events / total_events * 100, 2) AS recent_activity_percent,
        ROUND(high_risk_events / total_events * 100, 2) AS high_risk_percent,
        
        -- Data coverage assessment
        CASE 
            WHEN states_with_events >= 45 THEN 'NATIONAL_COVERAGE'
            WHEN states_with_events >= 30 THEN 'REGIONAL_COVERAGE'
            WHEN states_with_events >= 10 THEN 'MULTI_STATE_COVERAGE'
            ELSE 'LIMITED_COVERAGE'
        END AS geographic_coverage,
        
        -- Dataset health
        CASE 
            WHEN last_dataset_update >= CURRENT_TIMESTAMP() - INTERVAL 6 HOUR THEN 'CURRENT'
            WHEN last_dataset_update >= CURRENT_TIMESTAMP() - INTERVAL 24 HOUR THEN 'RECENT'
            WHEN last_dataset_update >= CURRENT_TIMESTAMP() - INTERVAL 48 HOUR THEN 'STALE'
            ELSE 'OUTDATED'
        END AS dataset_freshness,
        
        -- Federal compliance monitoring
        CASE 
            WHEN dataset_name = 'EMERGENCY_EVENTS' AND total_events = 0 THEN 'DATA_MISSING'
            WHEN recent_activity_percent = 0 AND dataset_name IN ('EMERGENCY_EVENTS', 'WEATHER_IMPACTS') THEN 'NO_RECENT_ACTIVITY'
            ELSE 'OPERATIONAL'
        END AS compliance_status,
        
        CURRENT_TIMESTAMP() AS performance_check_timestamp
        
    FROM combined_performance
)

SELECT * FROM performance_analysis