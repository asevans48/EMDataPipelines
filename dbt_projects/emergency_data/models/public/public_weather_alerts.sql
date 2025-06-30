-- models/public/public_weather_alerts.sql
{{ config(
    materialized='table',
    engine='OLAP',
    duplicate_key=['alert_api_id'],
    distributed_by=['state_code'],
    properties={
        "replication_num": "3",
        "storage_format": "DEFAULT",
        "compression": "LZ4"
    },
    partition_by='alert_date',
    post_hook="CREATE MATERIALIZED VIEW IF NOT EXISTS {{ this }}_current_mv DISTRIBUTED BY HASH(state_code) REFRESH ASYNC AS SELECT * FROM {{ this }} WHERE alert_status = 'ACTIVE'"
) }}

WITH current_weather_alerts AS (
    SELECT
        event_id,
        event_subtype,
        event_name,
        state_code,
        state_name,
        event_date,
        event_start_date,
        event_end_date,
        risk_level
    FROM {{ ref('emergency_events') }}
    WHERE event_type = 'WEATHER_ALERT'
        AND data_classification = 'PUBLIC'
        AND event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)  -- Last 30 days for weather
),

weather_impacts AS (
    SELECT
        station_id,
        impact_date,
        state_code,
        total_impact_score,
        impact_category,
        agricultural_impact_assessment,
        has_concurrent_alert,
        concurrent_alert_type,
        concurrent_alert_risk
    FROM {{ ref('weather_impacts') }}
    WHERE impact_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)  -- Last week
        AND impact_category IN ('MODERATE', 'SEVERE')
),

public_weather_alerts AS (
    SELECT
        -- API identifiers
        wa.event_id AS alert_api_id,
        CONCAT('NOAA_', wa.state_code, '_', DATE_FORMAT(wa.event_date, '%Y%m%d_%H%i')) AS public_alert_code,
        
        -- Alert information
        wa.event_subtype AS weather_event_type,
        wa.event_name AS alert_headline,
        
        -- Geographic scope
        wa.state_code,
        wa.state_name,
        
        -- Temporal information
        wa.event_date AS alert_date,
        wa.event_start_date AS effective_time,
        wa.event_end_date AS expiration_time,
        
        -- Risk assessment
        wa.risk_level AS alert_risk_level,
        
        -- Impact correlation
        wi.total_impact_score,
        wi.impact_category AS weather_impact_severity,
        wi.agricultural_impact_assessment,
        
        -- Alert status
        CASE 
            WHEN wa.event_end_date IS NULL OR wa.event_end_date > CURRENT_TIMESTAMP() THEN 'ACTIVE'
            WHEN wa.event_end_date <= CURRENT_TIMESTAMP() THEN 'EXPIRED'
            ELSE 'UNKNOWN'
        END AS alert_status,
        
        -- Public guidance
        CASE 
            WHEN wa.risk_level = 'CRITICAL' THEN 'IMMEDIATE_ACTION_REQUIRED'
            WHEN wa.risk_level = 'HIGH' THEN 'PREPARATION_RECOMMENDED'
            WHEN wa.risk_level = 'MEDIUM' THEN 'MONITOR_CONDITIONS'
            ELSE 'AWARENESS_ONLY'
        END AS recommended_action,
        
        -- Agricultural sector guidance
        CASE 
            WHEN wi.agricultural_impact_assessment = 'HIGH_CROP_RISK' THEN 'PROTECT_CROPS_IMMEDIATELY'
            WHEN wi.agricultural_impact_assessment = 'DROUGHT_RISK' THEN 'CONSERVE_WATER_RESOURCES'
            WHEN wi.agricultural_impact_assessment = 'HARVEST_RISK' THEN 'EXPEDITE_HARVEST_OPERATIONS'
            WHEN wi.agricultural_impact_assessment = 'STRUCTURAL_DAMAGE_RISK' THEN 'SECURE_FARM_STRUCTURES'
            ELSE 'NORMAL_OPERATIONS'
        END AS agricultural_guidance,
        
        -- API metadata
        'PUBLIC' AS data_classification,
        'weather_alert_api' AS data_source,
        CURRENT_TIMESTAMP() AS last_updated,
        DATE_FORMAT(CURRENT_TIMESTAMP(), '%Y-%m-%d %H:00:00') AS api_hour_bucket,
        
        -- Compliance notice
        'Weather alert data sourced from NOAA. Users responsible for verification with local authorities.' AS disclaimer
        
    FROM current_weather_alerts wa
    LEFT JOIN weather_impacts wi ON 
        wa.state_code = wi.state_code 
        AND wa.event_date = wi.impact_date
)

SELECT * FROM public_weather_alerts