-- models/metrics/data_quality_metrics.sql
{{ config(
    materialized='view',
    post_hook="INSERT INTO data_quality_audit_log SELECT *, CURRENT_TIMESTAMP() FROM {{ this }}"
) }}

WITH source_quality AS (
    SELECT 
        'FEMA_DISASTERS' AS data_source,
        COUNT(*) AS total_records,
        COUNT(CASE WHEN disaster_number IS NULL THEN 1 END) AS missing_primary_keys,
        COUNT(CASE WHEN state IS NULL THEN 1 END) AS missing_required_fields,
        COUNT(CASE WHEN declaration_date IS NULL THEN 1 END) AS missing_dates,
        COUNT(CASE WHEN processed_at >= CURRENT_TIMESTAMP() - INTERVAL 24 HOUR THEN 1 END) AS recent_records,
        MAX(processed_at) AS latest_update,
        COUNT(CASE WHEN data_classification NOT IN ('PUBLIC', 'INTERNAL', 'RESTRICTED', 'CONFIDENTIAL') THEN 1 END) AS invalid_classifications
    FROM {{ ref('stg_fema_disasters') }}
    
    UNION ALL
    
    SELECT 
        'NOAA_WEATHER' AS data_source,
        COUNT(*) AS total_records,
        COUNT(CASE WHEN alert_id IS NULL THEN 1 END) AS missing_primary_keys,
        COUNT(CASE WHEN weather_event IS NULL THEN 1 END) AS missing_required_fields,
        COUNT(CASE WHEN effective_datetime IS NULL THEN 1 END) AS missing_dates,
        COUNT(CASE WHEN processed_at >= CURRENT_TIMESTAMP() - INTERVAL 4 HOUR THEN 1 END) AS recent_records,
        MAX(processed_at) AS latest_update,
        COUNT(CASE WHEN data_classification NOT IN ('PUBLIC', 'INTERNAL', 'RESTRICTED', 'CONFIDENTIAL') THEN 1 END) AS invalid_classifications
    FROM {{ ref('stg_noaa_weather') }}
    
    UNION ALL
    
    SELECT 
        'COAGMET_WEATHER' AS data_source,
        COUNT(*) AS total_records,
        COUNT(CASE WHEN station_id IS NULL OR observation_datetime IS NULL THEN 1 END) AS missing_primary_keys,
        COUNT(CASE WHEN temperature_celsius IS NULL AND relative_humidity_percent IS NULL AND wind_speed_kmh IS NULL THEN 1 END) AS missing_required_fields,
        COUNT(CASE WHEN observation_datetime IS NULL THEN 1 END) AS missing_dates,
        COUNT(CASE WHEN processed_at >= CURRENT_TIMESTAMP() - INTERVAL 2 HOUR THEN 1 END) AS recent_records,
        MAX(processed_at) AS latest_update,
        COUNT(CASE WHEN data_classification NOT IN ('PUBLIC', 'INTERNAL', 'RESTRICTED', 'CONFIDENTIAL') THEN 1 END) AS invalid_classifications
    FROM {{ ref('stg_coagmet_data') }}
    
    UNION ALL
    
    SELECT 
        'USDA_AGRICULTURAL' AS data_source,
        COUNT(*) AS total_records,
        COUNT(CASE WHEN program_year IS NULL OR state_code IS NULL OR county_code IS NULL THEN 1 END) AS missing_primary_keys,
        COUNT(CASE WHEN commodity_name IS NULL THEN 1 END) AS missing_required_fields,
        0 AS missing_dates,  -- USDA data doesn't have timestamp requirements
        COUNT(CASE WHEN processed_at >= CURRENT_TIMESTAMP() - INTERVAL 24 HOUR THEN 1 END) AS recent_records,
        MAX(processed_at) AS latest_update,
        COUNT(CASE WHEN data_classification NOT IN ('PUBLIC', 'INTERNAL', 'RESTRICTED', 'CONFIDENTIAL') THEN 1 END) AS invalid_classifications
    FROM {{ ref('stg_usda_data') }}
),

quality_calculations AS (
    SELECT 
        data_source,
        total_records,
        missing_primary_keys,
        missing_required_fields,
        missing_dates,
        recent_records,
        latest_update,
        invalid_classifications,
        
        -- Quality percentages
        ROUND((total_records - missing_primary_keys) / total_records * 100, 2) AS primary_key_completeness_percent,
        ROUND((total_records - missing_required_fields) / total_records * 100, 2) AS required_field_completeness_percent,
        ROUND((total_records - missing_dates) / total_records * 100, 2) AS date_completeness_percent,
        ROUND(recent_records / total_records * 100, 2) AS data_freshness_percent,
        ROUND((total_records - invalid_classifications) / total_records * 100, 2) AS classification_validity_percent,
        
        -- Timeliness assessment
        CASE 
            WHEN data_source = 'NOAA_WEATHER' AND latest_update < CURRENT_TIMESTAMP() - INTERVAL 4 HOUR THEN 'STALE'
            WHEN data_source = 'COAGMET_WEATHER' AND latest_update < CURRENT_TIMESTAMP() - INTERVAL 2 HOUR THEN 'STALE'
            WHEN data_source IN ('FEMA_DISASTERS', 'USDA_AGRICULTURAL') AND latest_update < CURRENT_TIMESTAMP() - INTERVAL 24 HOUR THEN 'STALE'
            ELSE 'CURRENT'
        END AS timeliness_status,
        
        CURRENT_TIMESTAMP() AS quality_check_timestamp
    FROM source_quality
),

overall_quality AS (
    SELECT 
        qc.*,
        
        -- Overall quality score calculation
        ROUND(
            (primary_key_completeness_percent + 
             required_field_completeness_percent + 
             date_completeness_percent + 
             data_freshness_percent + 
             classification_validity_percent) / 5, 2
        ) AS overall_quality_score,
        
        -- Quality status
        CASE 
            WHEN (primary_key_completeness_percent + required_field_completeness_percent + date_completeness_percent + data_freshness_percent + classification_validity_percent) / 5 >= 95 THEN 'EXCELLENT'
            WHEN (primary_key_completeness_percent + required_field_completeness_percent + date_completeness_percent + data_freshness_percent + classification_validity_percent) / 5 >= 90 THEN 'GOOD'
            WHEN (primary_key_completeness_percent + required_field_completeness_percent + date_completeness_percent + data_freshness_percent + classification_validity_percent) / 5 >= 80 THEN 'ACCEPTABLE'
            WHEN (primary_key_completeness_percent + required_field_completeness_percent + date_completeness_percent + data_freshness_percent + classification_validity_percent) / 5 >= 70 THEN 'POOR'
            ELSE 'CRITICAL'
        END AS quality_status,
        
        -- Federal compliance status
        CASE 
            WHEN invalid_classifications = 0 AND timeliness_status = 'CURRENT' AND missing_primary_keys = 0 THEN 'COMPLIANT'
            WHEN invalid_classifications > 0 OR missing_primary_keys > total_records * 0.01 THEN 'NON_COMPLIANT'
            ELSE 'WARNING'
        END AS compliance_status
        
    FROM quality_calculations qc
)

SELECT * FROM overall_quality
