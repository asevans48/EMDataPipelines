-- models/marts/emergency_events.sql
{{ config(
    materialized='table',
    engine='OLAP',
    duplicate_key=['event_id'],
    distributed_by=['state_code'],
    properties={
        "replication_num": "3",
        "storage_format": "DEFAULT",
        "compression": "LZ4"
    },
    partition_by='event_date'
) }}

WITH disaster_events AS (
    SELECT
        CONCAT('FEMA_', disaster_number) AS event_id,
        'DISASTER' AS event_type,
        incident_type AS event_subtype,
        disaster_title AS event_name,
        state AS state_code,
        declaration_date AS event_date,
        incident_begin_date AS event_start_date,
        incident_end_date AS event_end_date,
        designated_area AS affected_area,
        fiscal_year_declared,
        
        -- Risk assessment
        CASE 
            WHEN incident_type IN ('Hurricane', 'Major Disaster') THEN 'CRITICAL'
            WHEN incident_type IN ('Severe Storm', 'Flood', 'Fire') THEN 'HIGH'
            WHEN incident_type IN ('Winter Storm', 'Tornado') THEN 'MEDIUM'
            ELSE 'LOW'
        END AS risk_level,
        
        -- Duration calculation
        CASE 
            WHEN incident_end_date IS NOT NULL THEN 
                DATEDIFF(incident_end_date, incident_begin_date)
            ELSE NULL
        END AS event_duration_days,
        
        data_classification,
        retention_date,
        processed_at
        
    FROM {{ ref('stg_fema_disasters') }}
),

weather_events AS (
    SELECT
        alert_id AS event_id,
        'WEATHER_ALERT' AS event_type,
        weather_event AS event_subtype,
        alert_headline AS event_name,
        state_code,
        effective_date AS event_date,
        effective_datetime AS event_start_date,
        expires_datetime AS event_end_date,
        affected_area_description AS affected_area,
        YEAR(effective_date) AS fiscal_year_declared,
        
        -- Risk level from staging
        risk_level,
        
        -- Duration from staging
        alert_duration_hours / 24.0 AS event_duration_days,
        
        data_classification,
        retention_date,
        processed_at
        
    FROM {{ ref('stg_noaa_weather') }}
    WHERE risk_level IN ('HIGH', 'CRITICAL')  -- Only significant weather events
),

unified_events AS (
    SELECT * FROM disaster_events
    UNION ALL
    SELECT * FROM weather_events
),

event_enrichment AS (
    SELECT
        e.*,
        
        -- Geographic enrichment using seeds
        s.state_name,
        s.fips_code AS state_fips_code,
        
        -- Event categorization
        CASE 
            WHEN event_subtype IN ('Hurricane', 'Typhoon', 'Cyclone') THEN 'TROPICAL_STORM'
            WHEN event_subtype IN ('Flood', 'Flash Flood', 'Dam Break') THEN 'FLOODING'
            WHEN event_subtype IN ('Fire', 'Wildfire', 'Urban Fire') THEN 'FIRE'
            WHEN event_subtype IN ('Tornado', 'Severe Storm', 'Wind') THEN 'SEVERE_WEATHER'
            WHEN event_subtype IN ('Winter Storm', 'Ice Storm', 'Blizzard') THEN 'WINTER_WEATHER'
            WHEN event_subtype IN ('Earthquake', 'Volcano', 'Landslide') THEN 'GEOLOGICAL'
            WHEN event_subtype IN ('Drought', 'Heat Wave') THEN 'CLIMATOLOGICAL'
            ELSE 'OTHER'
        END AS event_category,
        
        -- Seasonal analysis
        CASE 
            WHEN MONTH(event_date) IN (12, 1, 2) THEN 'WINTER'
            WHEN MONTH(event_date) IN (3, 4, 5) THEN 'SPRING'
            WHEN MONTH(event_date) IN (6, 7, 8) THEN 'SUMMER'
            WHEN MONTH(event_date) IN (9, 10, 11) THEN 'FALL'
        END AS event_season,
        
        -- Federal fiscal year calculation
        CASE 
            WHEN MONTH(event_date) >= 10 THEN YEAR(event_date) + 1
            ELSE YEAR(event_date)
        END AS federal_fiscal_year,
        
        -- Impact assessment flags
        CASE 
            WHEN event_duration_days > 30 THEN TRUE
            ELSE FALSE
        END AS is_long_duration_event,
        
        CASE 
            WHEN risk_level IN ('CRITICAL', 'HIGH') THEN TRUE
            ELSE FALSE
        END AS is_high_impact_event,
        
        -- Compliance and audit
        'FEMA_ELIGIBLE' AS funding_eligibility,
        CURRENT_TIMESTAMP() AS last_updated
        
    FROM unified_events e
    LEFT JOIN {{ ref('state_codes') }} s ON e.state_code = s.state_code
)

SELECT * FROM event_enrichment
