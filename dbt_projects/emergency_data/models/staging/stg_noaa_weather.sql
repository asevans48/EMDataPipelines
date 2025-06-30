
-- models/staging/stg_noaa_weather.sql  
{{ config(
    materialized='table',
    engine='OLAP', 
    duplicate_key=['alert_id'],
    distributed_by=['state_code'],
    properties={
        "replication_num": "3",
        "storage_format": "DEFAULT", 
        "compression": "LZ4"
    },
    partition_by='effective_date'
) }}

WITH source_data AS (
    SELECT * FROM {{ source('raw_data', 'noaa_weather_alerts') }}
),

standardized AS (
    SELECT
        -- Primary identifier
        alert_id,
        
        -- Event categorization
        UPPER(TRIM(event)) AS weather_event,
        UPPER(TRIM(severity)) AS severity_level,
        UPPER(TRIM(urgency)) AS urgency_level,
        UPPER(TRIM(certainty)) AS certainty_level,
        
        -- Alert content
        TRIM(headline) AS alert_headline,
        TRIM(description) AS alert_description,
        TRIM(instruction) AS public_instruction,
        
        -- Geographic information
        TRIM(area_desc) AS affected_area_description,
        -- Extract state code from area description
        CASE 
            WHEN area_desc LIKE '%CO%' THEN 'CO'
            WHEN area_desc LIKE '%California%' THEN 'CA'
            WHEN area_desc LIKE '%Texas%' THEN 'TX'
            -- Add more state mappings as needed
            ELSE NULL
        END AS state_code,
        
        -- Temporal information
        CAST(effective AS DATETIME) AS effective_datetime,
        CAST(expires AS DATETIME) AS expires_datetime,
        DATE(effective) AS effective_date,
        DATE(expires) AS expires_date,
        
        -- Duration calculation
        TIMESTAMPDIFF(HOUR, 
            CAST(effective AS DATETIME), 
            CAST(expires AS DATETIME)
        ) AS alert_duration_hours,
        
        -- Risk assessment
        CASE 
            WHEN severity = 'Extreme' AND urgency = 'Immediate' THEN 'CRITICAL'
            WHEN severity = 'Severe' AND urgency IN ('Immediate', 'Expected') THEN 'HIGH'
            WHEN severity = 'Moderate' THEN 'MEDIUM'
            ELSE 'LOW'
        END AS risk_level,
        
        -- Federal compliance metadata
        {{ get_data_classification('PUBLIC') }} AS data_classification,
        {{ get_retention_date('weather_data') }} AS retention_date,
        
        -- Audit fields
        ingestion_timestamp,
        data_source,
        CURRENT_TIMESTAMP() AS processed_at,
        '{{ var("dbt_version") }}' AS processing_version
        
    FROM source_data
    WHERE alert_id IS NOT NULL
        AND effective IS NOT NULL
)

SELECT * FROM standardized
