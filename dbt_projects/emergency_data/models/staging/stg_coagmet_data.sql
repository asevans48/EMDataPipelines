
-- models/staging/stg_coagmet_data.sql
{{ config(
    materialized='table',
    engine='OLAP',
    duplicate_key=['station_id', 'observation_datetime'],
    distributed_by=['station_id'],
    properties={
        "replication_num": "3",
        "storage_format": "DEFAULT",
        "compression": "LZ4"
    },
    partition_by='observation_date'
) }}

WITH source_data AS (
    SELECT * FROM {{ source('raw_data', 'coagmet_weather_data') }}
),

standardized AS (
    SELECT
        -- Station identification
        station_id,
        TRIM(station_name) AS station_name,
        
        -- Geographic coordinates
        ROUND(CAST(latitude AS DECIMAL(10,6)), 6) AS latitude,
        ROUND(CAST(longitude AS DECIMAL(10,6)), 6) AS longitude,
        
        -- Temporal information
        CAST(timestamp AS DATETIME) AS observation_datetime,
        DATE(timestamp) AS observation_date,
        HOUR(timestamp) AS observation_hour,
        
        -- Meteorological measurements with validation
        CASE 
            WHEN temperature BETWEEN -50 AND 60 THEN ROUND(CAST(temperature AS DECIMAL(5,2)), 2)
            ELSE NULL 
        END AS temperature_celsius,
        
        CASE 
            WHEN humidity BETWEEN 0 AND 100 THEN ROUND(CAST(humidity AS DECIMAL(5,2)), 2)
            ELSE NULL 
        END AS relative_humidity_percent,
        
        CASE 
            WHEN wind_speed BETWEEN 0 AND 200 THEN ROUND(CAST(wind_speed AS DECIMAL(5,2)), 2)
            ELSE NULL 
        END AS wind_speed_kmh,
        
        CASE 
            WHEN precipitation >= 0 AND precipitation <= 500 THEN ROUND(CAST(precipitation AS DECIMAL(8,2)), 2)
            ELSE NULL 
        END AS precipitation_mm,
        
        -- Derived calculations
        CASE 
            WHEN temperature IS NOT NULL AND humidity IS NOT NULL THEN
                ROUND(temperature - ((100 - humidity) / 5), 2)
            ELSE NULL
        END AS heat_index_celsius,
        
        -- Risk indicators for agriculture
        CASE 
            WHEN temperature < -5 THEN 'FREEZE_WARNING'
            WHEN temperature > 35 THEN 'HEAT_WARNING'
            WHEN wind_speed > 50 THEN 'WIND_WARNING'
            WHEN precipitation > 25 THEN 'HEAVY_RAIN'
            ELSE 'NORMAL'
        END AS agricultural_risk_indicator,
        
        -- Federal compliance metadata
        {{ get_data_classification('PUBLIC') }} AS data_classification,
        {{ get_retention_date('weather_data') }} AS retention_date,
        
        -- Audit fields
        ingestion_timestamp,
        data_source,
        CURRENT_TIMESTAMP() AS processed_at,
        '{{ var("dbt_version") }}' AS processing_version
        
    FROM source_data
    WHERE station_id IS NOT NULL
        AND timestamp IS NOT NULL
)

SELECT * FROM standardized