
-- models/marts/weather_impacts.sql
{{ config(
    materialized='table',
    engine='OLAP',
    duplicate_key=['impact_id'],
    distributed_by=['state_code'],
    properties={
        "replication_num": "3",
        "storage_format": "DEFAULT",
        "compression": "LZ4"
    },
    partition_by='impact_date'
) }}

WITH weather_observations AS (
    SELECT
        station_id,
        observation_date,
        observation_datetime,
        station_name,
        latitude,
        longitude,
        temperature_celsius,
        relative_humidity_percent,
        wind_speed_kmh,
        precipitation_mm,
        heat_index_celsius,
        agricultural_risk_indicator,
        processed_at
    FROM {{ ref('stg_coagmet_data') }}
    WHERE observation_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)  -- Last 90 days
),

daily_weather_summary AS (
    SELECT
        station_id,
        observation_date,
        
        -- Temperature statistics
        AVG(temperature_celsius) AS avg_temperature,
        MIN(temperature_celsius) AS min_temperature,
        MAX(temperature_celsius) AS max_temperature,
        
        -- Humidity statistics
        AVG(relative_humidity_percent) AS avg_humidity,
        MIN(relative_humidity_percent) AS min_humidity,
        
        -- Wind statistics
        AVG(wind_speed_kmh) AS avg_wind_speed,
        MAX(wind_speed_kmh) AS max_wind_speed,
        
        -- Precipitation
        SUM(precipitation_mm) AS total_precipitation,
        MAX(precipitation_mm) AS max_hourly_precipitation,
        
        -- Risk indicators
        COUNT(CASE WHEN agricultural_risk_indicator != 'NORMAL' THEN 1 END) AS risk_hours,
        MAX(CASE WHEN agricultural_risk_indicator = 'FREEZE_WARNING' THEN 1 ELSE 0 END) AS had_freeze_warning,
        MAX(CASE WHEN agricultural_risk_indicator = 'HEAT_WARNING' THEN 1 ELSE 0 END) AS had_heat_warning,
        MAX(CASE WHEN agricultural_risk_indicator = 'WIND_WARNING' THEN 1 ELSE 0 END) AS had_wind_warning,
        MAX(CASE WHEN agricultural_risk_indicator = 'HEAVY_RAIN' THEN 1 ELSE 0 END) AS had_heavy_rain,
        
        -- Station metadata
        ANY_VALUE(station_name) AS station_name,
        ANY_VALUE(latitude) AS latitude,
        ANY_VALUE(longitude) AS longitude,
        MAX(processed_at) AS last_processed_at
        
    FROM weather_observations
    GROUP BY station_id, observation_date
),

weather_alerts AS (
    SELECT
        alert_id,
        weather_event,
        severity_level,
        urgency_level,
        certainty_level,
        effective_date,
        expires_date,
        alert_duration_hours,
        risk_level,
        state_code,
        affected_area_description
    FROM {{ ref('stg_noaa_weather') }}
    WHERE effective_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 90 DAY)
),

impact_analysis AS (
    SELECT
        -- Generate unique impact ID
        MD5(CONCAT(w.station_id, '_', w.observation_date)) AS impact_id,
        
        -- Basic identification
        w.station_id,
        w.station_name,
        w.observation_date AS impact_date,
        w.latitude,
        w.longitude,
        
        -- Extract state from coordinates (simplified)
        CASE 
            WHEN w.latitude BETWEEN 37.0 AND 41.0 AND w.longitude BETWEEN -109.0 AND -102.0 THEN 'CO'
            ELSE 'UNKNOWN'
        END AS state_code,
        
        -- Weather conditions
        w.avg_temperature,
        w.min_temperature,
        w.max_temperature,
        w.avg_humidity,
        w.avg_wind_speed,
        w.max_wind_speed,
        w.total_precipitation,
        w.max_hourly_precipitation,
        
        -- Risk assessments
        w.risk_hours,
        w.had_freeze_warning,
        w.had_heat_warning,
        w.had_wind_warning,
        w.had_heavy_rain,
        
        -- Impact scoring
        CASE 
            WHEN w.min_temperature < -10 OR w.max_temperature > 40 THEN 3
            WHEN w.min_temperature < -5 OR w.max_temperature > 35 THEN 2
            WHEN w.min_temperature < 0 OR w.max_temperature > 30 THEN 1
            ELSE 0
        END AS temperature_impact_score,
        
        CASE 
            WHEN w.total_precipitation > 50 THEN 3
            WHEN w.total_precipitation > 25 THEN 2
            WHEN w.total_precipitation > 10 THEN 1
            ELSE 0
        END AS precipitation_impact_score,
        
        CASE 
            WHEN w.max_wind_speed > 80 THEN 3
            WHEN w.max_wind_speed > 50 THEN 2
            WHEN w.max_wind_speed > 30 THEN 1
            ELSE 0
        END AS wind_impact_score,
        
        -- Alert correlation
        COALESCE(a.alert_id IS NOT NULL, FALSE) AS has_concurrent_alert,
        a.weather_event AS concurrent_alert_type,
        a.risk_level AS concurrent_alert_risk,
        
        -- Compliance metadata
        {{ get_data_classification('PUBLIC') }} AS data_classification,
        {{ get_retention_date('weather_data') }} AS retention_date,
        CURRENT_TIMESTAMP() AS analysis_timestamp,
        w.last_processed_at
        
    FROM daily_weather_summary w
    LEFT JOIN weather_alerts a ON 
        w.observation_date BETWEEN a.effective_date AND COALESCE(a.expires_date, a.effective_date)
        AND (
            -- Geographic proximity check (simplified)
            (w.latitude BETWEEN 37.0 AND 41.0 AND w.longitude BETWEEN -109.0 AND -102.0 AND a.state_code = 'CO')
        )
),

final_impacts AS (
    SELECT
        *,
        
        -- Composite impact score
        temperature_impact_score + precipitation_impact_score + wind_impact_score AS total_impact_score,
        
        -- Impact categories
        CASE 
            WHEN temperature_impact_score + precipitation_impact_score + wind_impact_score >= 6 THEN 'SEVERE'
            WHEN temperature_impact_score + precipitation_impact_score + wind_impact_score >= 4 THEN 'MODERATE'
            WHEN temperature_impact_score + precipitation_impact_score + wind_impact_score >= 2 THEN 'MINOR'
            ELSE 'MINIMAL'
        END AS impact_category,
        
        -- Agricultural impact assessment
        CASE 
            WHEN had_freeze_warning = 1 AND MONTH(impact_date) IN (4, 5, 9, 10) THEN 'HIGH_CROP_RISK'
            WHEN had_heat_warning = 1 AND total_precipitation = 0 THEN 'DROUGHT_RISK'
            WHEN total_precipitation > 25 AND MONTH(impact_date) IN (6, 7, 8) THEN 'HARVEST_RISK'
            WHEN had_wind_warning = 1 THEN 'STRUCTURAL_DAMAGE_RISK'
            ELSE 'NORMAL_CONDITIONS'
        END AS agricultural_impact_assessment
        
    FROM impact_analysis
)

SELECT * FROM final_impacts