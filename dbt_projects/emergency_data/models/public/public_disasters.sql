-- models/public/public_disasters.sql
{{ config(
    materialized='table',
    engine='OLAP',
    duplicate_key=['disaster_id'],
    distributed_by=['state_code'],
    properties={
        "replication_num": "3",
        "storage_format": "DEFAULT",
        "compression": "LZ4"
    },
    partition_by='declaration_date',
    post_hook="CREATE MATERIALIZED VIEW IF NOT EXISTS {{ this }}_api_mv DISTRIBUTED BY HASH(state_code) REFRESH ASYNC AS SELECT * FROM {{ this }} WHERE is_active = TRUE"
) }}

WITH active_disasters AS (
    SELECT
        event_id,
        event_type,
        event_subtype,
        event_category,
        event_name,
        state_code,
        state_name,
        event_date,
        event_start_date,
        event_end_date,
        event_duration_days,
        risk_level,
        event_season,
        federal_fiscal_year,
        is_high_impact_event,
        funding_eligibility
    FROM {{ ref('emergency_events') }}
    WHERE event_type = 'DISASTER'
        AND data_classification = 'PUBLIC'
        AND event_date >= DATE_SUB(CURRENT_DATE(), INTERVAL 365 DAY)  -- Last year only for API
),

public_disasters AS (
    SELECT
        -- API-friendly identifiers
        event_id AS disaster_id,
        CONCAT(state_code, '_', DATE_FORMAT(event_date, '%Y%m%d'), '_', ROW_NUMBER() OVER (PARTITION BY state_code, event_date ORDER BY event_id)) AS public_disaster_code,
        
        -- Basic disaster information
        event_subtype AS disaster_type,
        event_category AS disaster_category,
        event_name AS disaster_title,
        
        -- Geographic information
        state_code,
        state_name AS state_name,
        
        -- Temporal information
        event_date AS declaration_date,
        event_start_date AS incident_begin_date,
        event_end_date AS incident_end_date,
        COALESCE(event_duration_days, 0) AS duration_days,
        event_season AS season,
        federal_fiscal_year,
        
        -- Risk and impact assessment
        risk_level,
        CASE 
            WHEN is_high_impact_event THEN 'HIGH'
            ELSE 'STANDARD'
        END AS impact_level,
        
        -- Public safety information
        CASE 
            WHEN risk_level IN ('HIGH', 'CRITICAL') THEN 'ACTIVE_MONITORING_REQUIRED'
            WHEN event_end_date IS NULL OR event_end_date > CURRENT_DATE() THEN 'ONGOING'
            ELSE 'RESOLVED'
        END AS status,
        
        -- Funding and assistance information
        funding_eligibility AS federal_assistance_available,
        
        -- API metadata
        TRUE AS is_active,
        'PUBLIC' AS data_classification,
        'emergency_management_api' AS data_source,
        CURRENT_TIMESTAMP() AS last_updated,
        
        -- Rate limiting and usage tracking
        DATE_FORMAT(CURRENT_TIMESTAMP(), '%Y-%m-%d %H:00:00') AS api_hour_bucket,
        
        -- Compliance information
        'This data is provided for public emergency preparedness purposes. Usage subject to federal data retention and privacy policies.' AS usage_notice
        
    FROM active_disasters
)

SELECT * FROM public_disasters

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

-- models/public/public_agricultural_data.sql
{{ config(
    materialized='table',
    engine='OLAP',
    duplicate_key=['ag_data_id'],
    distributed_by=['state_code'],
    properties={
        "replication_num": "3",
        "storage_format": "DEFAULT",
        "compression": "LZ4"
    },
    partition_by='program_year'
) }}

WITH agricultural_summary AS (
    SELECT
        state_code,
        county_code,
        program_year,
        commodity_name,
        farming_practice,
        coverage_level_percent,
        premium_amount_usd,
        liability_amount_usd,
        indemnity_amount_usd,
        loss_ratio,
        loss_category
    FROM {{ ref('stg_usda_data') }}
    WHERE program_year >= YEAR(CURRENT_DATE()) - 5  -- Last 5 years
        AND data_classification = 'PUBLIC'
),

county_aggregations AS (
    SELECT
        state_code,
        county_code,
        program_year,
        commodity_name,
        
        -- Statistical aggregations for privacy
        COUNT(*) AS policy_count,
        ROUND(AVG(coverage_level_percent), 1) AS avg_coverage_level,
        ROUND(SUM(premium_amount_usd), 0) AS total_premiums,
        ROUND(SUM(liability_amount_usd), 0) AS total_liability,
        ROUND(SUM(indemnity_amount_usd), 0) AS total_indemnities,
        ROUND(AVG(loss_ratio), 3) AS avg_loss_ratio,
        
        -- Risk indicators
        COUNT(CASE WHEN loss_category = 'HIGH_LOSS' THEN 1 END) AS high_loss_policies,
        COUNT(CASE WHEN loss_category = 'NO_LOSS' THEN 1 END) AS no_loss_policies,
        
        -- Performance metrics
        ROUND(SUM(indemnity_amount_usd) / NULLIF(SUM(premium_amount_usd), 0), 3) AS county_loss_ratio
        
    FROM agricultural_summary
    GROUP BY state_code, county_code, program_year, commodity_name
    HAVING COUNT(*) >= 5  -- Privacy threshold: minimum 5 policies to publish
),

public_agricultural_data AS (
    SELECT
        -- API identifiers
        MD5(CONCAT(state_code, '_', county_code, '_', program_year, '_', commodity_name)) AS ag_data_id,
        CONCAT(state_code, county_code, '_', program_year, '_', UPPER(LEFT(commodity_name, 4))) AS public_ag_code,
        
        -- Geographic identifiers
        state_code,
        county_code,
        
        -- Program information
        program_year,
        commodity_name AS crop_type,
        
        -- Aggregated statistics (privacy-preserving)
        policy_count AS number_of_policies,
        avg_coverage_level AS average_coverage_percent,
        
        -- Financial aggregates (rounded for privacy)
        CASE 
            WHEN total_premiums >= 1000000 THEN ROUND(total_premiums, -5)  -- Round to nearest $100k
            WHEN total_premiums >= 100000 THEN ROUND(total_premiums, -4)   -- Round to nearest $10k
            ELSE ROUND(total_premiums, -3)                                  -- Round to nearest $1k
        END AS total_premiums_usd,
        
        CASE 
            WHEN total_liability >= 1000000 THEN ROUND(total_liability, -5)
            WHEN total_liability >= 100000 THEN ROUND(total_liability, -4)
            ELSE ROUND(total_liability, -3)
        END AS total_liability_usd,
        
        CASE 
            WHEN total_indemnities >= 1000000 THEN ROUND(total_indemnities, -5)
            WHEN total_indemnities >= 100000 THEN ROUND(total_indemnities, -4)
            ELSE ROUND(total_indemnities, -3)
        END AS total_indemnities_usd,
        
        -- Risk assessment
        county_loss_ratio AS loss_ratio,
        ROUND(high_loss_policies / policy_count * 100, 1) AS high_loss_percentage,
        ROUND(no_loss_policies / policy_count * 100, 1) AS no_loss_percentage,
        
        -- Risk categorization for public use
        CASE 
            WHEN county_loss_ratio > 1.5 THEN 'HIGH_RISK'
            WHEN county_loss_ratio > 1.0 THEN 'ELEVATED_RISK'
            WHEN county_loss_ratio > 0.5 THEN 'MODERATE_RISK'
            ELSE 'LOW_RISK'
        END AS risk_category,
        
        -- Agricultural climate resilience indicator
        CASE 
            WHEN county_loss_ratio < 0.3 AND high_loss_policies = 0 THEN 'HIGHLY_RESILIENT'
            WHEN county_loss_ratio < 0.6 AND high_loss_policies <= policy_count * 0.1 THEN 'RESILIENT'
            WHEN county_loss_ratio < 1.0 THEN 'MODERATELY_RESILIENT'
            ELSE 'VULNERABLE'
        END AS climate_resilience_indicator,
        
        -- API metadata
        'PUBLIC' AS data_classification,
        'usda_agricultural_api' AS data_source,
        CURRENT_TIMESTAMP() AS last_updated,
        DATE_FORMAT(CURRENT_TIMESTAMP(), '%Y-%m-%d %H:00:00') AS api_hour_bucket,
        
        -- Privacy and compliance notice
        'Data aggregated to protect individual producer privacy. Minimum threshold reporting applied.' AS privacy_notice,
        'USDA RMA data subject to federal privacy and statistical disclosure policies.' AS compliance_notice
        
    FROM county_aggregations
    WHERE policy_count >= 5  -- Ensure privacy threshold
)

SELECT * FROM public_agricultural_data