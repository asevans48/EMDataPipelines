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