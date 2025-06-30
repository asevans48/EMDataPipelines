-- models/staging/stg_usda_data.sql
{{ config(
    materialized='table',
    engine='OLAP',
    duplicate_key=['program_year', 'state_code', 'county_code', 'commodity'],
    distributed_by=['state_code'],
    properties={
        "replication_num": "3",
        "storage_format": "DEFAULT",
        "compression": "LZ4"
    },
    partition_by='program_year'
) }}

WITH source_data AS (
    SELECT * FROM {{ source('raw_data', 'usda_agricultural_data') }}
),

standardized AS (
    SELECT
        -- Program identification
        CAST(program_year AS INT) AS program_year,
        UPPER(TRIM(state_code)) AS state_code,
        LPAD(TRIM(county_code), 3, '0') AS county_code,
        
        -- Commodity information
        UPPER(TRIM(commodity)) AS commodity_name,
        UPPER(TRIM(practice)) AS farming_practice,
        
        -- Financial data with validation
        CASE 
            WHEN coverage_level BETWEEN 0 AND 100 THEN ROUND(CAST(coverage_level AS DECIMAL(5,2)), 2)
            ELSE NULL 
        END AS coverage_level_percent,
        
        CASE 
            WHEN premium_amount >= 0 THEN ROUND(CAST(premium_amount AS DECIMAL(12,2)), 2)
            ELSE NULL 
        END AS premium_amount_usd,
        
        CASE 
            WHEN liability_amount >= 0 THEN ROUND(CAST(liability_amount AS DECIMAL(12,2)), 2)
            ELSE NULL 
        END AS liability_amount_usd,
        
        CASE 
            WHEN indemnity_amount >= 0 THEN ROUND(CAST(indemnity_amount AS DECIMAL(12,2)), 2)
            ELSE NULL 
        END AS indemnity_amount_usd,
        
        -- Loss ratio calculation
        CASE 
            WHEN premium_amount > 0 AND indemnity_amount >= 0 THEN
                ROUND(indemnity_amount / premium_amount, 4)
            ELSE NULL
        END AS loss_ratio,
        
        -- Risk categorization
        CASE 
            WHEN indemnity_amount > liability_amount * 0.5 THEN 'HIGH_LOSS'
            WHEN indemnity_amount > liability_amount * 0.25 THEN 'MODERATE_LOSS'
            WHEN indemnity_amount > 0 THEN 'LOW_LOSS'
            ELSE 'NO_LOSS'
        END AS loss_category,
        
        -- Federal compliance metadata
        {{ get_data_classification('PUBLIC') }} AS data_classification,
        {{ get_retention_date('agricultural_data') }} AS retention_date,
        
        -- Audit fields
        ingestion_timestamp,
        data_source,
        CURRENT_TIMESTAMP() AS processed_at,
        '{{ var("dbt_version") }}' AS processing_version
        
    FROM source_data
    WHERE program_year IS NOT NULL
        AND state_code IS NOT NULL
        AND county_code IS NOT NULL
        AND commodity IS NOT NULL
)

SELECT * FROM standardized