{{ config(
    materialized='view',
    tags=['staging', 'fema', 'emergency_data']
) }}

/*
Staging model for FEMA disaster declarations
Standardizes and cleans raw FEMA data for downstream processing
*/

WITH source_data AS (
    SELECT 
        disaster_number,
        state,
        declaration_type,
        declaration_date,
        incident_type,
        incident_begin_date,
        incident_end_date,
        title,
        fy_declared,
        disaster_closeout_date,
        place_code,
        designated_area,
        declaration_request_number,
        ingestion_timestamp,
        data_source
    FROM {{ source('raw_data', 'fema_disaster_declarations') }}
    WHERE ingestion_timestamp >= CURRENT_DATE - INTERVAL {{ var('disaster_data_retention') }} DAY
),

cleaned_data AS (
    SELECT
        -- Primary identifiers
        CAST(disaster_number AS STRING) AS disaster_number,
        UPPER(TRIM(state)) AS state_code,
        UPPER(TRIM(declaration_type)) AS declaration_type,
        
        -- Dates with validation
        CASE 
            WHEN declaration_date IS NOT NULL 
            THEN CAST(declaration_date AS TIMESTAMP)
            ELSE NULL 
        END AS declaration_date,
        
        CASE 
            WHEN incident_begin_date IS NOT NULL 
            THEN CAST(incident_begin_date AS TIMESTAMP)
            ELSE NULL 
        END AS incident_begin_date,
        
        CASE 
            WHEN incident_end_date IS NOT NULL 
            THEN CAST(incident_end_date AS TIMESTAMP)
            ELSE NULL 
        END AS incident_end_date,
        
        CASE 
            WHEN disaster_closeout_date IS NOT NULL 
            THEN CAST(disaster_closeout_date AS TIMESTAMP)
            ELSE NULL 
        END AS disaster_closeout_date,
        
        -- Incident details
        UPPER(TRIM(incident_type)) AS incident_type,
        TRIM(title) AS disaster_title,
        CAST(fy_declared AS INT) AS fiscal_year_declared,
        
        -- Location information
        CAST(place_code AS STRING) AS place_code,
        TRIM(designated_area) AS designated_area,
        
        -- Additional metadata
        CAST(declaration_request_number AS STRING) AS declaration_request_number,
        
        -- Data lineage
        CAST(ingestion_timestamp AS TIMESTAMP) AS ingestion_timestamp,
        TRIM(data_source) AS data_source,
        
        -- Derived fields
        CASE 
            WHEN incident_end_date IS NOT NULL AND incident_begin_date IS NOT NULL
            THEN DATEDIFF(incident_end_date, incident_begin_date)
            ELSE NULL
        END AS incident_duration_days,
        
        CASE 
            WHEN disaster_closeout_date IS NOT NULL
            THEN 'CLOSED'
            ELSE 'OPEN'
        END AS disaster_status,
        
        -- Federal compliance fields
        CURRENT_TIMESTAMP AS processed_timestamp,
        '{{ var("data_classification.internal") }}' AS data_classification,
        
        -- Data quality flags
        CASE 
            WHEN disaster_number IS NULL THEN 1
            WHEN state IS NULL OR LENGTH(TRIM(state)) = 0 THEN 1
            WHEN declaration_date IS NULL THEN 1
            ELSE 0
        END AS data_quality_issues
        
    FROM source_data
),

final AS (
    SELECT 
        *,
        -- Generate surrogate key for tracking
        MD5(CONCAT(
            COALESCE(disaster_number, ''),
            COALESCE(state_code, ''),
            COALESCE(CAST(declaration_date AS STRING), '')
        )) AS disaster_key
        
    FROM cleaned_data
    WHERE data_quality_issues = 0  -- Filter out records with quality issues
)

SELECT * FROM final