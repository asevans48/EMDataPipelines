-- models/staging/stg_fema_disasters.sql
{{ config(
    materialized='table',
    engine='OLAP',
    duplicate_key=['disaster_number'],
    distributed_by=['state'],
    properties={
        "replication_num": "3",
        "storage_format": "DEFAULT",
        "compression": "LZ4"
    },
    partition_by='declaration_date'
) }}

WITH source_data AS (
    SELECT * FROM {{ source('raw_data', 'fema_disaster_declarations') }}
),

standardized AS (
    SELECT
        -- Primary identifiers
        disaster_number,
        state,
        
        -- Dates with proper typing
        CAST(declaration_date AS DATE) AS declaration_date,
        CAST(incident_begin_date AS DATE) AS incident_begin_date,
        CAST(incident_end_date AS DATE) AS incident_end_date,
        
        -- Categorical data
        UPPER(TRIM(incident_type)) AS incident_type,
        UPPER(TRIM(declaration_type)) AS declaration_type,
        
        -- Text fields
        TRIM(title) AS disaster_title,
        TRIM(designated_area) AS designated_area,
        
        -- Numeric fields
        CAST(fy_declared AS INT) AS fiscal_year_declared,
        
        -- Federal compliance metadata
        {{ get_data_classification('PUBLIC') }} AS data_classification,
        {{ get_retention_date('emergency_events') }} AS retention_date,
        
        -- Audit fields
        ingestion_timestamp,
        data_source,
        CURRENT_TIMESTAMP() AS processed_at,
        '{{ var("dbt_version") }}' AS processing_version
        
    FROM source_data
    WHERE disaster_number IS NOT NULL
        AND state IS NOT NULL
        AND declaration_date IS NOT NULL
)

SELECT * FROM standardized
