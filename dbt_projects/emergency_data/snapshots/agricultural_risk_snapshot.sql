
-- snapshots/agricultural_risk_snapshot.sql
{% snapshot agricultural_risk_snapshot %}

{{
    config(
      target_database='emergency_data_dev',
      target_schema='snapshots',
      unique_key='program_year||state_code||county_code||commodity_name',
      strategy='timestamp',
      updated_at='processed_at',
      invalidate_hard_deletes=True
    )
}}

SELECT 
    program_year,
    state_code,
    county_code,
    commodity_name,
    farming_practice,
    coverage_level_percent,
    premium_amount_usd,
    liability_amount_usd,
    indemnity_amount_usd,
    loss_ratio,
    loss_category,
    data_classification,
    retention_date,
    processed_at,
    
    -- Snapshot metadata for federal compliance
    '{{ var("compliance_frameworks") | join(",") }}' AS applicable_frameworks,
    'AGRICULTURAL_RISK_TRACKING' AS snapshot_purpose,
    CURRENT_USER() AS snapshot_created_by
    
FROM {{ ref('stg_usda_data') }}

{% endsnapshot %}