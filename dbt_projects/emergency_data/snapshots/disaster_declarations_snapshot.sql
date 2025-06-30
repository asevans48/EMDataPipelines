-- snapshots/disaster_declarations_snapshot.sql
{% snapshot disaster_declarations_snapshot %}

{{
    config(
      target_database='emergency_data_dev',
      target_schema='snapshots',
      unique_key='disaster_number',
      strategy='timestamp',
      updated_at='processed_at',
      invalidate_hard_deletes=True
    )
}}

SELECT 
    disaster_number,
    state,
    declaration_date,
    incident_type,
    declaration_type,
    disaster_title,
    incident_begin_date,
    incident_end_date,
    designated_area,
    fiscal_year_declared,
    data_classification,
    retention_date,
    processed_at,
    
    -- Snapshot metadata for federal compliance
    '{{ var("compliance_frameworks") | join(",") }}' AS applicable_frameworks,
    'DISASTER_DECLARATION_TRACKING' AS snapshot_purpose,
    CURRENT_USER() AS snapshot_created_by
    
FROM {{ ref('stg_fema_disasters') }}

{% endsnapshot %}
