-- snapshots/emergency_events_summary_snapshot.sql
{% snapshot emergency_events_summary_snapshot %}

{{
    config(
      target_database='emergency_data_dev',
      target_schema='snapshots',
      unique_key='event_id',
      strategy='timestamp',
      updated_at='last_updated',
      invalidate_hard_deletes=True
    )
}}

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
    is_long_duration_event,
    is_high_impact_event,
    funding_eligibility,
    last_updated,
    
    -- Snapshot metadata for federal compliance
    '{{ var("compliance_frameworks") | join(",") }}' AS applicable_frameworks,
    'EMERGENCY_EVENTS_TRACKING' AS snapshot_purpose,
    CURRENT_USER() AS snapshot_created_by
    
FROM {{ ref('emergency_events') }}

{% endsnapshot %}