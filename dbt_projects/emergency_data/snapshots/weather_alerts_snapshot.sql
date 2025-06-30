
-- snapshots/weather_alerts_snapshot.sql
{% snapshot weather_alerts_snapshot %}

{{
    config(
      target_database='emergency_data_dev',
      target_schema='snapshots',
      unique_key='alert_id',
      strategy='timestamp',
      updated_at='processed_at',
      invalidate_hard_deletes=True
    )
}}

SELECT 
    alert_id,
    weather_event,
    severity_level,
    urgency_level,
    certainty_level,
    alert_headline,
    alert_description,
    public_instruction,
    state_code,
    affected_area_description,
    effective_datetime,
    expires_datetime,
    effective_date,
    expires_date,
    alert_duration_hours,
    risk_level,
    data_classification,
    retention_date,
    processed_at,
    
    -- Snapshot metadata for federal compliance
    '{{ var("compliance_frameworks") | join(",") }}' AS applicable_frameworks,
    'WEATHER_ALERT_TRACKING' AS snapshot_purpose,
    CURRENT_USER() AS snapshot_created_by
    
FROM {{ ref('stg_noaa_weather') }}

{% endsnapshot %}
