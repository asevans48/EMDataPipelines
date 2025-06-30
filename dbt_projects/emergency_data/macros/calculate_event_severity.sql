{% macro calculate_event_severity(incident_type, duration_days=null, affected_population=null) %}
    CASE 
        WHEN UPPER({{ incident_type }}) IN ('HURRICANE', 'MAJOR DISASTER', 'EARTHQUAKE') THEN 'CRITICAL'
        WHEN UPPER({{ incident_type }}) IN ('SEVERE STORM', 'FLOOD', 'WILDFIRE', 'TORNADO') THEN 'HIGH'
        WHEN UPPER({{ incident_type }}) IN ('WINTER STORM', 'DROUGHT', 'ICE STORM') THEN 'MEDIUM'
        WHEN {{ duration_days }} IS NOT NULL AND {{ duration_days }} > 30 THEN 'HIGH'
        WHEN {{ affected_population }} IS NOT NULL AND {{ affected_population }} > 100000 THEN 'HIGH'
        ELSE 'LOW'
    END
{% endmacro %}