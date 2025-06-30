
{% macro get_seasonal_risk_factor(event_date, incident_type) %}
    CASE 
        WHEN MONTH({{ event_date }}) IN (6, 7, 8) AND UPPER({{ incident_type }}) IN ('WILDFIRE', 'DROUGHT', 'HEAT WAVE') THEN 1.5
        WHEN MONTH({{ event_date }}) IN (6, 7, 8, 9) AND UPPER({{ incident_type }}) = 'HURRICANE' THEN 2.0
        WHEN MONTH({{ event_date }}) IN (12, 1, 2) AND UPPER({{ incident_type }}) IN ('WINTER STORM', 'ICE STORM', 'BLIZZARD') THEN 1.8
        WHEN MONTH({{ event_date }}) IN (3, 4, 5) AND UPPER({{ incident_type }}) IN ('TORNADO', 'SEVERE STORM', 'FLOOD') THEN 1.3
        ELSE 1.0
    END
{% endmacro %}
