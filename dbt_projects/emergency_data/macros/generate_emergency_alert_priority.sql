
{% macro generate_emergency_alert_priority(risk_level, urgency, certainty, population_affected=null) %}
    CASE 
        WHEN {{ risk_level }} = 'CRITICAL' AND {{ urgency }} = 'IMMEDIATE' THEN 1
        WHEN {{ risk_level }} = 'HIGH' AND {{ urgency }} IN ('IMMEDIATE', 'EXPECTED') THEN 2
        WHEN {{ risk_level }} = 'CRITICAL' AND {{ urgency }} = 'EXPECTED' THEN 2
        WHEN {{ risk_level }} = 'MEDIUM' AND {{ urgency }} = 'IMMEDIATE' THEN 3
        WHEN {{ risk_level }} = 'HIGH' AND {{ urgency }} = 'FUTURE' THEN 3
        WHEN {{ population_affected }} IS NOT NULL AND {{ population_affected }} > 500000 THEN 1
        WHEN {{ population_affected }} IS NOT NULL AND {{ population_affected }} > 100000 THEN 2
        WHEN {{ certainty }} = 'OBSERVED' THEN LEAST(COALESCE(
            CASE 
                WHEN {{ risk_level }} = 'CRITICAL' THEN 1
                WHEN {{ risk_level }} = 'HIGH' THEN 2
                ELSE 3
            END, 3), 2)
        ELSE 4
    END
{% endmacro %}
