{% macro categorize_disaster_impact(risk_score) %}
    CASE 
        WHEN {{ risk_score }} >= 6 THEN 'SEVERE'
        WHEN {{ risk_score }} >= 4 THEN 'MODERATE'
        WHEN {{ risk_score }} >= 2 THEN 'MINOR'
        ELSE 'MINIMAL'
    END
{% endmacro %}