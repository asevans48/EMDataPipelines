-- macros/emergency_analytics.sql
{% macro calculate_risk_score(temperature_col, precipitation_col, wind_col) %}
    CASE 
        WHEN {{ temperature_col }} < -10 OR {{ temperature_col }} > 40 THEN 3
        WHEN {{ temperature_col }} < -5 OR {{ temperature_col }} > 35 THEN 2  
        WHEN {{ temperature_col }} < 0 OR {{ temperature_col }} > 30 THEN 1
        ELSE 0
    END +
    CASE 
        WHEN {{ precipitation_col }} > 50 THEN 3
        WHEN {{ precipitation_col }} > 25 THEN 2
        WHEN {{ precipitation_col }} > 10 THEN 1
        ELSE 0
    END +
    CASE 
        WHEN {{ wind_col }} > 80 THEN 3
        WHEN {{ wind_col }} > 50 THEN 2
        WHEN {{ wind_col }} > 30 THEN 1
        ELSE 0
    END
{% endmacro %}