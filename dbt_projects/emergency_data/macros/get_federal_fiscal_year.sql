
{% macro get_federal_fiscal_year(date_column) %}
    CASE 
        WHEN MONTH({{ date_column }}) >= 10 THEN YEAR({{ date_column }}) + 1
        ELSE YEAR({{ date_column }})
    END
{% endmacro %}