{% macro pop_columns_contains(columns, contains) %}
{% set popped_columns=[] %}

{% for column in columns %}
    {% if contains | lower not in column.name | lower %}
        {% do popped_columns.append(column) %}
    {% endif %}
{% endfor %}

{{ return(popped_columns) }}
{% endmacro %}



{% macro parse_relation(relation) %}

{%- set dbt_columns = adapter.get_columns_in_relation(relation) -%}

{% set filter_columns=avo_audit.pop_columns_contains(dbt_columns, "context_") %}

{% set check_cols_csv = filter_columns | map(attribute='quoted') | join(', ') %}


{% set property_names = [] %}
{% set property_signatures = [] %}

{% for column in filter_columns %}

{% endfor %}


select TOP 10 *
INTO #TempTable
FROM {{ relation }}

EXEC tempdb.dbo.sp_help N'#TempTable';

{% endmacro %}