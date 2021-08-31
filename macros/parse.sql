{% macro pop_columns_contains(columns, contains) %}
{% set popped_columns=[] %}

{% for column in columns %}
    {% if contains | lower not in column.name | lower %}
        {% do popped_columns.append(column) %}
    {% endif %}
{% endfor %}

{{ return(popped_columns) }}
{% endmacro %} 

{% macro is_date(item) %}
{% set t = modules.datetime.datetime.now() %}
    --  Find better way to figure out if its a date. but this works for now
     {{ return(t.__class__ == item.__class__) }}
{% endmacro %}


{% macro is_property(index, columns) %}

{% set column = columns[index] %}

{% if "context_" | lower in column.name | lower %}
{ return(false)}
{ elif "event" | lower == column.name | lower %}
{{ return(false) }} 
{% else %}
{{ return(true) }}
{% endif %}
{% endmacro %}




{% macro convert_to_data_type(item) %}
-- https://www.webforefront.com/django/usebuiltinjinjafilters.html


{% set item_type = '' %}

{% if item is none %}
{% set item_type = "Null" %}
{% elif item is string %}
{% set item_type = "String" %}
{% elif item is number %}
{% set item_type = "Number" %}
{% elif item is mapping %}
    {% set item_type = "Dict" %}
{% elif avo_audit.is_date(item) %}
{% set item_type = "Datetime" %}
{% else %}
{{ log(item, true) }}


{% endif %}
{{ return(item_type) }}
{% endmacro %}


{% macro parse_relation(relation) %}

{%- set raw_columns = adapter.get_columns_in_relation(relation) -%}

-- {% set filter_columns=avo_audit.pop_columns_contains(dbt_columns, "context_") %}

{% set check_cols_csv = filter_columns | map(attribute='quoted') | join(', ') %}


{%- call statement('events', fetch_result=True) -%}
-- https://docs.getdbt.com/reference/dbt-jinja-functions/statement-blocks

    select * from {{ relation }} WHERE DATE(received_at) >= {{ dbt_date.n_days_ago(20, tz='UTC') }} LIMIT 1000

{%- endcall -%}

{%- set events = load_result('events') -%}
{%- set events_data = events['data'] -%}


--- convert the data into data types
{%- set new_rows = [] %}
{% for rower in events_data %}

{%- set new_columns = [] %}
{% for item in rower %}

{% set item = avo_audit.convert_to_data_type(item) %}
{% do new_columns.append(item) %}
{% endfor %}
{% do new_rows.append(new_columns) %}
{% endfor %}

--- clean up the columns we know we dont want in the array
--- event, context_

{% for r in new_rows [] %}

{% for column in r %}
{% if not (avo_audit.is_property(loop.index-1, raw_columns)) %}
{% do r.pop(loop.index-1) %}
{% endif %}
{% endfor %}
{% endfor %}

{% set property_columns = [] %}


{% for column in raw_columns %}
{{ log("LOOP INDEX", true) }}
{{ log(loop.index, true) }}
{% if (avo_audit.is_property(loop.index-1, raw_columns)) %}
{% do property_columns.append(column.name) %}
{% endif %}
{% endfor %}

{{ log("PROPERTY COLUMNS", true)}}
{{ log(property_columns, true)}}

select * from {{ relation }} limit 1


{% endmacro %}