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


{% macro is_event_info(index, column_names) %}
    
    {% set column = column_names[index] %}

    {% if "event" == column.name | lower %} 
        {{ return(true) }}
    {% elif "version" == column.name | lower %}
        {{ return(true) }}
    {% else %}
        {{ return(false) }}
    {% endif %}
{% endmacro %}



{% macro convert_to_data_type(item) %}
-- https://www.webforefront.com/django/usebuiltinjinjafilters.html

-- intialize variable as empty 
{% set item_type = '' %}
{% if item is none %}
{% set item_type = "Null" %}
{% elif item is string %}
{% set item_type = "String" %}
{% elif item is number %}
{% set item_type = "Number" %}
{% elif item is mapping %}
{   % set item_type = "Dict" %}
{% elif avo_audit.is_date(item) %}
{% set item_type = "Datetime" %}
{% else %}
{{ log(item, true) }}
{% endif %}
{{ return(item_type) }}
{% endmacro %}


{% macro parse_relation(relation, timewindow) %}

{%- set raw_columns = adapter.get_columns_in_relation(relation) -%}


{% set check_cols_csv = filter_columns | map(attribute='quoted') | join(', ') %}


{%- call statement('events', fetch_result=True) -%}
-- https://docs.getdbt.com/reference/dbt-jinja-functions/statement-blocks

    select * from {{ relation }} WHERE DATE(received_at) >= {{ dbt_date.n_days_ago(20, tz='UTC') }} LIMIT 1000

{%- endcall -%}

{%- set events = load_result('events') -%}
{%- set events_data = events['data'] -%}

{%- set event_infos = [] %}

--- convert the data into data types
{%- set new_rows = [] %}
{% for rower in events_data %}

{%- set new_columns = [] %}
{%- set event_info_columns = [] %}
{% for item in rower %}
{% if avo_audit.is_event_info(loop.index-1, raw_columns) %}
    {% do event_info_columns.append((raw_columns[loop.index-1].name, item)) %}
{% else %}
{% set item = avo_audit.convert_to_data_type(item) %}
{% do new_columns.append(item) %}
{% endif %}
{% endfor %}
{% do new_rows.append(new_columns) %}
{% do event_infos.append(event_info_columns) %}
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

{% if (avo_audit.is_property(loop.index-1, raw_columns)) %}
{% do property_columns.append(column.name) %}
{% endif %}
{% endfor %}

{{ log(event_infos, true)}}




select * from {{ relation }} limit 1


{% endmacro %}