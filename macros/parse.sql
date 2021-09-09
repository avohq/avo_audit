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


{% macro build_event_info(index, column_names, item) %}
    {% set column = column_names[index] %}
    {% if "event" == column.name | lower %} 
        {{ return(("event_name", item)) }}
    {% elif "version" == column.name | lower %}
        {{ return(("version", item)) }}
    {% else %}
        {{ return(none) }}
    {% endif %}
{% endmacro %}



{% macro convert_to_data_type(item) %}
-- https://www.webforefront.com/django/usebuiltinjinjafilters.html
    {% if item is none %}
        {{ return("Null") }}
    {% elif item is string %}
        {{ return("String") }}
    {% elif item is number %}
        {{ return("Number") }}
    {% elif item is mapping %}
        {{ return("Dict") }}
    {% elif avo_audit.is_date(item) %}
        {{ return("Datetime")}}
    {% endif %}
    {{ return(item_type) }}
{% endmacro %}


{% macro parse_relation(relation, timewindow) %}

    {%- set raw_columns = adapter.get_columns_in_relation(relation) -%}
    {% set check_cols_csv = filter_columns | map(attribute='quoted') | join(', ') %}

    {%- call statement('events', fetch_result=True) -%}
    -- TODO: Move this call statement out of the macro by converting it to runquery
        -- https://docs.getdbt.com/reference/dbt-jinja-functions/statement-blocks
        select * from {{ relation }} WHERE DATE(received_at) >= {{ dbt_date.n_days_ago(20, tz='UTC') }} LIMIT 1000
    {%- endcall -%}

    {%- set events = load_result('events') -%}
    {%- set events_data = events['data'] -%}
    {%- set event_infos = [] %}

--- convert the data into data types
    {%- set new_rows = [] %}
     -- O(n"2)
    {% for rower in events_data %}
        {%- set new_columns = [] %}
        {%- set event_info_columns = ({}) %}
        {% for item in rower %}
            {% set event_info = avo_audit.build_event_info(loop.index-1, raw_columns, item) %}
            {% if event_info != none %}
                {% do event_info_columns.update({event_info[0]: event_info[1]}) %}
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

    -- O(n"2)
    {% for r in new_rows %}
        {% for column in r %}
            {% if not (avo_audit.is_property(loop.index-1, raw_columns)) %}
                {% do r.pop(loop.index-1) %}
            {% endif %}
        {% endfor %}
    {% endfor %}

    {% set property_columns = [] %}

     -- O(n)
    {% for column in raw_columns %}

        {% if (avo_audit.is_property(loop.index-1, raw_columns)) %}
            {% do property_columns.append(column.name) %}
        {% endif %}
    {% endfor %}

    {% set eventNameToVersionToSignatures = [] %}

    -- O(n)
    {% for r in new_rows %}

        {% set event_info = event_infos[loop.index -1] %}

        {% set eventNameToVersionToSignature = [] %}

        {% do eventNameToVersionToSignatures.append((event_info["event_name"], event_info["version"], property_columns, r)) %}

    {% endfor %}

    {%- call statement('create', fetch_result=True) -%}

        CREATE TABLE IF NOT EXISTS
            dbt_avo_test.alex (event_name STRING, version STRING, property_name_mapping STRING, property_signature_mapping STRING);
        INSERT INTO
            dbt_avo_test.alex (event_name, version, property_name_mapping, property_signature_mapping)
        VALUES
            {% for valuesRow in eventNameToVersionToSignatures %}
                ("{{valuesRow[0]}}", "{{valuesRow[1]}}", "{{valuesRow[2]}}", "{{valuesRow[3]}}")
                {% if not loop.last %}
                ,
                {% endif %}
            {% endfor %}

    {%- endcall -%}

    -- TODO: Get rid of this select
    select * from {{ relation }}
{% endmacro %}