{% macro is_date(item) %}
    {% set t = modules.datetime.datetime.now() %}
    --  Find better way to figure out if its a date. but this works for now
    {{ return(t.__class__ == item.__class__) }}
{% endmacro %} 


{% macro is_property(index, columns) %}
    {% set column = columns[index] %}
    {% if "context_" in column.name | lower %}
        {{ return(false) }}
    {% elif "event" == column.name | lower %}
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

{% macro signature_name_mapping(relation) %}

    {%- set column_names = adapter.get_columns_in_relation(relation) -%}

    {%- set property_columns = [] %}

    {% for column in column_names %}

        {% if (avo_audit.is_property(loop.index-1, column_names)) %}
            {% do property_columns.append(column.name) %}
        {% endif %}
    {% endfor %}

    {{ return(property_columns) }}
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
{% endmacro %}

{% macro convert_to_type(column_name) %}

    {{ column_name }}

{% endmacro %}


{% macro parse_relation(relation, timewindow) %}

    {%- set column_names = adapter.get_columns_in_relation(relation) -%}
    {% set check_cols_csv = filter_columns | map(attribute='quoted') | join(', ') %}

    {%- call statement('events', fetch_result=True) -%}
    -- TODO: Move this call statement out of the macro by converting it to runquery
        -- https://docs.getdbt.com/reference/dbt-jinja-functions/statement-blocks
        select * from {{ relation }} WHERE DATE(received_at) >= {{ dbt_date.n_days_ago(20, tz='UTC') }} LIMIT 1000
    {%- endcall -%}

    {%- set events = load_result('events') -%}
    {%- set events_data = events['data'] -%}
    {%- set event_info_rows = [] %}

--- convert the data into data types
    {%- set property_signature_type_rows = [] %}
     -- O(n"2)
    {% for rower in events_data %}
        {%- set new_columns = [] %}
        {%- set event_info_columns = ({}) %}
        {% for item in rower %}
            {% set event_info = avo_audit.build_event_info(loop.index-1, column_names, item) %}
            {% if event_info != none %}
                {% do event_info_columns.update({event_info[0]: event_info[1]}) %}
            {% else %}
                {% set item = avo_audit.convert_to_data_type(item) %}
                {% do new_columns.append(item) %}
            {% endif %}
        {% endfor %}
        {% do property_signature_type_rows.append(new_columns) %}
        {% do event_info_rows.append(event_info_columns) %}
    {% endfor %}

--- clean up the columns we know we dont want in the array
--- event, context_

    {% set property_columns = [] %}

     -- O(n)
    {% for column in column_names %}

        {% if (avo_audit.is_property(loop.index-1, column_names)) %}
            {% do property_columns.append(column.name) %}
        {% endif %}
    {% endfor %}

    {% set eventNameToVersionToSignatures = [] %}

    -- O(n)
    {% for r in property_signature_type_rows %}

        {% set event_info = event_info_rows[loop.index -1] %}

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

    {%- set names = avo_audit.signature_name_mapping(relation) %}


    select 
        [{% for name in names %}
            ifnull(CAST({{name}} as string), 'Null')
            {% if not loop.last %}
                ,
            {% endif %}
        {% endfor %}]
         as fields_asd
    from {{ relation }}
    
        
    

{% endmacro %}