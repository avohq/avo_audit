-- This macro is a helper macro to join together multiple tables inside a dataset into 1 table.
-- It is intended to help those who for example have segment + bigquery (Like Avo does) to join together the raw event dataset 
-- into 1 table to then query the volume from that table.
-- Note that running this query in production can be expensive with large amount of data as it runs through all of your events, and inserts it into a new table.

{% macro filter_event_tables(event_relations, event_name_column, event_version_column, event_date_column, event_source_column) %}
  
  {%- set relations = [] -%}
  {% for event_relation in event_relations %}
    
    {%- set column_names = adapter.get_columns_in_relation(event_relation) -%}
    {%- set required_columns = [] -%}
    
    {% for column in column_names %}
      {% if column.name|lower == event_name_column or column.name|lower == event_version_column or column.name|lower == event_date_column or column.name|lower == event_source_column %}
        {% do required_columns.append(column.name) %}
      {% endif %}
    {% endfor %}

    {% if required_columns|length == 4 and event_relation.type == "table" %}
      {% do relations.append(event_relation) %}
    {% endif %}
  {% endfor %}

  {{return(relations) }}
{% endmacro %}
 

{% macro join_schema_into_table(raw_event_schema, event_name_column, event_version_column, event_date_column, event_source_column) %}

{% set event_relations = dbt_utils.get_relations_by_pattern(raw_event_schema, '%') %}

{%- set relations = avo_audit.filter_event_tables(event_relations, event_name_column, event_version_column, event_date_column, event_source_column) -%}


{% for event_relation in relations %}
  
  select
    {{event_name_column}} as {{event_name_column}},
    {{event_version_column}} as {{event_version_column}},
    {{event_date_column}} as {{event_date_column}},
    {{event_source_column}} as {{event_source_column}}
  from 
    {{event_relation}}
  GROUP BY
  {{event_name_column}},
  {{event_version_column}},
  {{event_date_column}},
  {{event_source_column}}
  {% if not loop.last %}
    UNION ALL
  {% endif %}

{% endfor %}
{% endmacro %}