{% macro filter_event_tables(event_relations, event_name_column, event_version_column, event_date_column) %}
  {%- set relations = [] -%}
  {% for event_relation in event_relations %}
    {%- set column_names = adapter.get_columns_in_relation(event_relation) -%}
    {%- set required_columns = [] -%}
    {% for column in column_names %}
      {% if column.name == event_name_column or column.name == event_version_column or column.name == event_date_column %}
        {% do required_columns.append(column.name) %}
      {% endif %}
    {% endfor %}

    {% if required_columns|length == 3 %}
      {% do relations.append(event_relation) %}
    {% endif %}
  {% endfor %}

  {{return(relations) }}
{% endmacro %}
 

{% macro join_schema_into_table(raw_event_schema, event_name_column, event_version_column, event_date_column) %}
{% set event_relations = dbt_utils.get_relations_by_pattern(raw_event_schema, '%') %}

{%- set relations = filter_event_tables(event_relations, event_name_column, event_version_column, event_date_column) -%}

{% for event_relation in relations %}
  select
    ROW_NUMBER() over () as ID,
    {{event_name_column}}, 
    {{event_version_column}}, 
    {{event_date_column}} 
  from 
    {{event_relation}}
  {% if not loop.last %}
    UNION ALL 
  {% endif %}
{% endfor %}
{% endmacro %}