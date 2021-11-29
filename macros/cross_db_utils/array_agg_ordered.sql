{% macro array_agg_ordered(column_name, sort_column, sort, new_name) %}
  {{ return(adapter.dispatch('array_agg_ordered', 'avo_audit')(column_name, sort_column, sort, new_name)) }}
{% endmacro %}


{% macro default__array_agg_ordered(column_name, sort_column, sort, new_name) %}

    ARRAY_AGG({{column_name}} order by {{sort_column}} {{sort}}) as {{new_name}}

{% endmacro %}


{% macro bigquery__array_agg_ordered(column_name, sort_column, sort, new_name) %}

   ARRAY_AGG({{column_name}} order by {{sort_column}} {{sort}}) as {{new_name}}

{% endmacro %}

{% macro snowflake__array_agg_ordered(column_name, sort_column, sort, new_name) %}

  ARRAY_AGG({{column_name}}) within group (order by {{sort_column}} {{sort}}) as {{new_name}}

{% endmacro %}
