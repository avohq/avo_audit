{% macro find_signals(relation) %}
  {{ return(adapter.dispatch('find_signals', 'avo_audit')(relation)) }}
{% endmacro %}


{% macro default__find_signals(relation) %}

  select 
    * 
  from {{relation}}
  where
    (select signal FROM UNNEST(signals) AS signal where signal = 1 GROUP BY signal) = 1
    OR (select signal from UNNEST(signals) AS signal where signal = -1 GROUP BY signal) = -1

{% endmacro %}


{% macro bigquery__find_signals(relation) %}

select 
  * 
from {{relation}}
where
    (select signal FROM UNNEST(signals) AS signal where signal = 1 GROUP BY signal) = 1
    OR (select signal from UNNEST(signals) AS signal where signal = -1 GROUP BY signal) = -1

{% endmacro %}

{% macro snowflake__find_signals(relation) %}
  select
    *,
    X.value::INTEGER as signal
  from {{relation}}, LATERAL FLATTEN({{relation}}.signals) X
  where signal = 1 OR signal = -1


{% endmacro %}
