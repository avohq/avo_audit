



{% macro get_total_volume(volume_relation, days_ago) %}

{%- call statement('total_events', fetch_result=True) -%}
    select source,  count(source)  from {{ volume_relation }} where DATE(ts) = {{ dbt_date.n_days_ago(days_ago) }} group by source
{%- endcall -%}

{%- set query = load_result('total_events') -%}
{%- set query_data = query['data'] -%}
{% if execute %}
  {%- set total_events  = query['data'] -%}
{% else %}
  {%- set total_events = [] -%}
{% endif %}


{{return(total_events)}}

{% endmacro %}


{% macro audit_event_volume(volume_relation) %}


{%- set total_events_1 = get_total_volume(volume_relation, 5) -%}
{%- set total_events_2 = get_total_volume(volume_relation, 6) -%}


{{log(total_events_1, true)}}
{{log(total_events_2, true)}}

with query1 as (
select 
    event_name,
    source,
    max(version) as version,
    DATE(ts) as day,
    count(event_name) as event_count,
    {% for tuple in total_events_1 %}
    {% if loop.first %}
      CASE WHEN source = '{{tuple[0]}}' then {{tuple[1]}}
    {% elif not loop.last %}
     WHEN source = '{{tuple[0]}}' then {{tuple[1]}}
    {% else %}
    WHEN source = '{{tuple[0]}}' then {{tuple[1]}}
    ELSE 0 END AS total_source_events
     {% endif %}
    {% endfor %}
    -- (count(event_name) / {{total_events_1[source]}}) as ratio
  from {{volume_relation}}
  where DATE(ts) = {{ dbt_date.n_days_ago(5) }}
  group by
  event_name,
  source,
  day
), query2 as 
(
select 
    event_name,
    source,
    max(version) as version,
    DATE(ts) as day,
    count(event_name) as event_count
    -- {{total_events_2[source]}} as total_events,
    -- (count(event_name) / {{total_events_2[source]}}) as ratio
  from {{volume_relation}}
  where DATE(ts) = {{ dbt_date.n_days_ago(6) }}
  group by
  event_name,
  source,
  day
)

select * from query1

-- select 
--   CASE WHEN query1.event_name IS NOT NULL THEN query1.event_name ELSE query2.event_name END as event,
--   CASE WHEN query1.source IS NOT NULL THEN query1.source ELSE query2.source END as source,
--   MAX(CASE WHEN query1.version IS NOT NULL THEN query1.version ELSE query2.version END) as version,
--   MAX(query1.day) as day_1,
--   SUM(query1.event_count) as event_count_1,
--   {{total_events_1[source]}} as total_events_1,
--   SUM(query1.ratio) as ratio_1,
--   MAX(query2.day) as day_2,
--   SUM(query2.event_count) as event_count_2,
--   {{total_events_2[source]}} as total_events_2,
--   SUM(query2.ratio) as ratio_2
-- from 
--   query1
-- FULL JOIN query2 ON (query1.event_name = query2.event_name AND query1.source = query2.source)
-- GROUP BY
--   event,
--   source
-- ORDER BY 
-- event


{% endmacro %}