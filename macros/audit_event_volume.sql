



{% macro get_total_volume(volume_relation, days_ago) %}

{%- call statement('total_events', fetch_result=True) -%}
    select source,  count(source)  from {{ volume_relation }} where DATE(ts) = {{ dbt_date.n_days_ago(days_ago) }} group by source
{%- endcall -%}
 
{%- set query = load_result('total_events') -%}
{%- set query_data = query['data'] -%}
{% if execute %}
  {%- set total_events = query['data'] -%}
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
    count(event_name) as event_count,
    {% for tuple in total_events_2 %}
    {% if loop.first %}
      CASE WHEN source = '{{tuple[0]}}' then {{tuple[1]}}
    {% elif not loop.last %}
     WHEN source = '{{tuple[0]}}' then {{tuple[1]}}
    {% else %}
    WHEN source = '{{tuple[0]}}' then {{tuple[1]}}
    ELSE 0 END AS total_source_events
     {% endif %}
    {% endfor %}
  from {{volume_relation}}
  where DATE(ts) = {{ dbt_date.n_days_ago(6) }}
  group by
  event_name,
  source,
  day
),

union_query as (
select event_name,
 source,
 day,
 SUM(event_count) as event_count,
 SUM(total_source_events) as event_source_count,
 SUM(event_count / total_source_events) as ratio,
 MAX(version) as version
 from query1
 GROUP BY
   event_name,
   source,
   day
UNION ALL (
  select event_name,
 source,
 day,
 SUM(event_count) as event_count,
 SUM(total_source_events) as event_source_count,
 SUM(event_count / total_source_events) as ratio,
MAX(version) as version,
 from query2
 GROUP BY
   event_name,
   source,
   day
)
)


select * from union_query
-- select 
-- a.event_name,
-- a.source,
-- a.day as day_1,
-- b.day as day_2,
-- a.ratio as ratio_1,
-- b.ratio as ratio_2,
-- SUM(a.ratio - b.ratio) // Ready to start comparing ratios with some margins in place. 


{% endmacro %}