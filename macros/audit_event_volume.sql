



{% macro get_total_volume(volume_relation, days_ago) %}

{%- call statement('total_events', fetch_result=True) -%}
    select count(*) from {{ volume_relation }} where DATE(sent_at) = {{ dbt_date.n_days_ago(days_ago) }}
{%- endcall -%}

{%- set query = load_result('total_events') -%}
{%- set query_data = query['data'] -%}

{% if execute %}
  {%- set total_events  = query['data'][0][0] -%}
{% else %}
  {%- set total_events = 0 -%}
{% endif %}


{{return(total_events)}}

{% endmacro %}


{% macro audit_event_volume(volume_relation) %}


{%- set total_events_1 = get_total_volume(volume_relation, 1) -%}
{%- set total_events_2 = get_total_volume(volume_relation, 2) -%}


{{log(total_events_1, true)}}
{{log(total_events_2, true)}}



with query1 as (
select 
    event,
    version,
    DATE(sent_at) as day,
    count(event) as event_count_1,
    {{total_events_2}} as total_events_1,
    (count(event) / {{total_events_1}}) as ratio_1
  from {{volume_relation}}
  where DATE(sent_at) = {{ dbt_date.n_days_ago(1) }}
  group by
  event,
  version,
  day
), query2 as 
(
select 
    event,
    version,
    DATE(sent_at) as day,
    count(event) as event_count_2,
    {{total_events_2}} as total_events_2,
    (count(event) / {{total_events_2}}) as ratio_2
  from {{volume_relation}}
  where DATE(sent_at) = {{ dbt_date.n_days_ago(2) }}
  group by
  event,
  version
  day,
), 
union_query as (
  select
  * from query1
  UNION ALL
  (select * from query2)
)

select 
  query1.event as event_1,
  SUM(query1.event_count) as event_count_1,
  {{total_events_1}} as total_events_1,
  SUM(query1.ratio) as ratio_1,
  count(query2.event) as event_count_2,
  {{total_events_2}} as total_events_2,
  (count(query2.event) / {{total_events_2}}) as ratio_2
from 
  query2
FULL JOIN query2 USING (event, version)
GROUP BY
  event_1



{% endmacro %}