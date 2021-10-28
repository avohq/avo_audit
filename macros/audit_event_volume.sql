{% macro new_audit_event_volume(volume_relation, end_date, days_back, days_lag, event_name_column, event_version_column, event_date_column, event_source_column) %}

{%- set total_days = days_back + days_lag -%}
{% set threshold = 3.5 %}

with union_query as (
  {% for i in range(total_days) %}

{%- set d = dbt_date.n_days_ago(i, end_date) -%}

with all_events_query as (
  select {{event_name_column}}, {{event_source_column}} from {{ volume_relation }} where DATE({{event_date_column}}) >= {{ d }} AND DATE({{event_date_column}}) <= {{end_date}}
),total_events_query as (
   select count({{event_source_column}}) as total_source_events from {{ volume_relation }} where DATE({{event_date_column}}) = {{ d }}
 ),
 events_dates_combo as (
   select 
    all_events.{{event_name_column}} as event_name, 
    all_events.{{event_source_column}} as source,
    day
    from all_events_query all_events,
    UNNEST(GENERATE_DATE_ARRAY(
        {{dbt_date.n_days_ago(total_days, end_date)}},
        {{end_date}},
        INTERVAL 1 day)) as day
    GROUP BY 
      event_name,
      source,
      day
 ),
 all_event_dates as (
 select 
  combo.event_name event_name,
  combo.source source,
  combo.day as day,
  max(volume.{{event_version_column}}) as version,
  count(volume.{{event_name_column}}) as event_count,
  (
      select total_source_events from total_events_query
  ) as total_source_events
  from events_dates_combo combo
  LEFT JOIN {{volume_relation}} volume
  ON volume.{{event_name_column}} = combo.event_name AND volume.{{event_source_column}} = combo.source AND DATE(volume.{{event_date_column}}) = combo.day
  where combo.day = {{d}}
  GROUP BY
  combo.day,
  combo.event_name,
  combo.source
)

select 
event_name,
 source,
 day,
 SUM(event_count) as event_count,
 SUM(total_source_events) as event_source_count,
 SUM(event_count / total_source_events) * 100 as percentage,
 MAX(version) as version
 from all_event_dates
 GROUP BY
   event_name,
   source,
   day
{% if not loop.first %}
)
{% endif %}
{% if not loop.last %}
    UNION ALL (
{% endif %} 
{% endfor %}
),

avarage as (
select
event_name,
source,
AVG(p.percentage) as avg_percentage,
STDDEV(p.percentage) as std_percentage,
  from 
  (
    select event_name, source, day, percentage from union_query GROUP BY event_name, source, day, percentage
  ) as p
GROUP BY
event_name,
source
),
groupQuery as (
select 
  t.event_name as event_name,  
  t.source as source,
  t.day as day,
  MAX(t.event_count) as event_count,
  MAX(t.event_source_count) as total_source_count,
  MAX(t.version) as version,
  MAX(t.percentage) as percentage,
 MAX(m.avg_percentage) as avg_percentage,
 MAX(m.std_percentage) as std_percentage,
CASE when MAX(t.percentage) > MAX(m.avg_percentage) + MAX(m.std_percentage) * {{threshold}} then 1
when MAX(t.percentage) < MAX(m.avg_percentage) - MAX(m.std_percentage) * {{threshold}} then -1
ELSE 0
end as signal
  from union_query t
  LEFT JOIN avarage m
  ON t.event_name = m.event_name AND t.source = m.source
  group by
  t.event_name,
  t.source, 
  t.day
),

signal_query as (
select 
  event_name,
  source,
  ARRAY_AGG(day order by day ASC) as days,
  ARRAY_AGG(event_count order by day ASC) as event_counts,
  ARRAY_AGG(total_source_count order by day ASC) as total_events_on_source, 
  ARRAY_AGG(percentage order by day ASC) as percentages,
  ARRAY_AGG(signal order by day ASC) as signals,
  from groupQuery
  GROUP BY
    event_name,
    source
)


select * from signal_query


{% endmacro %}