{% macro new_audit_event_volume(volume_relation, end_date, days_back, days_lag) %}

{%- set total_days = days_back + days_lag -%}

{% for i in range(total_days) %}

{%- set d = dbt_date.n_days_ago(i, end_date) -%}

with all_events_query as (
  select event_name, source from {{ volume_relation }} where DATE(ts) >= {{ d }} AND DATE(ts) <= {{end_date}}
),total_events_query as (
   select count(source) as total_source_events from {{ volume_relation }} where DATE(ts) = {{ d }}
 ),
 events_dates_combo as (
   select 
    all_events.event_name as event_name, 
    all_events.source as source,
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
  max(volume.version) as version,
  count(volume.event_name) as event_count,
  (
      select total_source_events from total_events_query
  ) as total_source_events
  from events_dates_combo combo
  LEFT JOIN {{volume_relation}} volume
  ON volume.event_name = combo.event_name AND volume.source = combo.source AND DATE(volume.ts) = combo.day
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
{% endmacro %}