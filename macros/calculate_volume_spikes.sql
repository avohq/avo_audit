{% macro calculate_volume_spikes(table_ref, sensitivity_percentage) %}

{% set threshold = 3.5 %}

with avarage as (
select
event_name,
source,
AVG(p.percentage) as avg_percentage,
STDDEV(p.percentage) as std_percentage,
  from 
  (
    select event_name, source, day, percentage from {{table_ref}} GROUP BY event_name, source, day, percentage
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
  from {{table_ref}} t
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
 WHERE
    (select signal FROM UNNEST(signals) AS signal where signal = 1 GROUP BY signal) = 1
    OR (select signal from UNNEST(signals) AS signal where signal = -1 GROUP BY signal) = -1
  


{% endmacro %}