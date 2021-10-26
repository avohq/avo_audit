{% macro calculate_volume_spikes(table_ref, sensitivity_percentage) %}


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
x
groupQuery as (
select 
  t.event_name as event_name,  
  t.source as source,
  ARRAY_AGG(DISTINCT t.day 
            ORDER BY t.day ASC) as days,
  ARRAY_AGG(t.event_count
            ORDER BY t.day ASC) as event_counts,
  ARRAY_AGG(t.event_source_count
            ORDER BY t.day ASC) as source_counts,
  ARRAY_AGG(
    t.percentage
    ORDER BY t.day ASC) as percentages,
 MAX(m.avg_percentage) as avg_percentage,
 MAX(m.std_percentage) as std_percentage
  from {{table_ref}} t
  LEFT JOIN avarage m
  ON t.event_name = m.event_name AND t.source = m.source
  group by
  t.event_name,
  t.source
)

select 
 *
  from groupQuery


{% endmacro %}