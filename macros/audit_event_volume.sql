# Algorithm used in this macro is inspired by Stack overflow answer called 474 Robust peak detection algorithm (using z-scores)
# Currently it is a simplified version of it and not as good, but the goal is to implement the algorithm as described in the stack overflow answer.
#
# Credit for algorithm: 
# 
# Brakel, J.P.G. van (2014). "Robust peak detection algorithm using z-scores". Stack Overflow. 
# Available at: https://stackoverflow.com/questions/22583391/peak-signal-detection-in-realtime-timeseries-data/22640362#22640362 (version: 2020-11-08).
#
# 

{% macro new_audit_event_volume(volume_relation, end_date, days_back, days_lag, event_name_column, event_date_column, event_source_column) %}

{%- set total_days = days_back + days_lag -%}
{% set threshold = 2.5 %}

with union_query as (
  -- Big Union query that runs ties queries together for each day in the time period selected
  -- And unions the days together

  {% for i in range(total_days) %}

    {%- set d = dbt_date.n_days_ago(i, end_date) -%}

      with all_events_query as (
        -- Find all event/source combos for the date range given to ensure there will be no nulls for each day.

        select {{event_name_column}}, {{event_source_column}} 
        from {{ volume_relation }} 
        where DATE({{event_date_column}}) >= {{ d }} 
          and DATE({{event_date_column}}) <= {{end_date}}

      ), total_events_query as (
          -- Count all events for each source on each day.

          select count({{event_source_column}}) as total_source_events 
          from {{ volume_relation }} 
          where DATE({{event_date_column}}) = {{ d }}
    
     ), events_dates_combo as (
        -- create a event/source/day combo for all all event/source from all_events_query
        -- So all combos exist on each day in the following queries.

        select 
          all_events.{{event_name_column}} as event_name, 
          all_events.{{event_source_column}} as source,
          day
        from all_events_query all_events,
          UNNEST(GENERATE_DATE_ARRAY(
            {{dbt_date.n_days_ago(total_days, end_date)}},
            {{end_date}},
            INTERVAL 1 day)) as day
        group by
          event_name,
          source,
          day
 
      ), all_event_dates as (
        -- Makes event_name/source/day combo for all event_name/source combos in 'all_events_query'
        -- Counts the number of event_name in the combo
        -- adds total source events for that day with as 'total_source_events'
        -- Example output:
        -- event_name  | source     | day          | event_count | total_source events
        -- "event_1"  | "source1"  | "2021-10-25" |     238     |  16432
        -- "event_2"  | "source1" | "2021-10-25" |     1023    |  16432

        select 
          combo.event_name event_name,
          combo.source source,
          combo.day as day,
          count(volume.{{event_name_column}}) as event_count,
          (
            select total_source_events from total_events_query
          ) as event_source_count
        from events_dates_combo combo
        left join {{volume_relation}} volume
        on volume.{{event_name_column}} = combo.event_name 
          and volume.{{event_source_column}} = combo.source 
          and DATE(volume.{{event_date_column}}) = combo.day
        where combo.day = {{d}}
        and (select total_source_events from total_events_query) > 0
        group by
          combo.day,
          combo.event_name,
          combo.source
      )

      -- Select the each combo from 'all_event_dates' query, and add the percentage column which is 
      -- total amount of event_name for this source divided by total events on the source. multiplied by 100(for actual percentages)
      select 
        event_name,
        source,
        day,
        event_count,
        event_source_count,
        ABS(event_count / event_source_count) * 100 as percentage
      from all_event_dates
      group by
        event_name,
        source,
        day,
        event_count,
        event_source_count
      {% if not loop.first %}
        )
      {% endif %}
      {% if not loop.last %}
        union all (
      {% endif %} 
  {% endfor %}

), 

avarage as (  
  -- Get the Avarage and standard deviation of percentages over the time period for all event_name source combinations.
  -- This is to be able to check each percentage whether its out of its normal bounds.

  select
    event_name,
    source,
    AVG(p.percentage) as avg_percentage,
    STDDEV(p.percentage) as std_percentage,
    from 
    (
      select event_name, source, day, percentage from union_query GROUP BY event_name, source, day, percentage
    ) as p
  group by
    event_name,
    source

), calculate_signal as (
  -- Calculcates the signal based on the percentage of events for each day against 
  -- the avarage and standard deviation. 
  --
  -- Signal:   
  --   - When signal is 1 there is a spike in amount of events for this event_name/source combination
  --   - When signal is -1 there is a drop in amount of events for this event_name/source combination
  --   - When signal is 0 there the amount of events is within it's normal range.
  -- 
  -- Based on the algorithm referenced at the top of this file.

  select 
    t.event_name as event_name,  
    t.source as source,
    t.day as day,
    MAX(t.event_count) as event_count,
    MAX(t.event_source_count) as total_source_count,
    MAX(t.percentage) as percentage,
    MAX(m.avg_percentage) as avg_percentage,
    MAX(m.std_percentage) as std_percentage,
    case when MAX(t.percentage) > MAX(m.avg_percentage) + MAX(m.std_percentage) * {{threshold}} then 1
    when MAX(t.percentage) < MAX(m.avg_percentage) - MAX(m.std_percentage) * {{threshold}} then -1
    else 0
    end as signal
  from union_query t
  left join avarage m
  on t.event_name = m.event_name 
    and t.source = m.source
  group by
    t.event_name,
    t.source, 
    t.day

), aggregate_by_day_asc as (
  -- Aggregates together all counters and days for  event_name/source combinations and orders it by day ascending.

  select 
    event_name,
    source,
    MAX(avg_percentage) as avg_percentage,
    MAX(std_percentage) as std_percentage,
    ARRAY_AGG(day order by day ASC) as days,
    ARRAY_AGG(event_count order by day ASC) as event_counts,
    ARRAY_AGG(total_source_count order by day ASC) as total_events_on_source, 
    ARRAY_AGG(percentage order by day ASC) as percentages,
    ARRAY_AGG(signal order by day ASC) as signals,
  from calculate_signal
  group by
    event_name,
    source
)


-- Only return event_name/source combinations that have spiked or dropped for at least 1 day.
-- Disregard all combinations that were iniside the norm for the whole time period to reduce noise.
select 
  * 
from aggregate_by_day_asc
where
    (select signal FROM UNNEST(signals) AS signal where signal = 1 GROUP BY signal) = 1
    OR (select signal from UNNEST(signals) AS signal where signal = -1 GROUP BY signal) = -1


{% endmacro %}