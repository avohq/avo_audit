# Algorithm used in this macro is inspired by Stack overflow answer called 474 Robust peak detection algorithm (using z-scores)
# Currently it is a simplified version of it and not as good, but the goal is to implement the algorithm as described in the stack overflow answer.
#
# Credit for algorithm: 
# 
# Brakel, J.P.G. van (2014). "Robust peak detection algorithm using z-scores". Stack Overflow. 
# Available at: https://stackoverflow.com/questions/22583391/peak-signal-detection-in-realtime-timeseries-data/22640362#22640362 (version: 2020-11-08).
#
# 

{% macro test_detect_event_anomaly(model, event_name_column, event_date_column, event_source_column, end_date=avo_audit.date_yesterday(),  n_days=15, threshold=2.5, minimum_avg_event_volume=0) %}

{% set dt = "cast('" +  end_date +"' as date)" %}

with generate_dates as (
  {{ avo_audit.generate_dates_table(dt,  n_days) }}
),
all_events_query as (
        -- Find all event/source combos for the date range given to ensure there will be no nulls for each day.

        select {{event_name_column}}, {{event_source_column}} 
        from {{ model }} 
        where DATE({{event_date_column}}) >= {{ dbt_date.n_days_ago(  n_days, dt) }} 
          and DATE({{event_date_column}}) <= {{dt}}
), 
events_dates_combo as (
        -- create a event/source/day combo for all all event/source from all_events_query
        -- So all combos exist on each day in the following queries.

        select 
          e.{{event_name_column}} as event_name, 
          e.{{event_source_column}} as source,
          g.day as day
          from all_events_query as e cross join generate_dates as g
         group by
          event_name,
          source,
          day
 
)
, union_query as (
  -- Big Union query that runs ties queries together for each day in the time period selected
  -- And unions the days together

  {% for i in range(  n_days) %}

    {%- set d = dbt_date.n_days_ago(i, dt) -%}

      with total_events_query_{{i}} as (
          -- Count all events for each source on each day.

          select count({{event_source_column}}) as total_source_events 
          from {{ model }} 
          where DATE({{event_date_column}}) = {{ d }}
    
     ), all_event_dates_{{i}} as (
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
            select total_source_events from total_events_query_{{i}}
          ) as event_source_count
        from events_dates_combo combo
        left join {{model}} volume
        on volume.{{event_name_column}} = combo.event_name 
          and volume.{{event_source_column}} = combo.source 
          and DATE(volume.{{event_date_column}}) = combo.day
        where combo.day = {{d}}
        and (select total_source_events from total_events_query_{{i}}) > 0
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
      from all_event_dates_{{i}}
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

daily_percentage as (
  select event_name, source, day, event_count, percentage 
  from union_query 
  GROUP BY event_name, source, day, event_count, percentage
), avarage as (  
  -- Get the Avarage and standard deviation of percentages over the time period for all event_name source combinations.
  -- This is to be able to check each percentage whether its out of its normal bounds.

  select
    event_name,
    source,
    AVG(percentage) as avg_percentage,
    STDDEV(percentage) as std_percentage,
    AVG(event_count) as avg_event_count
    from daily_percentage
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
    case 
      when MAX(m.avg_event_count) > {{minimum_avg_event_volume}} then -- anomaly detection does not work for very low volume data.
        case
          when MAX(t.percentage) > MAX(m.avg_percentage) + MAX(m.std_percentage) * {{threshold}} then 1
          when MAX(t.percentage) < MAX(m.avg_percentage) - MAX(m.std_percentage) * {{threshold}} then -1
          else 0
        end
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
    {{ avo_audit.array_agg_ordered('day', 'day', 'asc', 'days') }},
    {{ avo_audit.array_agg_ordered('event_count', 'day', 'asc', 'event_counts') }},
    {{ avo_audit.array_agg_ordered('total_source_count', 'day', 'asc', 'total_events_on_source') }},
    {{ avo_audit.array_agg_ordered('percentage', 'day', 'asc', 'percentages') }}, 
    {{ avo_audit.array_agg_ordered('signal', 'day', 'asc', 'signals') }}  
  from calculate_signal
  group by
    event_name,
    source
)
{{avo_audit.find_signals('aggregate_by_day_asc')}}


{% endmacro %}