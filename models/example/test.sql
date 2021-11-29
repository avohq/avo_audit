{% set end_date_script = modules.datetime.date(2021, 11, 03) %}
{% set today = modules.datetime.date.today() %}
{% set delta = today - end_date_script %}

{% set end_date = dbt_date.n_days_ago(delta.days) %}

{% set total_days = 15 %}
{% set volume_relation = ref('avo_audit_experiment_data') %}
{% set d = dbt_date.n_days_ago(1, end_date) %}


with generate_dates as (
  {{ avo_audit.generate_dates_table(end_date, total_days) }}
),  
all_events_query as (
        -- Find all event/source combos for the date range given to ensure there will be no nulls for each day.

        select event, client 
        from {{ volume_relation }} 
        where DATE(sent_at) >= {{ dbt_date.n_days_ago(total_days, end_date) }} 
          and DATE(sent_at) <= {{end_date}}
), 
events_dates_combo as (
        -- create a event/source/day combo for all all event/source from all_events_query
        -- So all combos exist on each day in the following queries.

        select 
          e.event as event_name, 
          e.client as source,
          g.day as day
          from all_events_query as e cross join generate_dates as g
         group by
          event_name,
          source,
          day
 
),
total_events_query as (
          -- Count all events for each source on each day.

          select count(event) as total_source_events 
          from {{ volume_relation }} 
          where DATE(sent_at) = {{ d }}
    
     ),
all_event_dates as (
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
          count(volume.event) as event_count,
          (
            select total_source_events from total_events_query
          ) as event_source_count
        from events_dates_combo combo
        left join {{volume_relation}} volume
        on volume.event = combo.event_name 
          and volume.client = combo.source 
          and DATE(volume.sent_at) = combo.day
        where combo.day = {{d}}
        and (select total_source_events from total_events_query) > 0
        group by
          combo.day,
          combo.event_name,
          combo.source
      )

select * from all_event_dates