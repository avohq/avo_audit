{% macro generate_dates_table(end_date, total_days) %}
  {{ return(adapter.dispatch('generate_dates_table', 'avo_audit')(end_date, total_days)) }}
{% endmacro %}


{% macro default__generate_dates_table(end_date, total_days) %}

     select day from UNNEST(GENERATE_DATE_ARRAY(
            {{dbt_date.n_days_ago(total_days, end_date)}},
            {{end_date}},
            INTERVAL 1 day)) as day

{% endmacro %}


{% macro bigquery__generate_dates_table(end_date, total_days) %}

  select day from UNNEST(GENERATE_DATE_ARRAY(
            {{dbt_date.n_days_ago(total_days, end_date)}},
            {{end_date}},
            INTERVAL 1 day)) as day

{% endmacro %}

{% macro snowflake__generate_dates_table(end_date, total_days) %}
     select
  dateadd(
    day,
    '-' || row_number() over (order by null),
    dateadd(day, '+1', {{end_date}})
  ) as day
from table (generator(rowcount => {{total_days}}))

{% endmacro %}
