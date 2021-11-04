

{{ config(materialized='table', sort='timestamp', dist='event') }}
{%- set endDate = dbt_date.n_days_ago(1) -%}
{%- set days_back = 10 -%}
{%- set days_lag = 5 -%}
{%- set event_name_column = 'event' -%}
{%- set event_date_column = 'sent_at' -%}
{%- set event_source_column = 'client' -%}


{{
  new_audit_event_volume(ref('avo_audit_experiment_data'), endDate, days_back, days_lag, event_name_column, event_date_column, event_source_column)
}}