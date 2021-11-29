

{{ config(materialized='table', sort='timestamp', dist='event') }}
{% set end_date_script = modules.datetime.date(2021, 11, 03) %}
{% set today = modules.datetime.date.today() %}
{% set delta = today - end_date_script %}

{% set end_date = dbt_date.n_days_ago(delta.days) %}
{%- set days_back = 10 -%}
{%- set days_lag = 5 -%}
{%- set event_name_column = 'event' -%}
{%- set event_date_column = 'sent_at' -%}
{%- set event_source_column = 'client' -%}


{{
  audit_event_volume(ref('avo_audit_experiment_data'), end_date, days_back, days_lag, event_name_column, event_date_column, event_source_column)
}}