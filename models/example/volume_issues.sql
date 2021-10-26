

{{ config(materialized='table', sort='timestamp', dist='event') }}
{%- set endDate = dbt_date.n_days_ago(1) -%}
{{
  new_audit_event_volume(ref('all_events_table'), endDate, 10, 5)
}}