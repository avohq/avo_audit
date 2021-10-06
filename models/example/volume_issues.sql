

{{ config(materialized='table', sort='timestamp', dist='event') }}
{{
  audit_event_volume(ref('all_events_table'))
}}