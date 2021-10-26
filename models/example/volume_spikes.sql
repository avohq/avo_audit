
{{ config(materialized='table', sort='timestamp', dist='event') }}
{{
  calculate_volume_spikes(ref('volume_issues'), 10)
}}