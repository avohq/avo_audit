

{{ config(materialized='table', sort='timestamp', dist='event') }}

{{
    join_schema_into_table('avo_analytics_playground', 'event', 'version', 'sent_at', 'client')
}}