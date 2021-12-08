{%- set n_days = 10 -%}
{%- set event_name_column = 'event' -%}
{%- set event_date_column = 'sent_at' -%}
{%- set event_source_column = 'client' -%}

{{
  avo_audit.test_detect_event_anomaly(ref('avo_audit_normal_data'), event_name_column, event_date_column, event_source_column, "2021-12-01", n_days, minimum_avg_event_volume=100)
}}