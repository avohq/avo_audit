{%- set n_days = 15 -%}
{%- set event_name_column = 'event' -%}
{%- set event_date_column = 'sent_at' -%}
{%- set event_source_column = 'client' -%}

{{
  avo_audit.avo_audit_detect_event_count_anomoly(ref('avo_audit_significant_drop'), event_name_column, event_date_column, event_source_column, "2021-11-04", n_days)
}}