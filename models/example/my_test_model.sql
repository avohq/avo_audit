


{% set dbt_relation=adapter.get_relation(
      database=target.database,
      schema="avo_analytics_playground",
      identifier="branch_created"
) -%}



{{
    parse_relation(dbt_relation)
}}