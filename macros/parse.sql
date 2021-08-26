{% macro parse_relation(relation) %}

select * from {{ relation }}  WHERE dateColumn > {{ dbt_date.n_days_ago(1) }}

{% endmacro %}