{% macro date_yesterday() %}

{% set today = modules.datetime.date.today() %}
{% set yesterday = (today - modules.datetime.timedelta(1)).isoformat() %}


{{return(yesterday)}}

{% endmacro %}