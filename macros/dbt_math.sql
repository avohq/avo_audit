{% macro mean(arr, len) %}



{% set arr_size = arr|length %}
{{log("ARR SIZE", true)}}
{{log(arr_size, true)}}
{{log("len", true)}}
{{log(len, true)}}
{% set var = 0 %}

{% set l = 0 %}

{% if len < arr|length %}
  {% set l = len %}
{% else %}
  {% set l = arr|length %}
{% endif %}

{{log(arr, true)}}
{{log(l, true)}}

{% set ns = namespace(var=0.0) %}     
{% for i in range(l) %}
  {% set ns.var = arr[i]|float + ns.var %}
  {{log(ns.var, true)}}
{% endfor %}
{{log("TOTAL", true)}}
{{log(ns.var, true)}}

{{return(var / l)}}
{% endmacro %}