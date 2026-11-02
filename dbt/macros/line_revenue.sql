{#- Net line revenue after the line discount. Single definition used by every model. -#}
{% macro line_revenue(quantity, unit_price, discount_pct) -%}
    round(({{ quantity }} * {{ unit_price }} * (1 - {{ discount_pct }} / 100.0))::numeric, 2)
{%- endmacro %}
