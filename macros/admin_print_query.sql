{% macro admin_print_query(sql) %}
  {%- set res = run_query(sql) -%}

  {# Print column headers #}
  {%- if res is none -%}
    {{ log("No results (res is none).", info=True) }}
    {{ return("OK") }}
  {%- endif -%}

  {{ log("---- QUERY ----", info=True) }}
  {{ log(sql, info=True) }}
  {{ log("---- RESULTS ----", info=True) }}

  {%- for row in res.rows -%}
    {{ log(row, info=True) }}
  {%- endfor -%}

  {{ return("OK") }}
{% endmacro %}
