{% macro rockset__get_catalog(information_schema, schemas) -%}
 {{adapter.get_catalog(information_schema, schemas)}}
{% endmacro %}
