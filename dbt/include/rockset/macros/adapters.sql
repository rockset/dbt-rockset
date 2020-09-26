{% macro rockset__alter_column_type(relation, column_name, new_column_type) -%}
  {% call statement('alter_column_type') %}
    INSERT INTO {{ relation }} ( "_id", {{ column_name }} )
    SELECT
      "_id",
      CAST( {{ column_name }} as {{ new_column_type }} ) as {{ column_name }}
    FROM {{ relation }}
  {%- endcall %}
{% endmacro %}
