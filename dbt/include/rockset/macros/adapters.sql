{% macro rockset__alter_column_type(relation, column_name, new_column_type) -%}
  {% call statement('alter_column_type') %}
    INSERT INTO {{ relation }} ( "_id", {{ column_name }} )
    SELECT
      "_id",
      CAST( {{ column_name }} as {{ new_column_type }} ) as {{ column_name }}
    FROM {{ relation }}
  {%- endcall %}
{% endmacro %}

-- Rockset does not have a notion of database, so do not include it when resolving refs from other models
-- It resolves as schema.identifier (i.e. workspace.collection), instead of database.schema.identifier
{% macro ref(modelname) %}{{ builtins.ref(modelname).include(database=False).render() }}{% endmacro %}
