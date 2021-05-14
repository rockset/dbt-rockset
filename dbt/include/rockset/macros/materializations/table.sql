{% macro rockset__create_table_as(temporary, relation, sql) -%}
  {{ adapter.create_table(relation, sql) }}
{% endmacro %}

{% materialization table, adapter='rockset' -%}
  {%- set identifier = model['alias'] -%}
  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set already_exists = (old_relation is not none) -%}
  {%- set target_relation = api.Relation.create(database=database, schema=schema, identifier=identifier, type='table') -%}
  
  {{ run_hooks(pre_hooks) }}

  {%- if already_exists -%}
      {{ adapter.drop_relation(old_relation) }}
  {%- endif -%}

  {% call statement('main') -%}
    {{ create_table_as(False, target_relation, sql) }}
  {%- endcall %}

  {{ run_hooks(post_hooks) }}

  {% do persist_docs(target_relation, model) %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}