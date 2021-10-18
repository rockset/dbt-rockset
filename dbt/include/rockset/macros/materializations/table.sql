{% materialization table, adapter='rockset' -%}
  {%- set identifier = model['alias'] -%}
  {%- set target_relation = api.Relation.create(database=None, schema=schema, identifier=identifier, type='table') -%}
  
  {{ run_hooks(pre_hooks) }}
  {{ adapter.create_table(target_relation, sql) }}

  {#-- Rockset does not support CREATE TABLE sql. All logic to create collections happens in create_table_as --#}
  {% call statement('main') -%}
    {{ "SELECT 1" }}
  {%- endcall %}

  {{ run_hooks(post_hooks) }}

  {% do persist_docs(target_relation, model) %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
