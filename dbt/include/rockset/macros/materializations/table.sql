{% materialization table, adapter='rockset' -%}
  {%- set identifier = model['alias'] -%}
  {%- set target_relation = api.Relation.create(database=database, schema=schema, identifier=identifier, type='table') -%}
  
  {{ run_hooks(pre_hooks) }}
  {{ adapter.create_table(target_relation, sql) }}

  {#-- Rockset does not support CREATE TABLE sql. All logic to create collections happens in adapter.create_table --#}
   {% call statement('main') -%}
     {{ adapter.get_dummy_sql() }}
   {%- endcall %}

  {{ run_hooks(post_hooks) }}

  {% do persist_docs(target_relation, model) %}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
