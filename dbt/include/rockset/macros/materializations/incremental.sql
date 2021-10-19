{% materialization incremental, adapter='rockset' -%}
  {%- set unique_key = config.get('unique_key') -%}
  {%- set full_refresh_mode = (should_full_refresh()) -%}
  {%- set target_relation = this %}
  {%- set existing_relation = adapter.get_collection(target_relation) %}

  {{ run_hooks(pre_hooks) }}

  {% if existing_relation is none %}
    {{ adapter.create_table(target_relation, sql) }}
  {% elif full_refresh_mode %}
    {{ adapter.drop_relation(existing_relation) }}
    {{ adapter.create_table(target_relation, sql) }}
  {% else %}
    {{ adapter.add_incremental_docs(target_relation, sql, unique_key) }}
  {% endif %}

  {#-- Rockset does not support CREATE TABLE sql. All logic to create / add docs to collections happens above --#}
  {%- call statement('main') -%}
    {{ adapter.get_dummy_sql() }}
  {% endcall %}

  {{ run_hooks(post_hooks) }}

  {% set target_relation = this.incorporate(type='table') %}
  {% do persist_docs(target_relation, model) %}
  {{ return({'relations': [target_relation]}) }}

{%- endmaterialization %}