
{% materialization seed, adapter='rockset' %}

  {%- set identifier = model['alias'] -%}
  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set full_refresh_mode = (should_full_refresh()) -%}

  {%- set exists_as_view = (old_relation is not none and old_relation.is_view) -%}
  {%- set exists_as_table = (old_relation is not none and old_relation.is_table) -%}

  {%- set agate_table = load_agate_table() -%}
  {%- do store_result('agate_table', response='OK', agate_table=agate_table) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% if exists_as_view %}
    {{ exceptions.raise_compiler_error("Cannot seed to '{}', it is a view".format(old_relation)) }}
  {% elif exists_as_table and full_refresh_mode %}
    {{ adapter.drop_relation(old_relation) }}
  {% endif %}
  {% set sql = load_csv_rows(model, agate_table) %}

  {% call statement('main') -%}
    {{ "SELECT 1" }}
  {%- endcall %}

  {% set target_relation = this.incorporate(type='table') %}
  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}

{% macro rockset__load_csv_rows(model, agate_table) %}
  {%- set column_override = model['config'].get('column_types', {}) -%}
  {{ adapter.load_dataframe(model['database'], model['schema'], model['alias'],
                agate_table, column_override) }}
{% endmacro %}
