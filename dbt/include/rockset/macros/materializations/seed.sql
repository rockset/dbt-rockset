
{% materialization seed, adapter='rockset' %}

  {%- set identifier = model['alias'] -%}
  {%- set old_relation = adapter.get_relation(database=database, schema=schema, identifier=identifier) -%}
  {%- set full_refresh_mode = (should_full_refresh()) -%}

  {%- set exists_as_table = (old_relation is not none and old_relation.is_table) -%}
  {%- set exists_as_view = (old_relation is not none and old_relation.is_view) -%}

  {%- set agate_table = load_agate_table() -%}
  {%- do store_result('agate_table', response='OK', agate_table=agate_table) -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {% set create_table_sql = "" %}
  {% if exists_as_view %}
    {{ exceptions.raise_compiler_error("Cannot seed to '{}', it is a view".format(old_relation)) }}
  {% elif exists_as_table %}
    {% set create_table_sql = reset_csv_table(model, full_refresh_mode, old_relation, agate_table) %}
  {% else %}
    {% set create_table_sql = create_csv_table(model, agate_table) %}
  {% endif %}
  {% set sql = load_csv_rows(model, agate_table) %}

  {#-- Rockset does not support CREATE TABLE sql. All logic to create collections happens in create_table_as --#}
  {% call statement('main') -%}
    {{ "SELECT 1" }}
  {%- endcall %}

  {% set target_relation = this.incorporate(type='table') %}
  {% do persist_docs(target_relation, model) %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}
  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}

{% macro rockset__create_csv_table(model, agate_table) %}
    -- no-op
{% endmacro %}

{% macro rockset__reset_csv_table(model, full_refresh, old_relation, agate_table) %}
    {{ adapter.drop_relation(old_relation) }}
{% endmacro %}

{% macro rockset__load_csv_rows(model, agate_table) %}
  {%- set column_override = model['config'].get('column_types', {}) -%}
  {{ adapter.load_dataframe(model['database'], model['schema'], model['alias'],
                agate_table, column_override) }}
{% endmacro %}
