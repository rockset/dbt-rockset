{% materialization view, adapter='rockset' -%}  
   {% set target_relation = this.incorporate(type='view') %}
   {{ adapter.create_view(target_relation, sql) }}

   {#-- Rockset does not support CREATE VIEW sql. All logic to create views happens in create_view --#}
   {% call statement('main') -%}
      {{ adapter.get_dummy_sql() }}
   {%- endcall %}

   {{ run_hooks(post_hooks) }}

   {% do persist_docs(target_relation, model) %}

   {{ return({'relations': [target_relation]}) }}
{%- endmaterialization %}

{% macro rockset__create_view_as(relation, sql) -%}  
   {{ adapter.create_view(relation, sql) }}

   {#-- Rockset does not support CREATE VIEW sql. All logic to create views happens in create_view --#}
  {% call statement('main') -%}
     {{ adapter.get_dummy_sql() }}
  {%- endcall %}

   {{ run_hooks(post_hooks) }}

   {% do persist_docs(relation, model) %}

   {{ return(adapter.get_dummy_sql()) }}
{%- endmacro %}

