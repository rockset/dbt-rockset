{% materialization view, adapter='rockset' -%}  
   {% set target_relation = this.incorporate(type='view') %}
   {{ adapter.create_view(target_relation, sql) }}
{%- endmaterialization %}
