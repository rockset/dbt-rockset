{#-- Rockset has not yet implemented views --#}
{% materialization view, adapter='rockset' -%}
  
   {{ adapter.create_view(relation, sql) }}
{%- endmaterialization %}
