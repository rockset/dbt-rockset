{% materialization view, adapter='rockset' -%}
   {#-- This just throws unimplemented for now --#}
   {{ adapter.create_view(relation, sql) }}

{%- endmaterialization %}
