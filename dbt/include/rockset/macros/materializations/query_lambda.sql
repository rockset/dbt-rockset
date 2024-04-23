{% materialization query_lambda, adapter='rockset' -%}  
   {% set target_relation = this.incorporate(type='view') %}
   {% set tags = config.get('tags', default=[]) %}
   {% set parameters = config.get('parameters',default=[]) %}
   {{ adapter.create_or_update_query_lambda(target_relation, sql, tags, parameters) }}

   {{ run_hooks(pre_hooks) }}

   {#-- All logic to create Query Lambdas happens in the adapter --#}
   {% call statement('main') -%}
      {{ adapter.get_dummy_sql() }}
   {%- endcall %}

   {{ run_hooks(post_hooks) }}

   {% do persist_docs(target_relation, model) %}

   {{ return({'relations':[target_relation]}) }}
{%- endmaterialization %}