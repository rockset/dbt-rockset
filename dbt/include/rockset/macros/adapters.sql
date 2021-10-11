-- Rockset does not have a notion of database, so do not include it when resolving refs or sources
-- It resolves as schema.identifier (i.e. workspace.collection), instead of database.schema.identifier
{% macro ref(modelname) %}
  {{ builtins.ref(modelname).include(database=False).render() }}
{% endmacro %}

{% macro source(source_name, table_name) %}
  {{ builtins.source(source_name, table_name).include(database=False).render() }}
{% endmacro %}
