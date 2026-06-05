-- This macro overrides dbt's default schema naming behaviour.
-- By default dbt appends custom schema to target schema: gold_gold
-- This macro tells dbt: if a custom schema is set, use it directly.

{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}