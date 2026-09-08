{% macro raw_cnpj(dataset) -%}
read_parquet(
    '{{ var("cnpj_raw_path", "downloads/lakehouse/raw/cnpj") }}/{{ dataset }}/ano_mes=*/*.parquet',
    hive_partitioning = true,
    union_by_name = true
)
{%- endmacro %}

{% macro data_receita(coluna) -%}
try_strptime({{ coluna }}, '%Y%m%d')::date
{%- endmacro %}

{% macro decimal_receita(coluna) -%}
try_cast(replace({{ coluna }}, ',', '.') as decimal(18, 2))
{%- endmacro %}
