select
    estabelecimentos.ano_mes || '-'
        || cast(cast(date_trunc('month', estabelecimentos.data_inicio_atividade) as date) as varchar)
        || '-' || coalesce(estabelecimentos.uf, 'sem_uf')
        || '-' || coalesce(estabelecimentos.municipio, 'sem_municipio')
        as chave_aberturas_estabelecimentos_por_local,
    estabelecimentos.ano_mes as ano_mes_referencia,
    cast(date_trunc('month', estabelecimentos.data_inicio_atividade) as date) as mes_inicio_atividade,
    estabelecimentos.uf,
    estabelecimentos.municipio,
    municipios.descricao_municipio,
    count(*) as quantidade_estabelecimentos_iniciados,
    count(distinct estabelecimentos.cnpj_basico) as quantidade_empresas_distintas_com_inicio,
    sum(case when estabelecimentos.identificador_matriz_filial = '1' then 1 else 0 end)
        as quantidade_matrizes_iniciadas,
    sum(case when estabelecimentos.identificador_matriz_filial = '2' then 1 else 0 end)
        as quantidade_filiais_iniciadas
from {{ ref('stg_cnpj_estabelecimentos') }} as estabelecimentos
left join {{ ref('stg_cnpj_municipios') }} as municipios
    on estabelecimentos.municipio = municipios.codigo_municipio
group by
    estabelecimentos.ano_mes,
    cast(date_trunc('month', estabelecimentos.data_inicio_atividade) as date),
    estabelecimentos.uf,
    estabelecimentos.municipio,
    municipios.descricao_municipio
