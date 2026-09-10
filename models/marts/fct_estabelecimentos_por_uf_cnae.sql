select
    estabelecimentos.ano_mes || '-' || coalesce(estabelecimentos.uf, 'sem_uf')
        || '-' || coalesce(estabelecimentos.municipio, 'sem_municipio')
        || '-' || coalesce(estabelecimentos.cnae_fiscal_principal, 'sem_cnae')
        as chave_estabelecimentos_por_uf_cnae,
    estabelecimentos.ano_mes,
    estabelecimentos.uf,
    estabelecimentos.municipio,
    municipios.descricao_municipio,
    estabelecimentos.cnae_fiscal_principal,
    cnaes.descricao_cnae as descricao_cnae_principal,
    count(*) as quantidade_estabelecimentos,
    count(distinct estabelecimentos.cnpj_basico) as quantidade_empresas,
    sum(case when estabelecimentos.situacao_cadastral = '02' then 1 else 0 end)
        as quantidade_estabelecimentos_ativos,
    sum(case when estabelecimentos.identificador_matriz_filial = '1' then 1 else 0 end)
        as quantidade_matrizes,
    sum(case when estabelecimentos.identificador_matriz_filial = '2' then 1 else 0 end)
        as quantidade_filiais
from {{ ref('stg_cnpj_estabelecimentos') }} as estabelecimentos
left join {{ ref('stg_cnpj_municipios') }} as municipios
    on estabelecimentos.municipio = municipios.codigo_municipio
left join {{ ref('stg_cnpj_cnaes') }} as cnaes
    on estabelecimentos.cnae_fiscal_principal = cnaes.codigo_cnae
group by
    estabelecimentos.ano_mes,
    estabelecimentos.uf,
    estabelecimentos.municipio,
    municipios.descricao_municipio,
    estabelecimentos.cnae_fiscal_principal,
    cnaes.descricao_cnae
