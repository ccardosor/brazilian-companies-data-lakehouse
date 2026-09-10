{{ config(materialized='view') }}

select
    estabelecimentos.ano_mes || '-' || estabelecimentos.cnpj_completo as chave_estabelecimento,
    estabelecimentos.ano_mes,
    estabelecimentos.cnpj_basico,
    estabelecimentos.cnpj_ordem,
    estabelecimentos.cnpj_dv,
    estabelecimentos.cnpj_completo,
    estabelecimentos.identificador_matriz_filial,
    case estabelecimentos.identificador_matriz_filial
        when '1' then 'Matriz'
        when '2' then 'Filial'
    end as descricao_matriz_filial,
    estabelecimentos.nome_fantasia,
    estabelecimentos.situacao_cadastral,
    case estabelecimentos.situacao_cadastral
        when '01' then 'Nula'
        when '02' then 'Ativa'
        when '03' then 'Suspensa'
        when '04' then 'Inapta'
        when '08' then 'Baixada'
    end as descricao_situacao_cadastral,
    estabelecimentos.data_situacao_cadastral,
    estabelecimentos.motivo_situacao_cadastral,
    motivos.descricao_motivo,
    estabelecimentos.data_inicio_atividade,
    estabelecimentos.cnae_fiscal_principal,
    cnaes.descricao_cnae as descricao_cnae_principal,
    estabelecimentos.cnae_fiscal_secundaria,
    estabelecimentos.uf,
    estabelecimentos.municipio,
    municipios.descricao_municipio,
    estabelecimentos.pais,
    paises.descricao_pais,
    estabelecimentos.nome_cidade_exterior,
    estabelecimentos.tipo_logradouro,
    estabelecimentos.logradouro,
    estabelecimentos.numero,
    estabelecimentos.complemento,
    estabelecimentos.bairro,
    estabelecimentos.cep,
    estabelecimentos.correio_eletronico,
    estabelecimentos.situacao_especial,
    estabelecimentos.data_situacao_especial,
    empresas.chave_empresa,
    empresas.razao_social,
    empresas.natureza_juridica,
    empresas.descricao_natureza_juridica,
    empresas.porte_empresa,
    empresas.descricao_porte_empresa,
    estabelecimentos.arquivo_origem,
    estabelecimentos.data_processamento_utc
from {{ ref('stg_cnpj_estabelecimentos') }} as estabelecimentos
left join {{ ref('dim_empresas') }} as empresas
    on estabelecimentos.ano_mes = empresas.ano_mes
    and estabelecimentos.cnpj_basico = empresas.cnpj_basico
left join {{ ref('stg_cnpj_motivos') }} as motivos
    on estabelecimentos.motivo_situacao_cadastral = motivos.codigo_motivo
left join {{ ref('stg_cnpj_cnaes') }} as cnaes
    on estabelecimentos.cnae_fiscal_principal = cnaes.codigo_cnae
left join {{ ref('stg_cnpj_municipios') }} as municipios
    on estabelecimentos.municipio = municipios.codigo_municipio
left join {{ ref('stg_cnpj_paises') }} as paises
    on estabelecimentos.pais = paises.codigo_pais
