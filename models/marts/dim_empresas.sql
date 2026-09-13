with dados_ordenados as (
    select
        *,
        count(*) over (
            partition by ano_mes, cnpj_basico
        ) as linhas_origem_mesma_chave,
        row_number() over (
            partition by ano_mes, cnpj_basico
            order by
                case when razao_social is not null then 0 else 1 end,
                case when natureza_juridica is not null and natureza_juridica != '0000' then 0 else 1 end,
                case when qualificacao_responsavel is not null and qualificacao_responsavel != '00' then 0 else 1 end,
                case when porte_empresa is not null then 0 else 1 end,
                data_processamento_utc desc,
                arquivo_origem
                    ) as rn
    from {{ ref('stg_cnpj_empresas') }}
),

empresas as (
    select
        *
    from dados_ordenados
    where rn = 1
)

select
    empresas.ano_mes || '-' || empresas.cnpj_basico as chave_empresa,
    empresas.ano_mes,
    empresas.cnpj_basico,
    empresas.razao_social,
    empresas.natureza_juridica,
    naturezas.descricao_natureza_juridica,
    empresas.qualificacao_responsavel,
    qualificacoes.descricao_qualificacao as descricao_qualificacao_responsavel,
    empresas.capital_social,
    empresas.porte_empresa,
    case empresas.porte_empresa
        when '00' then 'Nao informado'
        when '01' then 'Micro empresa'
        when '03' then 'Empresa de pequeno porte'
        when '05' then 'Demais'
    end as descricao_porte_empresa,
    empresas.ente_federativo_responsavel,
    empresas.linhas_origem_mesma_chave,
    empresas.arquivo_origem,
    empresas.data_processamento_utc
from empresas
left join {{ ref('stg_cnpj_naturezas_juridicas') }} as naturezas
    on empresas.natureza_juridica = naturezas.codigo_natureza_juridica
left join {{ ref('stg_cnpj_qualificacoes') }} as qualificacoes
    on empresas.qualificacao_responsavel = qualificacoes.codigo_qualificacao
