with fonte as (
    select * from {{ raw_cnpj('empresas') }}
)

select
    cast(ano_mes as varchar) as ano_mes,
    cnpj_basico,
    razao_social,
    natureza_juridica,
    qualificacao_responsavel,
    {{ decimal_receita('capital_social') }} as capital_social,
    porte_empresa,
    ente_federativo_responsavel,
    arquivo_origem,
    data_processamento_utc
from fonte
