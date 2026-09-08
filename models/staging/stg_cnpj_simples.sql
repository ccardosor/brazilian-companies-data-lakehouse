with fonte as (
    select * from {{ raw_cnpj('simples') }}
)

select
    cast(ano_mes as varchar) as ano_mes,
    cnpj_basico,
    opcao_simples,
    {{ data_receita('data_opcao_simples') }} as data_opcao_simples,
    {{ data_receita('data_exclusao_simples') }} as data_exclusao_simples,
    opcao_mei,
    {{ data_receita('data_opcao_mei') }} as data_opcao_mei,
    {{ data_receita('data_exclusao_mei') }} as data_exclusao_mei,
    arquivo_origem,
    data_processamento_utc
from fonte
