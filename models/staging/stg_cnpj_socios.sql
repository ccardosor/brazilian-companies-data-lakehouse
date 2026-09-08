with fonte as (
    select * from {{ raw_cnpj('socios') }}
)

select
    cast(ano_mes as varchar) as ano_mes,
    cnpj_basico,
    identificador_socio,
    nome_socio_razao_social,
    cnpj_cpf_socio,
    qualificacao_socio,
    {{ data_receita('data_entrada_sociedade') }} as data_entrada_sociedade,
    pais,
    representante_legal,
    nome_representante_legal,
    qualificacao_representante_legal,
    faixa_etaria,
    arquivo_origem,
    data_processamento_utc
from fonte
