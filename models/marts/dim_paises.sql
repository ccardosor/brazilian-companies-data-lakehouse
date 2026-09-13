with paises_receita as (
    select
        lpad(cnpj_paises.codigo_pais, 3, '0') as codigo_pais,
        cnpj_paises.descricao_pais,
        'receita' as dataset_origem
    from {{ ref('stg_cnpj_paises') }} as cnpj_paises
),

paises_serpro_complementares as (
    select
        lpad(serpro_paises.codigo_pais, 3, '0') as codigo_pais,
        serpro_paises.descricao_pais,
        'serpro' as dataset_origem
    from {{ ref('stg_serpro_paises') }} as serpro_paises
    where not exists (
        select 1
        from paises_receita
        where paises_receita.codigo_pais = lpad(serpro_paises.codigo_pais, 3, '0')
    )
),

paises_unificados as (
    select
        codigo_pais,
        descricao_pais,
        dataset_origem
    from paises_receita
    union all
    select
        codigo_pais,
        descricao_pais,
        dataset_origem
    from paises_serpro_complementares
)

select
    codigo_pais,
    descricao_pais,
    dataset_origem
from paises_unificados
