with motivos_receita as (
    select
        lpad(cnpj_motivos.codigo_motivo, 2, '0') as codigo_motivo,
        cnpj_motivos.descricao_motivo,
        'receita' as dataset_origem
    from {{ ref('stg_cnpj_motivos') }} as cnpj_motivos
),

motivos_serpro_complementares as (
    select
        lpad(serpro_motivos.codigo_motivo, 2, '0') as codigo_motivo,
        serpro_motivos.descricao_motivo,
        'serpro' as dataset_origem
    from {{ ref('stg_serpro_motivos') }} as serpro_motivos
    left join motivos_receita
        on lpad(serpro_motivos.codigo_motivo, 2, '0') = motivos_receita.codigo_motivo
    where motivos_receita.codigo_motivo is null
),

motivos_unificados as (
    select
        codigo_motivo,
        descricao_motivo,
        dataset_origem
    from motivos_receita
    union all
    select
        codigo_motivo,
        descricao_motivo,
        dataset_origem
    from motivos_serpro_complementares
)

select
    codigo_motivo,
    descricao_motivo,
    dataset_origem
from motivos_unificados
