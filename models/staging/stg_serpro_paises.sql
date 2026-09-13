select
    lpad(codigo, 3, '0') as codigo_pais,
    descricao as descricao_pais,
    snapshot_id,
    arquivo_origem,
    data_processamento_utc
from {{ raw_serpro_dominio('paises') }}
