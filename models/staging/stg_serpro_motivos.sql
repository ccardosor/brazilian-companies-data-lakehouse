select
    lpad(codigo, 2, '0') as codigo_motivo,
    descricao as descricao_motivo,
    snapshot_id,
    arquivo_origem,
    data_processamento_utc
from {{ raw_serpro_dominio('motivos_situacao_cadastral') }}
