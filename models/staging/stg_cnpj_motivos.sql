select
    codigo_motivo,
    descricao_motivo,
    arquivo_origem,
    data_processamento_utc
from {{ raw_cnpj('motivos') }}
