select
    codigo_natureza_juridica,
    descricao_natureza_juridica,
    arquivo_origem,
    data_processamento_utc
from {{ raw_cnpj('naturezas_juridicas') }}
