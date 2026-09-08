select
    codigo_cnae,
    descricao_cnae,
    arquivo_origem,
    data_processamento_utc
from {{ raw_cnpj('cnaes') }}
