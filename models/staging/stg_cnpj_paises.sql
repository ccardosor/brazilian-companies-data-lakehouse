select
    codigo_pais,
    descricao_pais,
    arquivo_origem,
    data_processamento_utc
from {{ raw_cnpj('paises') }}
