select
    codigo_municipio,
    descricao_municipio,
    arquivo_origem,
    data_processamento_utc
from {{ raw_cnpj('municipios') }}
