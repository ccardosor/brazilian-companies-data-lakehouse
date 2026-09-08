select
    codigo_qualificacao,
    descricao_qualificacao,
    arquivo_origem,
    data_processamento_utc
from {{ raw_cnpj('qualificacoes') }}
