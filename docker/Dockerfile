# Container da conversao CSV -> Parquet direto no object storage.
# Recebe apenas a competencia (YYYY-MM); o resto vem por variavel de ambiente.
FROM python:3.12-slim

# PYTHONUNBUFFERED garante que o progresso apareca na saida do container em
# tempo real, sem ficar preso no buffer de bloco do stdout.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    TMPDIR=/tmp

WORKDIR /app

COPY docker/requirements.txt ./requirements.txt
RUN pip install --no-cache-dir -r requirements.txt

# Usuario sem privilegios: o container so precisa ler o bucket e escrever em /tmp.
RUN useradd --create-home --uid 10001 cnpj \
    && chown -R cnpj:cnpj /app

COPY --chown=cnpj:cnpj cnpj_pipeline ./cnpj_pipeline
COPY --chown=cnpj:cnpj docker/entrypoint.sh /usr/local/bin/entrypoint.sh
RUN chmod +x /usr/local/bin/entrypoint.sh

USER cnpj

# Baixa o httpfs na build para o container nao depender de rede para extensoes
# nem repetir o download a cada execucao.
RUN python -c "import duckdb; duckdb.connect().execute('install httpfs')"

ENTRYPOINT ["/usr/local/bin/entrypoint.sh"]
