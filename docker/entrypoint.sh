#!/bin/sh
# Recebe a competencia como argumento posicional ou via CNPJ_TARGET_MONTH.
set -e

MES="${1:-$CNPJ_TARGET_MONTH}"

if [ -z "$MES" ]; then
    echo "Erro: informe a competencia no formato YYYY-MM." >&2
    echo "  docker run ... cnpj-parquet-s3 2026-09" >&2
    echo "  docker run ... -e CNPJ_TARGET_MONTH=2026-09 cnpj-parquet-s3" >&2
    exit 2
fi

case "$MES" in
    [0-9][0-9][0-9][0-9]-[0-9][0-9]) ;;
    *)
        echo "Erro: competencia invalida '$MES'. Formato esperado YYYY-MM." >&2
        exit 2
        ;;
esac

if [ "$#" -gt 0 ]; then
    shift
fi

if [ -z "$S3_BUCKET_NAME" ]; then
    echo "Erro: S3_BUCKET_NAME nao definido." >&2
    exit 2
fi

echo "Competencia: $MES"
echo "Bucket: $S3_BUCKET_NAME | endpoint: ${S3_ENDPOINT_URL:-AWS padrao}"
echo "Temporarios: ${TMPDIR:-/tmp}"
echo

# exec para o Python virar PID 1 e receber SIGTERM direto do orquestrador.
exec python -u -m cnpj_pipeline.cli parquet-s3 --month "$MES" "$@"
