from __future__ import annotations

import argparse
import os
from pathlib import Path

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    def load_dotenv() -> bool:
        return False


DEFAULT_BASE_URL = (
    "https://arquivos.receitafederal.gov.br/public.php/dav/files/YggdBLfdninEJX9/"
)


def _imprimir(mensagem: str) -> None:
    """Escreve o progresso com flush.

    Sem isso, com a saida redirecionada para arquivo o Python acumula os prints
    em blocos e o log fica vazio por varios minutos durante a conversao.
    """
    print(mensagem, flush=True)


def _env_bool(nome: str, padrao: bool | None = None) -> bool | None:
    valor = os.getenv(nome)
    if valor is None or valor.strip() == "":
        return padrao
    return valor.strip().lower() in {"1", "true", "t", "yes", "y", "sim"}


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Ingere arquivos de CNPJ da Receita Federal."
    )
    parser.add_argument("--month", default=os.getenv("CNPJ_TARGET_MONTH"))
    parser.add_argument(
        "--base-url",
        default=os.getenv("CNPJ_BASE_URL", DEFAULT_BASE_URL),
    )
    parser.add_argument("--no-extract", action="store_true")
    parser.add_argument("--force", action="store_true")

    subparsers = parser.add_subparsers(dest="destination", required=True)

    local = subparsers.add_parser(
        "local", help="Baixa e extrai os arquivos localmente."
    )
    local.add_argument("--month", default=argparse.SUPPRESS)
    local.add_argument("--force", action="store_true", default=argparse.SUPPRESS)
    local.add_argument("--no-extract", action="store_true", default=argparse.SUPPRESS)
    local.add_argument(
        "--base-dir",
        default=os.getenv("LOCAL_BASE_DIR", "./downloads"),
        help="Diretorio local usado para armazenar zips e arquivos extraidos.",
    )

    s3 = subparsers.add_parser("s3", help="Baixa os arquivos e envia para o S3.")
    s3.add_argument("--month", default=argparse.SUPPRESS)
    s3.add_argument("--force", action="store_true", default=argparse.SUPPRESS)
    s3.add_argument("--no-extract", action="store_true", default=argparse.SUPPRESS)
    s3.add_argument("--bucket", default=os.getenv("S3_BUCKET_NAME"), required=False)
    s3.add_argument("--prefix-root", default=os.getenv("S3_PREFIX_ROOT", "raw/cnpj"))
    s3.add_argument("--region", default=os.getenv("AWS_REGION"))
    s3.add_argument("--endpoint-url", default=os.getenv("S3_ENDPOINT_URL"))
    s3.add_argument(
        "--url-style",
        choices=("path", "vhost"),
        default=os.getenv("S3_URL_STYLE"),
        help="Estilo de URL do object storage. MinIO exige path.",
    )
    s3.add_argument(
        "--no-ssl",
        action="store_true",
        help="Desliga SSL na conexao com o object storage.",
    )
    s3.add_argument(
        "--spool-dir",
        default=None,
        help="Diretorio temporario local opcional para os arquivos zip baixados.",
    )

    parquet = subparsers.add_parser(
        "parquet-local",
        help="Converte CSVs locais extraidos para Parquet particionado.",
    )
    parquet.add_argument("--month", default=argparse.SUPPRESS)
    parquet.add_argument("--force", action="store_true", default=argparse.SUPPRESS)
    parquet.add_argument(
        "--base-dir",
        default=os.getenv("LOCAL_BASE_DIR", "./downloads"),
        help="Diretorio local de ingestao que contem <competencia>_unzipped.",
    )
    parquet.add_argument(
        "--input-dir",
        default=None,
        help="Diretorio opcional com CSVs extraidos. Sobrescreve --base-dir.",
    )
    parquet.add_argument(
        "--lakehouse-dir",
        default=os.getenv("LOCAL_LAKEHOUSE_DIR", "./downloads/lakehouse"),
        help="Raiz local do lakehouse usada para gravar os Parquets raw/cnpj.",
    )

    parquet_s3 = subparsers.add_parser(
        "parquet-s3",
        help="Converte os CSVs brutos ja enviados ao S3 para Parquet no S3.",
    )
    parquet_s3.add_argument("--month", default=argparse.SUPPRESS)
    parquet_s3.add_argument("--force", action="store_true", default=argparse.SUPPRESS)
    parquet_s3.add_argument("--bucket", default=os.getenv("S3_BUCKET_NAME"))
    parquet_s3.add_argument(
        "--prefix-root",
        default=os.getenv("S3_PREFIX_ROOT", "raw/cnpj"),
        help="Prefixo da ingestao, onde estao os arquivos brutos da competencia.",
    )
    parquet_s3.add_argument(
        "--lakehouse-prefix",
        default=os.getenv("S3_LAKEHOUSE_PREFIX_ROOT", "lakehouse"),
        help="Prefixo do lakehouse no bucket, onde os Parquets sao gravados.",
    )
    parquet_s3.add_argument("--region", default=os.getenv("AWS_REGION"))
    parquet_s3.add_argument("--endpoint-url", default=os.getenv("S3_ENDPOINT_URL"))
    parquet_s3.add_argument(
        "--url-style",
        choices=("path", "vhost"),
        default=os.getenv("S3_URL_STYLE"),
        help="Estilo de URL do object storage. MinIO exige path.",
    )
    parquet_s3.add_argument(
        "--no-ssl",
        action="store_true",
        help="Desliga SSL na conexao com o object storage.",
    )

    serpro = subparsers.add_parser(
        "serpro-dominios",
        help=(
            "Baixa dominios complementares PJ do Serpro sem bloquear a pipeline "
            "principal."
        ),
    )
    serpro.add_argument(
        "--base-dir",
        default=os.getenv("LOCAL_BASE_DIR", "./downloads"),
        help="Diretorio local usado para armazenar os dominios complementares.",
    )
    serpro.add_argument(
        "--lakehouse-dir",
        default=os.getenv("LOCAL_LAKEHOUSE_DIR", "./downloads/lakehouse"),
        help="Raiz local do lakehouse usada para gravar os Parquets raw/serpro.",
    )
    serpro.add_argument(
        "--timeout",
        type=int,
        default=int(os.getenv("SERPRO_TIMEOUT_SECONDS", "60")),
        help="Timeout em segundos para cada download do Serpro.",
    )
    serpro.add_argument("--force", action="store_true", default=argparse.SUPPRESS)
    serpro.add_argument(
        "--no-parquet",
        action="store_true",
        help="Baixa os CSVs do Serpro sem converter os dominios para Parquet.",
    )

    return parser


def main() -> None:
    load_dotenv()
    args = build_parser().parse_args()

    from cnpj_pipeline.parquet import converter_csvs_extraidos_para_parquet
    from cnpj_pipeline.pipeline import ingest_month, ingest_to_s3_with_temp_spool
    from cnpj_pipeline.sinks import LocalSink, S3Sink

    if args.destination == "local":
        from cnpj_pipeline.source import ReceitaCnpjSource

        source = ReceitaCnpjSource(args.base_url)
        sink = LocalSink(Path(args.base_dir))
        ingest_month(
            source=source,
            sink=sink,
            month=args.month,
            spool_dir=Path(args.base_dir),
            extract=not args.no_extract,
            force=args.force,
        )
        return

    if args.destination == "parquet-local":
        if not args.month:
            raise SystemExit(
                "CNPJ_TARGET_MONTH ou --month e obrigatorio para conversao local."
            )

        input_dir = (
            Path(args.input_dir)
            if args.input_dir
            else Path(args.base_dir) / f"{args.month}_unzipped"
        )
        resultados = converter_csvs_extraidos_para_parquet(
            input_dir=input_dir,
            lakehouse_dir=Path(args.lakehouse_dir),
            month=args.month,
            force=args.force,
            progress_callback=_imprimir,
        )
        convertidos = [item for item in resultados if not item.skipped]
        pulados = [item for item in resultados if item.skipped]
        print(f"Arquivos convertidos: {len(convertidos)}")
        print(f"Arquivos pulados: {len(pulados)}")
        print(f"Lakehouse local: {Path(args.lakehouse_dir)}")
        return

    if args.destination == "parquet-s3":
        if not args.month:
            raise SystemExit(
                "CNPJ_TARGET_MONTH ou --month e obrigatorio para conversao no S3."
            )
        if not args.bucket:
            raise SystemExit(
                "S3_BUCKET_NAME ou --bucket e obrigatorio para conversao no S3."
            )

        from cnpj_pipeline.parquet_s3 import converter_csvs_s3_para_parquet

        sink = S3Sink(
            bucket_name=args.bucket,
            prefix_root=args.prefix_root,
            region_name=args.region,
            endpoint_url=args.endpoint_url,
            url_style=args.url_style,
            use_ssl=False if args.no_ssl else _env_bool("S3_USE_SSL"),
        )
        sink.check_access()

        resultados = converter_csvs_s3_para_parquet(
            sink=sink,
            month=args.month,
            lakehouse_prefix=args.lakehouse_prefix,
            force=args.force,
            progress_callback=_imprimir,
        )
        convertidos = [item for item in resultados if item.status == "ok"]
        pulados = [item for item in resultados if item.status == "pulado"]
        falhas = [item for item in resultados if item.status == "falha"]
        print(f"Arquivos convertidos: {len(convertidos)}")
        print(f"Arquivos pulados: {len(pulados)}")
        print(f"Arquivos com falha: {len(falhas)}")
        for item in falhas:
            print(f"  falha em {item.origem}: {item.erro}")
        prefixo = (args.lakehouse_prefix or "").strip("/")
        raiz = f"{prefixo}/raw/cnpj" if prefixo else "raw/cnpj"
        print(f"Lakehouse S3: s3://{args.bucket}/{raiz}")
        if falhas:
            raise SystemExit(1)
        return

    if args.destination == "serpro-dominios":
        from cnpj_pipeline.serpro import (
            SerproDomainSource,
            baixar_dominios_serpro,
            converter_dominios_serpro_para_parquet,
        )

        resultados = baixar_dominios_serpro(
            base_dir=Path(args.base_dir),
            source=SerproDomainSource(timeout=args.timeout),
            force=args.force,
            progress_callback=_imprimir,
        )
        for resultado in resultados:
            detalhe = resultado.path or resultado.error or "sem detalhe"
            print(f"{resultado.dataset}: {resultado.status} ({detalhe})")
        print(f"Diretorio local: {Path(args.base_dir) / 'serpro' / 'dominios' / 'pj'}")
        if args.no_parquet:
            return

        resultados_parquet = converter_dominios_serpro_para_parquet(
            base_dir=Path(args.base_dir),
            lakehouse_dir=Path(args.lakehouse_dir),
            force=args.force,
            progress_callback=_imprimir,
        )
        for resultado in resultados_parquet:
            detalhe = resultado.parquet_path or resultado.error or "sem detalhe"
            print(f"{resultado.dataset}: {resultado.status} ({detalhe})")
        print(
            "Lakehouse Serpro: "
            f"{Path(args.lakehouse_dir) / 'raw' / 'serpro' / 'dominios_pj'}"
        )
        return

    if not args.bucket:
        raise SystemExit("S3_BUCKET_NAME ou --bucket e obrigatorio para ingestao S3.")

    from cnpj_pipeline.source import ReceitaCnpjSource

    source = ReceitaCnpjSource(args.base_url)
    sink = S3Sink(
        bucket_name=args.bucket,
        prefix_root=args.prefix_root,
        region_name=args.region,
        endpoint_url=args.endpoint_url,
        url_style=args.url_style,
        use_ssl=False if args.no_ssl else _env_bool("S3_USE_SSL"),
    )
    sink.check_access()

    if args.spool_dir:
        ingest_month(
            source=source,
            sink=sink,
            month=args.month,
            spool_dir=Path(args.spool_dir),
            extract=not args.no_extract,
            force=args.force,
        )
    else:
        ingest_to_s3_with_temp_spool(
            source=source,
            sink=sink,
            month=args.month,
            extract=not args.no_extract,
            force=args.force,
        )


if __name__ == "__main__":
    main()
