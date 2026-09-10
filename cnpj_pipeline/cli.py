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


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Ingere arquivos de CNPJ da Receita Federal."
    )
    parser.add_argument("--month", default=os.getenv("CNPJ_TARGET_MONTH"))
    parser.add_argument("--base-url", default=os.getenv("CNPJ_BASE_URL", DEFAULT_BASE_URL))
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

    return parser


def main() -> None:
    load_dotenv()
    args = build_parser().parse_args()

    from cnpj_pipeline.pipeline import ingest_month, ingest_to_s3_with_temp_spool
    from cnpj_pipeline.parquet import converter_csvs_extraidos_para_parquet
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
            progress_callback=print,
        )
        convertidos = [item for item in resultados if not item.skipped]
        pulados = [item for item in resultados if item.skipped]
        print(f"Arquivos convertidos: {len(convertidos)}")
        print(f"Arquivos pulados: {len(pulados)}")
        print(f"Lakehouse local: {Path(args.lakehouse_dir)}")
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
