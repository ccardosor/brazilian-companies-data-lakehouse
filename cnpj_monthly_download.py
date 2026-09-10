from __future__ import annotations

import os
from pathlib import Path

from cnpj_pipeline.pipeline import ingest_month, ingest_to_s3_with_temp_spool
from cnpj_pipeline.sinks import LocalSink, S3Sink
from cnpj_pipeline.source import ReceitaCnpjSource

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    def load_dotenv() -> bool:
        return False


DEFAULT_BASE_URL = (
    "https://arquivos.receitafederal.gov.br/public.php/dav/files/YggdBLfdninEJX9/"
)


def main() -> None:
    load_dotenv()

    destino = os.getenv("CNPJ_DESTINATION", "local").lower()
    source = ReceitaCnpjSource(
        os.getenv("CNPJ_BASE_URL", os.getenv("BASE_URL", DEFAULT_BASE_URL))
    )

    if destino == "local":
        base_dir = Path(os.getenv("LOCAL_BASE_DIR", "./downloads"))
        ingest_month(
            source=source,
            sink=LocalSink(base_dir),
            month=os.getenv("CNPJ_TARGET_MONTH"),
            spool_dir=base_dir,
            extract=True,
        )
        return

    if destino != "s3":
        raise SystemExit("CNPJ_DESTINATION deve ser 'local' ou 's3'.")

    bucket_name = os.getenv("S3_BUCKET_NAME")
    if not bucket_name:
        raise SystemExit("Configure S3_BUCKET_NAME no .env antes de rodar a carga S3.")

    sink = S3Sink(
        bucket_name=bucket_name,
        prefix_root=os.getenv("S3_PREFIX_ROOT", "raw/cnpj"),
        region_name=os.getenv("AWS_REGION"),
        endpoint_url=os.getenv("S3_ENDPOINT_URL"),
    )
    sink.check_access()

    spool_dir = os.getenv("LOCAL_SPOOL_DIR")
    if spool_dir:
        ingest_month(
            source=source,
            sink=sink,
            month=os.getenv("CNPJ_TARGET_MONTH"),
            spool_dir=Path(spool_dir),
            extract=True,
        )
        return

    ingest_to_s3_with_temp_spool(
        source=source,
        sink=sink,
        month=os.getenv("CNPJ_TARGET_MONTH"),
        extract=True,
    )


if __name__ == "__main__":
    main()
