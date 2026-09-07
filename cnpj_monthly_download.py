import os
from pathlib import Path

from cnpj_pipeline.pipeline import ingest_month, ingest_to_s3_with_temp_spool
from cnpj_pipeline.sinks import S3Sink
from cnpj_pipeline.source import ReceitaCnpjSource

try:
    from dotenv import load_dotenv
except ModuleNotFoundError:
    def load_dotenv() -> bool:
        return False


DEFAULT_BASE_URL = (
    "https://arquivos.receitafederal.gov.br/public.php/dav/files/YggdBLfdninEJX9/"
)

def main():
    load_dotenv()

    bucket_name = os.getenv("S3_BUCKET_NAME")
    if not bucket_name:
        raise SystemExit("Configure S3_BUCKET_NAME no .env antes de rodar a carga S3.")

    source = ReceitaCnpjSource(
        os.getenv("CNPJ_BASE_URL", os.getenv("BASE_URL", DEFAULT_BASE_URL))
    )
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
    else:
        ingest_to_s3_with_temp_spool(
            source=source,
            sink=sink,
            month=os.getenv("CNPJ_TARGET_MONTH"),
            extract=True,
        )

if __name__ == "__main__":
    main()
