from __future__ import annotations

import os
from pathlib import Path

from cnpj_pipeline.pipeline import ingest_month
from cnpj_pipeline.sinks import LocalSink
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

    base_url = os.getenv("CNPJ_BASE_URL", os.getenv("BASE_URL", DEFAULT_BASE_URL))
    target_month = os.getenv("CNPJ_TARGET_MONTH")
    local_base_dir = Path(os.getenv("LOCAL_BASE_DIR", "./downloads"))

    source = ReceitaCnpjSource(base_url)
    sink = LocalSink(local_base_dir)

    ingest_month(
        source=source,
        sink=sink,
        month=target_month,
        spool_dir=local_base_dir,
        extract=True,
    )


if __name__ == "__main__":
    main()
