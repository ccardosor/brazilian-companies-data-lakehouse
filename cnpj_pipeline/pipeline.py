from __future__ import annotations

from pathlib import Path
from tempfile import TemporaryDirectory

from cnpj_pipeline.sinks import LocalSink, S3Sink
from cnpj_pipeline.source import ReceitaCnpjSource


def ingest_month(
    source: ReceitaCnpjSource,
    sink: LocalSink | S3Sink,
    month: str | None,
    spool_dir: Path,
    extract: bool = True,
    force: bool = False,
) -> str:
    target_month = month or source.latest_month()
    print(f"Competencia selecionada: {target_month}")

    if not force and sink.is_done(target_month):
        print("Ingestao ja finalizada anteriormente.")
        return target_month

    files = source.list_zip_files(target_month)
    if not files:
        raise RuntimeError(f"Nenhum arquivo .zip encontrado para {target_month}.")

    print(f"Arquivos encontrados: {len(files)}")
    spool_dir.mkdir(parents=True, exist_ok=True)

    for item in files:
        if not force and sink.has_zip(target_month, item.name):
            print(f"Pulando arquivo ja ingerido: {item.name}")
            continue

        local_zip_path = spool_dir / target_month / item.name
        if not local_zip_path.exists() or force:
            print(f"Baixando: {item.name}")
            source.download_file(target_month, item.name, local_zip_path)

        ingested = sink.ingest_zip(target_month, local_zip_path, extract=extract)
        print(f"Ingerido: {ingested.zip_location}")

    sink.mark_done(target_month)
    print("Marcador de finalizacao criado.")
    return target_month


def ingest_to_s3_with_temp_spool(
    source: ReceitaCnpjSource,
    sink: S3Sink,
    month: str | None,
    extract: bool = True,
    force: bool = False,
) -> str:
    with TemporaryDirectory(prefix="cnpj-ingestion-") as temp_dir:
        return ingest_month(
            source=source,
            sink=sink,
            month=month,
            spool_dir=Path(temp_dir),
            extract=extract,
            force=force,
        )
