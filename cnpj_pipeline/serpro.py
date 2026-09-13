from __future__ import annotations

import csv
import hashlib
import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from shutil import copy2
from urllib.parse import urlparse

import duckdb
import requests

SERPRO_DOMINIOS_PJ = {
    "paises": "https://bcadastros.serpro.gov.br/documentacao/dominios/pj/pais.csv",
    "motivos_situacao_cadastral": (
        "https://bcadastros.serpro.gov.br/documentacao/dominios/pj/"
        "motivo_situacao_cadastral.csv"
    ),
}


@dataclass(frozen=True)
class SerproDomainResult:
    dataset: str
    url: str
    filename: str
    status: str
    path: str | None
    sha256: str | None
    size_bytes: int | None
    rows: int | None
    error: str | None = None
    reused_from: str | None = None


@dataclass(frozen=True)
class SerproParquetResult:
    dataset: str
    csv_path: str
    parquet_path: str | None
    status: str
    rows: int | None
    error: str | None = None


class SerproDomainSource:
    def __init__(self, timeout: int = 60, session=None) -> None:
        self.timeout = timeout
        self.session = session or requests.Session()

    def download_file(self, url: str, destination: Path) -> None:
        destination.parent.mkdir(parents=True, exist_ok=True)
        temp_destination = destination.with_suffix(f"{destination.suffix}.part")

        with self.session.get(url, stream=True, timeout=self.timeout) as response:
            response.raise_for_status()
            with temp_destination.open("wb") as file:
                for chunk in response.iter_content(chunk_size=1024 * 1024):
                    if chunk:
                        file.write(chunk)

        temp_destination.replace(destination)


def baixar_dominios_serpro(
    base_dir: Path,
    source: SerproDomainSource | None = None,
    force: bool = False,
    progress_callback: Callable[[str], None] | None = None,
) -> list[SerproDomainResult]:
    source = source or SerproDomainSource()
    root_dir = base_dir / "serpro" / "dominios" / "pj"
    current_dir = root_dir / "atual"
    snapshot_id = datetime.now(UTC).strftime("%Y%m%dT%H%M%SZ")
    snapshot_dir = root_dir / "snapshots" / snapshot_id

    results: list[SerproDomainResult] = []
    for dataset, url in SERPRO_DOMINIOS_PJ.items():
        filename = _filename_from_url(url)
        current_path = current_dir / filename
        snapshot_path = snapshot_dir / filename

        if current_path.exists() and not force:
            result = _result_from_existing_file(
                dataset=dataset,
                url=url,
                filename=filename,
                path=current_path,
                status="existente",
            )
            results.append(result)
            _emitir(progress_callback, f"Dominio Serpro ja existe: {filename}")
            continue

        try:
            _emitir(progress_callback, f"Baixando dominio Serpro: {filename}")
            source.download_file(url, snapshot_path)
            current_path.parent.mkdir(parents=True, exist_ok=True)
            copy2(snapshot_path, current_path)
            result = _result_from_existing_file(
                dataset=dataset,
                url=url,
                filename=filename,
                path=current_path,
                status="baixado",
            )
        except Exception as error:
            if _is_valid_local_file(current_path):
                result = _result_from_existing_file(
                    dataset=dataset,
                    url=url,
                    filename=filename,
                    path=current_path,
                    status="reutilizado",
                    error=str(error),
                    reused_from=current_path,
                )
                _emitir(
                    progress_callback,
                    f"Falha ao baixar {filename}; reutilizando snapshot local.",
                )
            else:
                result = SerproDomainResult(
                    dataset=dataset,
                    url=url,
                    filename=filename,
                    status="falhou",
                    path=None,
                    sha256=None,
                    size_bytes=None,
                    rows=None,
                    error=str(error),
                )
                _emitir(
                    progress_callback,
                    f"Falha ao baixar {filename}; nenhum snapshot local valido.",
                )

        results.append(result)

    _write_manifest(root_dir, snapshot_id, results)
    return results


def converter_dominios_serpro_para_parquet(
    base_dir: Path,
    lakehouse_dir: Path,
    force: bool = False,
    progress_callback: Callable[[str], None] | None = None,
) -> list[SerproParquetResult]:
    root_dir = base_dir / "serpro" / "dominios" / "pj"
    current_dir = root_dir / "atual"
    download_manifest = _read_latest_manifest(root_dir)
    snapshot_id = download_manifest.get("snapshot_id") or datetime.now(UTC).strftime(
        "%Y%m%dT%H%M%SZ"
    )
    output_root = lakehouse_dir / "raw" / "serpro" / "dominios_pj"

    results: list[SerproParquetResult] = []
    connection = duckdb.connect(database=":memory:")
    try:
        for dataset, url in SERPRO_DOMINIOS_PJ.items():
            filename = _filename_from_url(url)
            csv_path = current_dir / filename
            output_dir = output_root / dataset / f"snapshot_id={snapshot_id}"
            parquet_path = output_dir / f"{dataset}.parquet"

            if not _is_valid_local_file(csv_path):
                results.append(
                    SerproParquetResult(
                        dataset=dataset,
                        csv_path=str(csv_path),
                        parquet_path=None,
                        status="falhou",
                        rows=None,
                        error="CSV atual nao encontrado ou vazio.",
                    )
                )
                _emitir(
                    progress_callback,
                    f"CSV Serpro ausente ou vazio; pulando Parquet: {dataset}",
                )
                continue

            if parquet_path.exists() and not force:
                rows = connection.execute(
                    "select count(*) from read_parquet(?)",
                    [str(parquet_path)],
                ).fetchone()[0]
                results.append(
                    SerproParquetResult(
                        dataset=dataset,
                        csv_path=str(csv_path),
                        parquet_path=str(parquet_path),
                        status="existente",
                        rows=rows,
                    )
                )
                _emitir(progress_callback, f"Parquet Serpro ja existe: {dataset}")
                continue

            try:
                _emitir(progress_callback, f"Convertendo dominio Serpro: {dataset}")
                output_dir.mkdir(parents=True, exist_ok=True)
                temp_path = parquet_path.with_suffix(".parquet.tmp")
                if temp_path.exists():
                    temp_path.unlink()

                rows = _write_serpro_parquet(
                    connection=connection,
                    csv_path=csv_path,
                    parquet_path=temp_path,
                    dataset=dataset,
                    snapshot_id=snapshot_id,
                )
                temp_path.replace(parquet_path)
                results.append(
                    SerproParquetResult(
                        dataset=dataset,
                        csv_path=str(csv_path),
                        parquet_path=str(parquet_path),
                        status="convertido",
                        rows=rows,
                    )
                )
            except Exception as error:
                results.append(
                    SerproParquetResult(
                        dataset=dataset,
                        csv_path=str(csv_path),
                        parquet_path=None,
                        status="falhou",
                        rows=None,
                        error=str(error),
                    )
                )
    finally:
        connection.close()

    _write_parquet_manifest(output_root, snapshot_id, results)
    return results


def _filename_from_url(url: str) -> str:
    path = urlparse(url).path
    filename = Path(path).name
    if not filename:
        raise ValueError(f"URL sem nome de arquivo: {url}")
    return filename


def _result_from_existing_file(
    dataset: str,
    url: str,
    filename: str,
    path: Path,
    status: str,
    error: str | None = None,
    reused_from: Path | None = None,
) -> SerproDomainResult:
    content = path.read_bytes()
    return SerproDomainResult(
        dataset=dataset,
        url=url,
        filename=filename,
        status=status,
        path=str(path),
        sha256=hashlib.sha256(content).hexdigest(),
        size_bytes=len(content),
        rows=_count_data_rows(content),
        error=error,
        reused_from=str(reused_from) if reused_from else None,
    )


def _count_data_rows(content: bytes) -> int:
    text = _decode_csv_bytes(content)
    lines = [line for line in text.splitlines() if line.strip()]
    if not lines:
        return 0
    return max(len(lines) - 1, 0)


def _read_latest_manifest(root_dir: Path) -> dict[str, object]:
    manifest_path = root_dir / "_manifests" / "ultimo-manifest.json"
    if not manifest_path.exists():
        return {}
    return json.loads(manifest_path.read_text(encoding="utf-8"))


def _write_serpro_parquet(
    connection: duckdb.DuckDBPyConnection,
    csv_path: Path,
    parquet_path: Path,
    dataset: str,
    snapshot_id: str,
) -> int:
    rows = _read_serpro_csv_rows(csv_path)
    connection.execute(
        """
        create or replace table serpro_dominio (
            codigo varchar,
            descricao varchar,
            dataset varchar,
            arquivo_origem varchar,
            snapshot_id varchar,
            data_processamento_utc timestamp
        )
        """
    )
    processado_em = datetime.now(UTC).replace(tzinfo=None)
    if rows:
        connection.executemany(
            """
            insert into serpro_dominio values (?, ?, ?, ?, ?, ?)
            """,
            [
                (
                    row["codigo"],
                    row["descricao"],
                    dataset,
                    csv_path.name,
                    snapshot_id,
                    processado_em,
                )
                for row in rows
            ],
        )
    connection.execute(
        f"copy serpro_dominio to {_sql_literal(str(parquet_path))} (format parquet)"
    )
    return len(rows)


def _read_serpro_csv_rows(csv_path: Path) -> list[dict[str, str]]:
    text = _decode_csv_bytes(csv_path.read_bytes())
    reader = csv.DictReader(text.splitlines(), delimiter=";")
    rows: list[dict[str, str]] = []
    for row in reader:
        normalized_row = {
            _normalizar_cabecalho(key): value for key, value in row.items()
        }
        rows.append(
            {
                "codigo": (normalized_row.get("codigo") or "").strip(),
                "descricao": (normalized_row.get("descricao") or "").strip(),
            }
        )
    return rows


def _normalizar_cabecalho(value: str | None) -> str:
    if value is None:
        return ""
    return (
        value.strip()
        .lower()
        .replace("ó", "o")
        .replace("ç", "c")
        .replace("ã", "a")
        .replace("á", "a")
        .replace("é", "e")
    )


def _decode_csv_bytes(content: bytes) -> str:
    for encoding in ("utf-8-sig", "utf-8", "latin1"):
        try:
            return content.decode(encoding)
        except UnicodeDecodeError:
            continue
    return content.decode("latin1", errors="replace")


def _is_valid_local_file(path: Path) -> bool:
    return path.exists() and path.is_file() and path.stat().st_size > 0


def _write_manifest(
    root_dir: Path,
    snapshot_id: str,
    results: list[SerproDomainResult],
) -> None:
    manifest_dir = root_dir / "_manifests"
    manifest_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "fonte": "serpro",
        "escopo": "dominios_pj",
        "coletado_em_utc": datetime.now(UTC).isoformat(),
        "snapshot_id": snapshot_id,
        "bloqueia_pipeline_principal": False,
        "arquivos": [_result_to_dict(result) for result in results],
    }
    content = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    (manifest_dir / "ultimo-manifest.json").write_text(content, encoding="utf-8")
    (manifest_dir / f"{snapshot_id}.json").write_text(content, encoding="utf-8")


def _write_parquet_manifest(
    output_root: Path,
    snapshot_id: str,
    results: list[SerproParquetResult],
) -> None:
    manifest_dir = (
        output_root / "_manifests" / "conversao" / f"snapshot_id={snapshot_id}"
    )
    manifest_dir.mkdir(parents=True, exist_ok=True)
    payload = {
        "fonte": "serpro",
        "escopo": "dominios_pj",
        "convertido_em_utc": datetime.now(UTC).isoformat(),
        "snapshot_id": snapshot_id,
        "arquivos": [_parquet_result_to_dict(result) for result in results],
    }
    (manifest_dir / "manifest.json").write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def _result_to_dict(result: SerproDomainResult) -> dict[str, object]:
    return {
        "dataset": result.dataset,
        "url": result.url,
        "arquivo": result.filename,
        "status": result.status,
        "caminho": result.path,
        "sha256": result.sha256,
        "tamanho_bytes": result.size_bytes,
        "linhas_dados": result.rows,
        "erro": result.error,
        "reutilizado_de": result.reused_from,
    }


def _parquet_result_to_dict(result: SerproParquetResult) -> dict[str, object]:
    return {
        "dataset": result.dataset,
        "csv_origem": result.csv_path,
        "parquet_destino": result.parquet_path,
        "status": result.status,
        "linhas": result.rows,
        "erro": result.error,
    }


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def _emitir(progress_callback: Callable[[str], None] | None, message: str) -> None:
    if progress_callback:
        progress_callback(message)
