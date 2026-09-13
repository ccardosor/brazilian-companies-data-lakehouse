from __future__ import annotations

import hashlib
import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from shutil import copy2
from urllib.parse import urlparse

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
    progress_callback=None,
) -> list[SerproDomainResult]:
    source = source or SerproDomainSource()
    root_dir = base_dir / "serpro" / "dominios" / "pj"
    current_dir = root_dir / "atual"
    snapshot_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
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
        "coletado_em_utc": datetime.now(timezone.utc).isoformat(),
        "snapshot_id": snapshot_id,
        "bloqueia_pipeline_principal": False,
        "arquivos": [_result_to_dict(result) for result in results],
    }
    content = json.dumps(payload, ensure_ascii=False, indent=2) + "\n"
    (manifest_dir / "ultimo-manifest.json").write_text(content, encoding="utf-8")
    (manifest_dir / f"{snapshot_id}.json").write_text(content, encoding="utf-8")


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


def _emitir(progress_callback, message: str) -> None:
    if progress_callback:
        progress_callback(message)
