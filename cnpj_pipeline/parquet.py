from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from uuid import uuid4

from cnpj_pipeline.layouts import CsvLayout, identificar_layout


@dataclass(frozen=True)
class ConvertedFile:
    dataset: str
    source_path: Path
    parquet_path: Path
    source_size_bytes: int
    parquet_size_bytes: int | None = None
    skipped: bool = False


def converter_csvs_extraidos_para_parquet(
    input_dir: Path,
    lakehouse_dir: Path,
    month: str,
    force: bool = False,
    progress_callback: Callable[[str], None] | None = None,
) -> list[ConvertedFile]:
    try:
        import duckdb
    except ModuleNotFoundError as error:
        raise RuntimeError(
            "A dependencia duckdb nao esta instalada. Rode `pip install -r requirements.txt`."
        ) from error

    if not input_dir.exists():
        raise FileNotFoundError(f"Diretorio de CSVs nao encontrado: {input_dir}")

    arquivos = _listar_csvs_com_layout(input_dir)
    if not arquivos:
        raise RuntimeError(f"Nenhum CSV conhecido foi encontrado em {input_dir}.")

    resultados: list[ConvertedFile] = []
    conexao = duckdb.connect(database=":memory:")
    try:
        total_arquivos = len(arquivos)
        for indice, (caminho_csv, layout) in enumerate(arquivos, start=1):
            source_size_bytes = caminho_csv.stat().st_size
            destino = _parquet_path(lakehouse_dir, layout, month, caminho_csv)
            _emitir_progresso(
                progress_callback,
                (
                    f"[{indice}/{total_arquivos}] Convertendo {layout.dataset}: "
                    f"{caminho_csv.name} ({_formatar_bytes(source_size_bytes)})"
                ),
            )
            if destino.exists() and not force:
                parquet_size_bytes = destino.stat().st_size
                _emitir_progresso(
                    progress_callback,
                    f"[{indice}/{total_arquivos}] Pulado: {destino}",
                )
                resultados.append(
                    ConvertedFile(
                        dataset=layout.dataset,
                        source_path=caminho_csv,
                        parquet_path=destino,
                        source_size_bytes=source_size_bytes,
                        parquet_size_bytes=parquet_size_bytes,
                        skipped=True,
                    )
                )
                continue

            destino.parent.mkdir(parents=True, exist_ok=True)
            usou_fallback = _converter_arquivo(conexao, caminho_csv, destino, layout)
            parquet_size_bytes = destino.stat().st_size
            detalhe_fallback = " com fallback UTF-8" if usou_fallback else ""
            _emitir_progresso(
                progress_callback,
                (
                    f"[{indice}/{total_arquivos}] Concluido{detalhe_fallback}: "
                    f"{destino} ({_formatar_bytes(parquet_size_bytes)})"
                ),
            )
            resultados.append(
                ConvertedFile(
                    dataset=layout.dataset,
                    source_path=caminho_csv,
                    parquet_path=destino,
                    source_size_bytes=source_size_bytes,
                    parquet_size_bytes=parquet_size_bytes,
                )
            )
    finally:
        conexao.close()

    _escrever_manifest_conversao(lakehouse_dir, month, resultados)
    return resultados


def _listar_csvs_com_layout(input_dir: Path) -> list[tuple[Path, CsvLayout]]:
    arquivos: list[tuple[Path, CsvLayout]] = []
    for caminho in sorted(input_dir.rglob("*")):
        if not caminho.is_file():
            continue

        layout = identificar_layout(caminho)
        if layout is not None:
            arquivos.append((caminho, layout))

    return arquivos


def _escrever_manifest_conversao(
    lakehouse_dir: Path,
    month: str,
    resultados: list[ConvertedFile],
) -> None:
    manifest_path = (
        lakehouse_dir
        / "raw"
        / "cnpj"
        / "_manifests"
        / "conversao"
        / f"ano_mes={month}"
        / "manifest.json"
    )
    manifest_path.parent.mkdir(parents=True, exist_ok=True)
    payload = {
        "ano_mes": month,
        "finalizado_em_utc": datetime.now(timezone.utc).isoformat(),
        "arquivos_convertidos": sum(not item.skipped for item in resultados),
        "arquivos_pulados": sum(item.skipped for item in resultados),
        "arquivos": [
            {
                "dataset": item.dataset,
                "origem": str(item.source_path),
                "destino": str(item.parquet_path),
                "origem_bytes": item.source_size_bytes,
                "destino_bytes": item.parquet_size_bytes,
                "pulado": item.skipped,
            }
            for item in resultados
        ],
    }
    manifest_path.write_text(
        json.dumps(payload, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )


def _parquet_path(
    lakehouse_dir: Path,
    layout: CsvLayout,
    month: str,
    source_path: Path,
) -> Path:
    return (
        lakehouse_dir
        / "raw"
        / "cnpj"
        / layout.dataset
        / f"ano_mes={month}"
        / f"{source_path.name}.parquet"
    )


def _converter_arquivo(
    conexao: "duckdb.DuckDBPyConnection",
    source_path: Path,
    destino: Path,
    layout: CsvLayout,
) -> bool:
    destino_temporario = destino.with_name(f".{destino.name}.{uuid4().hex}.tmp")
    if destino_temporario.exists():
        destino_temporario.unlink()

    usou_fallback = False
    try:
        _executar_copy(conexao, source_path, destino_temporario, layout, "latin-1")
    except Exception as error:
        if not _erro_de_encoding_latin1(error):
            _remover_se_existir(destino_temporario)
            raise

        usou_fallback = True
        csv_utf8 = source_path.with_name(f".{source_path.name}.{uuid4().hex}.utf8.tmp")
        try:
            _transcodificar_para_utf8(source_path, csv_utf8)
            _executar_copy(conexao, csv_utf8, destino_temporario, layout, "utf-8")
        finally:
            _remover_se_existir(csv_utf8)

    destino_temporario.replace(destino)
    return usou_fallback


def _executar_copy(
    conexao: "duckdb.DuckDBPyConnection",
    source_path: Path,
    destino: Path,
    layout: CsvLayout,
    encoding: str,
) -> None:
    colunas_csv = ", ".join(f"'{coluna}': 'VARCHAR'" for coluna in layout.colunas)
    colunas_select = ",\n        ".join(
        f"nullif(trim({quote_identifier(coluna)}), '') as {quote_identifier(coluna)}"
        for coluna in layout.colunas
    )

    sql = f"""
copy (
    select
        {colunas_select},
        {sql_literal(source_path.name)} as arquivo_origem,
        current_timestamp as data_processamento_utc
    from read_csv(
        {sql_literal(str(source_path))},
        delim = ';',
        quote = '"',
        escape = '"',
        header = false,
        columns = {{{colunas_csv}}},
        encoding = {sql_literal(encoding)},
        null_padding = false,
        ignore_errors = false
    )
) to {sql_literal(str(destino))}
(format parquet, compression zstd);
"""
    conexao.execute(sql)


def _erro_de_encoding_latin1(error: Exception) -> bool:
    mensagem = str(error).lower()
    return "latin-1" in mensagem and "encoded" in mensagem


def _transcodificar_para_utf8(source_path: Path, destino: Path) -> None:
    with (
        source_path.open("r", encoding="latin-1", newline="") as origem,
        destino.open("w", encoding="utf-8", newline="") as saida,
    ):
        while chunk := origem.read(1024 * 1024):
            saida.write(chunk)


def _remover_se_existir(caminho: Path) -> None:
    try:
        caminho.unlink()
    except FileNotFoundError:
        return


def _emitir_progresso(
    progress_callback: Callable[[str], None] | None,
    mensagem: str,
) -> None:
    if progress_callback is not None:
        progress_callback(mensagem)


def _formatar_bytes(valor: int) -> str:
    unidades = ("B", "KB", "MB", "GB", "TB")
    tamanho = float(valor)
    for unidade in unidades:
        if tamanho < 1024 or unidade == unidades[-1]:
            return f"{tamanho:.1f} {unidade}"
        tamanho /= 1024
    return f"{valor} B"


def quote_identifier(valor: str) -> str:
    return '"' + valor.replace('"', '""') + '"'


def sql_literal(valor: str) -> str:
    return "'" + valor.replace("'", "''") + "'"
