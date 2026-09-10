from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path

from cnpj_pipeline.layouts import CsvLayout, identificar_layout


@dataclass(frozen=True)
class ConvertedFile:
    dataset: str
    source_path: Path
    parquet_path: Path
    skipped: bool = False


def converter_csvs_extraidos_para_parquet(
    input_dir: Path,
    lakehouse_dir: Path,
    month: str,
    force: bool = False,
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
        for caminho_csv, layout in arquivos:
            destino = _parquet_path(lakehouse_dir, layout, month, caminho_csv)
            if destino.exists() and not force:
                resultados.append(
                    ConvertedFile(
                        dataset=layout.dataset,
                        source_path=caminho_csv,
                        parquet_path=destino,
                        skipped=True,
                    )
                )
                continue

            destino.parent.mkdir(parents=True, exist_ok=True)
            _converter_arquivo(conexao, caminho_csv, destino, layout)
            resultados.append(
                ConvertedFile(
                    dataset=layout.dataset,
                    source_path=caminho_csv,
                    parquet_path=destino,
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
        encoding = 'latin-1',
        null_padding = false,
        ignore_errors = false
    )
) to {sql_literal(str(destino))}
(format parquet, compression zstd);
"""
    conexao.execute(sql)


def quote_identifier(valor: str) -> str:
    return '"' + valor.replace('"', '""') + '"'


def sql_literal(valor: str) -> str:
    return "'" + valor.replace("'", "''") + "'"
