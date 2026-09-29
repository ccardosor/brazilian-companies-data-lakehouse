"""Conversao dos CSVs brutos ja ingeridos no S3 para Parquet no proprio S3.

O caminho principal usa a extensao httpfs do DuckDB para ler o CSV direto do
object storage e gravar o Parquet tambem no object storage, sem passar por
disco. Arquivos temporarios locais so aparecem em dois casos, e sao apagados
logo depois de cada arquivo: o fallback de encoding latin-1 e a leitura de
zips quando a ingestao rodou com `--no-extract`.
"""

from __future__ import annotations

import json
import zipfile
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path
from tempfile import TemporaryDirectory

from cnpj_pipeline.layouts import CsvLayout, identificar_layout
from cnpj_pipeline.parquet import (
    _erro_de_encoding_latin1,
    _formatar_bytes,
    _transcodificar_para_utf8,
    executar_copy_csv_para_parquet,
    sql_literal,
)
from cnpj_pipeline.sinks import S3Sink

DEFAULT_LAKEHOUSE_PREFIX = "lakehouse"

STATUS_OK = "ok"
STATUS_FALHA = "falha"
STATUS_PULADO = "pulado"


@dataclass(frozen=True)
class OrigemS3:
    """Uma origem a converter: um CSV solto no S3 ou um zip a ser aberto."""

    chave: str
    layout: CsvLayout | None = None
    is_zip: bool = False
    tamanho_bytes: int | None = None

    @property
    def nome_arquivo(self) -> str:
        return self.chave.rsplit("/", 1)[-1]


@dataclass(frozen=True)
class ConversaoS3Resultado:
    dataset: str
    origem: str
    destino: str
    status: str
    linhas: int | None = None
    origem_bytes: int | None = None
    destino_bytes: int | None = None
    erro: str | None = None


def converter_csvs_s3_para_parquet(
    sink: S3Sink,
    month: str,
    lakehouse_prefix: str = DEFAULT_LAKEHOUSE_PREFIX,
    force: bool = False,
    progress_callback: Callable[[str], None] | None = None,
    conexao=None,
) -> list[ConversaoS3Resultado]:
    """Converte para Parquet os arquivos brutos da competencia ja enviados ao S3.

    A conversao e retomavel: arquivos cujo Parquet ja existe no bucket sao
    pulados, e o manifest e regravado a cada arquivo para que uma interrupcao
    nao perca o progresso ja feito.
    """
    prefixo = _prefixo_lakehouse(lakehouse_prefix)
    origens = _listar_origens(sink, month, progress_callback)
    if not origens:
        raise RuntimeError(
            "Nenhum arquivo conhecido encontrado em "
            f"{sink.s3_uri(sink.extracted_prefix(month))} nem em "
            f"{sink.s3_uri(sink.zipped_prefix(month))}."
        )

    manifest_anterior = _ler_manifest(sink, prefixo, month)
    zips_concluidos = set(manifest_anterior.get("zips_concluidos", []) or [])
    entradas_anteriores = _entradas_por_origem(manifest_anterior)
    linhas_conhecidas = _linhas_por_destino(manifest_anterior)

    resultados: list[ConversaoS3Resultado] = []
    conexao_propria = conexao is None
    conexao = conexao if conexao is not None else _abrir_conexao_duckdb(sink)

    try:
        total = len(origens)
        for indice, origem in enumerate(origens, start=1):
            rotulo = f"[{indice}/{total}]"
            if origem.is_zip:
                if not force and origem.chave in zips_concluidos:
                    _emitir(
                        progress_callback,
                        f"{rotulo} {origem.nome_arquivo}: ja convertido, pulado",
                    )
                    # Preserva no manifest o que a execucao anterior ja registrou
                    # para este zip, senao a regravacao apagaria o historico.
                    resultados.extend(
                        _resultados_preservados(
                            entradas_anteriores, sink.s3_uri(origem.chave)
                        )
                    )
                    continue

                novos = _processar_zip(
                    sink=sink,
                    conexao=conexao,
                    origem=origem,
                    prefixo=prefixo,
                    month=month,
                    force=force,
                    rotulo=rotulo,
                    linhas_conhecidas=linhas_conhecidas,
                    progress_callback=progress_callback,
                )
                resultados.extend(novos)
                if all(item.status != STATUS_FALHA for item in novos):
                    zips_concluidos.add(origem.chave)
            else:
                resultados.append(
                    _processar_csv(
                        sink=sink,
                        conexao=conexao,
                        origem=origem,
                        prefixo=prefixo,
                        month=month,
                        force=force,
                        rotulo=rotulo,
                        linhas_conhecidas=linhas_conhecidas,
                        progress_callback=progress_callback,
                    )
                )

            _escrever_manifest(
                sink=sink,
                prefixo=prefixo,
                month=month,
                resultados=resultados,
                zips_concluidos=zips_concluidos,
                finalizado=False,
            )
    finally:
        if conexao_propria:
            conexao.close()

    _escrever_manifest(
        sink=sink,
        prefixo=prefixo,
        month=month,
        resultados=resultados,
        zips_concluidos=zips_concluidos,
        finalizado=True,
    )
    return resultados


def _listar_origens(
    sink: S3Sink,
    month: str,
    progress_callback: Callable[[str], None] | None,
) -> list[OrigemS3]:
    """Prefere os CSVs ja extraidos; cai para os zips se `unzipped/` estiver vazio."""
    csvs: list[OrigemS3] = []
    for chave in sink.list_keys(sink.extracted_prefix(month)):
        layout = identificar_layout(Path(chave))
        if layout is not None:
            csvs.append(OrigemS3(chave=chave, layout=layout))

    if csvs:
        _emitir(
            progress_callback,
            f"Origem: {len(csvs)} CSVs em {sink.s3_uri(sink.extracted_prefix(month))}",
        )
        return csvs

    zips = [
        OrigemS3(chave=chave, is_zip=True)
        for chave in sink.list_keys(sink.zipped_prefix(month))
        if chave.lower().endswith(".zip")
    ]
    if zips:
        _emitir(
            progress_callback,
            f"Origem: {len(zips)} zips em {sink.s3_uri(sink.zipped_prefix(month))} "
            "(nenhum CSV extraido encontrado)",
        )
    return zips


def _processar_csv(
    sink: S3Sink,
    conexao,
    origem: OrigemS3,
    prefixo: str,
    month: str,
    force: bool,
    rotulo: str,
    linhas_conhecidas: dict[str, int],
    progress_callback: Callable[[str], None] | None,
) -> ConversaoS3Resultado:
    layout = origem.layout
    assert layout is not None
    destino = _parquet_key(prefixo, layout.dataset, month, origem.nome_arquivo)

    pulado = _resultado_se_ja_convertido(
        sink=sink,
        dataset=layout.dataset,
        origem_uri=sink.s3_uri(origem.chave),
        destino=destino,
        force=force,
        rotulo=rotulo,
        nome_arquivo=origem.nome_arquivo,
        linhas_conhecidas=linhas_conhecidas,
        progress_callback=progress_callback,
    )
    if pulado is not None:
        return pulado

    origem_bytes = sink.object_size(origem.chave)
    _emitir(
        progress_callback,
        f"{rotulo} {origem.nome_arquivo}: convertendo "
        f"({_formatar_bytes(origem_bytes or 0)})",
    )

    try:
        linhas = _converter_para_s3(
            sink=sink,
            conexao=conexao,
            origem_uri=sink.s3_uri(origem.chave),
            chave_origem=origem.chave,
            destino_uri=sink.s3_uri(destino),
            layout=layout,
            nome_arquivo=origem.nome_arquivo,
        )
    except Exception as error:  # noqa: BLE001 - uma falha nao aborta o lote
        _emitir(progress_callback, f"{rotulo} {origem.nome_arquivo}: FALHA - {error}")
        return ConversaoS3Resultado(
            dataset=layout.dataset,
            origem=sink.s3_uri(origem.chave),
            destino=sink.s3_uri(destino),
            status=STATUS_FALHA,
            origem_bytes=origem_bytes,
            erro=str(error),
        )

    destino_bytes = sink.object_size(destino)
    _emitir(
        progress_callback,
        f"{rotulo} {origem.nome_arquivo}: convertido, "
        f"{_formatar_linhas(linhas)} linhas, "
        f"{_formatar_bytes(destino_bytes or 0)} enviado",
    )
    return ConversaoS3Resultado(
        dataset=layout.dataset,
        origem=sink.s3_uri(origem.chave),
        destino=sink.s3_uri(destino),
        status=STATUS_OK,
        linhas=linhas,
        origem_bytes=origem_bytes,
        destino_bytes=destino_bytes,
    )


def _processar_zip(
    sink: S3Sink,
    conexao,
    origem: OrigemS3,
    prefixo: str,
    month: str,
    force: bool,
    rotulo: str,
    linhas_conhecidas: dict[str, int],
    progress_callback: Callable[[str], None] | None,
) -> list[ConversaoS3Resultado]:
    """Baixa um zip por vez, converte membro a membro e limpa os temporarios."""
    resultados: list[ConversaoS3Resultado] = []

    with TemporaryDirectory(prefix="cnpj-parquet-s3-") as temp_dir:
        temp_path = Path(temp_dir)
        zip_local = temp_path / origem.nome_arquivo
        _emitir(progress_callback, f"{rotulo} {origem.nome_arquivo}: baixando zip")
        try:
            sink.download_to(origem.chave, zip_local)
        except Exception as error:  # noqa: BLE001 - uma falha nao aborta o lote
            _emitir(
                progress_callback, f"{rotulo} {origem.nome_arquivo}: FALHA - {error}"
            )
            return [
                ConversaoS3Resultado(
                    dataset="desconhecido",
                    origem=sink.s3_uri(origem.chave),
                    destino="",
                    status=STATUS_FALHA,
                    erro=str(error),
                )
            ]

        with zipfile.ZipFile(zip_local, "r") as archive:
            membros = [
                nome
                for nome in archive.namelist()
                if identificar_layout(Path(nome)) is not None
            ]
            for membro in membros:
                layout = identificar_layout(Path(membro))
                assert layout is not None
                nome_arquivo = membro.rsplit("/", 1)[-1]
                destino = _parquet_key(prefixo, layout.dataset, month, nome_arquivo)
                origem_uri = f"{sink.s3_uri(origem.chave)}!{membro}"

                pulado = _resultado_se_ja_convertido(
                    sink=sink,
                    dataset=layout.dataset,
                    origem_uri=origem_uri,
                    destino=destino,
                    force=force,
                    rotulo=rotulo,
                    nome_arquivo=nome_arquivo,
                    linhas_conhecidas=linhas_conhecidas,
                    progress_callback=progress_callback,
                )
                if pulado is not None:
                    resultados.append(pulado)
                    continue

                csv_local = temp_path / nome_arquivo
                try:
                    with archive.open(membro) as entrada, csv_local.open("wb") as saida:
                        while chunk := entrada.read(1024 * 1024):
                            saida.write(chunk)

                    origem_bytes = csv_local.stat().st_size
                    _emitir(
                        progress_callback,
                        f"{rotulo} {nome_arquivo}: convertendo "
                        f"({_formatar_bytes(origem_bytes)})",
                    )
                    linhas = _converter_local_para_s3(
                        conexao=conexao,
                        csv_local=csv_local,
                        destino_uri=sink.s3_uri(destino),
                        layout=layout,
                        nome_arquivo=nome_arquivo,
                    )
                except Exception as error:  # noqa: BLE001 - uma falha nao aborta o lote
                    _emitir(
                        progress_callback, f"{rotulo} {nome_arquivo}: FALHA - {error}"
                    )
                    resultados.append(
                        ConversaoS3Resultado(
                            dataset=layout.dataset,
                            origem=origem_uri,
                            destino=sink.s3_uri(destino),
                            status=STATUS_FALHA,
                            erro=str(error),
                        )
                    )
                    continue
                finally:
                    # Libera o disco antes de seguir para o proximo membro.
                    _remover_se_existir(csv_local)

                destino_bytes = sink.object_size(destino)
                _emitir(
                    progress_callback,
                    f"{rotulo} {nome_arquivo}: convertido, "
                    f"{_formatar_linhas(linhas)} linhas, "
                    f"{_formatar_bytes(destino_bytes or 0)} enviado",
                )
                resultados.append(
                    ConversaoS3Resultado(
                        dataset=layout.dataset,
                        origem=origem_uri,
                        destino=sink.s3_uri(destino),
                        status=STATUS_OK,
                        linhas=linhas,
                        origem_bytes=origem_bytes,
                        destino_bytes=destino_bytes,
                    )
                )

        _remover_se_existir(zip_local)

    return resultados


def _resultado_se_ja_convertido(
    sink: S3Sink,
    dataset: str,
    origem_uri: str,
    destino: str,
    force: bool,
    rotulo: str,
    nome_arquivo: str,
    linhas_conhecidas: dict[str, int],
    progress_callback: Callable[[str], None] | None,
) -> ConversaoS3Resultado | None:
    """O bucket e a fonte da verdade; o manifest so recupera a contagem de linhas."""
    if force or not sink.object_exists(destino):
        return None

    destino_uri = sink.s3_uri(destino)
    _emitir(progress_callback, f"{rotulo} {nome_arquivo}: ja convertido, pulado")
    return ConversaoS3Resultado(
        dataset=dataset,
        origem=origem_uri,
        destino=destino_uri,
        status=STATUS_PULADO,
        linhas=linhas_conhecidas.get(destino_uri),
        destino_bytes=sink.object_size(destino),
    )


def _converter_para_s3(
    sink: S3Sink,
    conexao,
    origem_uri: str,
    chave_origem: str,
    destino_uri: str,
    layout: CsvLayout,
    nome_arquivo: str,
) -> int:
    """Converte lendo direto do S3; so usa disco no fallback de encoding."""
    try:
        return executar_copy_csv_para_parquet(
            conexao=conexao,
            origem=origem_uri,
            destino=destino_uri,
            layout=layout,
            encoding="latin-1",
            nome_arquivo=nome_arquivo,
        )
    except Exception as error:
        if not _erro_de_encoding_latin1(error):
            raise

    with TemporaryDirectory(prefix="cnpj-parquet-s3-") as temp_dir:
        temp_path = Path(temp_dir)
        csv_latin1 = temp_path / nome_arquivo
        csv_utf8 = temp_path / f"{nome_arquivo}.utf8"
        try:
            sink.download_to(chave_origem, csv_latin1)
            _transcodificar_para_utf8(csv_latin1, csv_utf8)
            _remover_se_existir(csv_latin1)
            return executar_copy_csv_para_parquet(
                conexao=conexao,
                origem=str(csv_utf8),
                destino=destino_uri,
                layout=layout,
                encoding="utf-8",
                nome_arquivo=nome_arquivo,
            )
        finally:
            _remover_se_existir(csv_latin1)
            _remover_se_existir(csv_utf8)


def _converter_local_para_s3(
    conexao,
    csv_local: Path,
    destino_uri: str,
    layout: CsvLayout,
    nome_arquivo: str,
) -> int:
    """Converte um CSV ja em disco (extraido de zip) gravando o Parquet no S3."""
    try:
        return executar_copy_csv_para_parquet(
            conexao=conexao,
            origem=str(csv_local),
            destino=destino_uri,
            layout=layout,
            encoding="latin-1",
            nome_arquivo=nome_arquivo,
        )
    except Exception as error:
        if not _erro_de_encoding_latin1(error):
            raise

    csv_utf8 = csv_local.with_name(f"{csv_local.name}.utf8")
    try:
        _transcodificar_para_utf8(csv_local, csv_utf8)
        return executar_copy_csv_para_parquet(
            conexao=conexao,
            origem=str(csv_utf8),
            destino=destino_uri,
            layout=layout,
            encoding="utf-8",
            nome_arquivo=nome_arquivo,
        )
    finally:
        _remover_se_existir(csv_utf8)


def _abrir_conexao_duckdb(sink: S3Sink):
    try:
        import duckdb
    except ModuleNotFoundError as error:
        raise RuntimeError(
            "A dependencia duckdb nao esta instalada. "
            "Rode `pip install -r requirements.txt`."
        ) from error

    conexao = duckdb.connect(database=":memory:")
    try:
        conexao.execute("install httpfs;")
    except Exception:  # noqa: BLE001 - extensao ja instalada ou sem rede
        pass
    conexao.execute("load httpfs;")

    for nome, valor in sink.duckdb_config().items():
        conexao.execute(f"set {nome} = {_literal_duckdb(valor)};")

    return conexao


def _literal_duckdb(valor: object) -> str:
    if isinstance(valor, bool):
        return "true" if valor else "false"
    return sql_literal(str(valor))


def _prefixo_lakehouse(lakehouse_prefix: str | None) -> str:
    raiz = (lakehouse_prefix or "").strip("/")
    return f"{raiz}/raw/cnpj" if raiz else "raw/cnpj"


def _parquet_key(prefixo: str, dataset: str, month: str, nome_arquivo: str) -> str:
    return f"{prefixo}/{dataset}/ano_mes={month}/{nome_arquivo}.parquet"


def _manifest_key(prefixo: str, month: str) -> str:
    return f"{prefixo}/_manifests/conversao/ano_mes={month}/manifest.json"


def _ler_manifest(sink: S3Sink, prefixo: str, month: str) -> dict:
    conteudo = sink.get_bytes(_manifest_key(prefixo, month))
    if not conteudo:
        return {}

    try:
        payload = json.loads(conteudo.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return {}

    return payload if isinstance(payload, dict) else {}


def _entradas_por_origem(manifest: dict) -> dict[str, dict]:
    entradas: dict[str, dict] = {}
    for item in manifest.get("arquivos", []) or []:
        origem = item.get("origem")
        if origem:
            entradas[origem] = item
    return entradas


def _resultados_preservados(
    entradas: dict[str, dict],
    zip_uri: str,
) -> list[ConversaoS3Resultado]:
    """Reconstroi os registros dos membros de um zip que nao foi reaberto."""
    prefixo_membro = f"{zip_uri}!"
    preservados: list[ConversaoS3Resultado] = []
    for origem, item in entradas.items():
        if not origem.startswith(prefixo_membro):
            continue
        preservados.append(
            ConversaoS3Resultado(
                dataset=item.get("dataset", "desconhecido"),
                origem=origem,
                destino=item.get("destino", ""),
                status=STATUS_PULADO,
                linhas=item.get("linhas"),
                origem_bytes=item.get("origem_bytes"),
                destino_bytes=item.get("destino_bytes"),
            )
        )
    return preservados


def _linhas_por_destino(manifest: dict) -> dict[str, int]:
    linhas: dict[str, int] = {}
    for item in manifest.get("arquivos", []) or []:
        destino = item.get("destino")
        quantidade = item.get("linhas")
        if destino and isinstance(quantidade, int):
            linhas[destino] = quantidade
    return linhas


def _escrever_manifest(
    sink: S3Sink,
    prefixo: str,
    month: str,
    resultados: list[ConversaoS3Resultado],
    zips_concluidos: set[str],
    finalizado: bool,
) -> None:
    agora = datetime.now(UTC).isoformat()
    payload = {
        "ano_mes": month,
        "bucket": sink.bucket_name,
        "prefixo_lakehouse": prefixo,
        "atualizado_em_utc": agora,
        "finalizado_em_utc": agora if finalizado else None,
        "arquivos_ok": sum(item.status == STATUS_OK for item in resultados),
        "arquivos_falha": sum(item.status == STATUS_FALHA for item in resultados),
        "arquivos_pulados": sum(item.status == STATUS_PULADO for item in resultados),
        "linhas_totais": sum(item.linhas or 0 for item in resultados),
        "zips_concluidos": sorted(zips_concluidos),
        "arquivos": [
            {
                "dataset": item.dataset,
                "origem": item.origem,
                "destino": item.destino,
                "status": item.status,
                "linhas": item.linhas,
                "origem_bytes": item.origem_bytes,
                "destino_bytes": item.destino_bytes,
                "erro": item.erro,
            }
            for item in resultados
        ],
    }
    sink.put_bytes(
        _manifest_key(prefixo, month),
        (json.dumps(payload, ensure_ascii=False, indent=2) + "\n").encode("utf-8"),
    )


def _remover_se_existir(caminho: Path) -> None:
    try:
        caminho.unlink()
    except FileNotFoundError:
        return


def _formatar_linhas(valor: int | None) -> str:
    if valor is None:
        return "?"
    if valor >= 1_000_000:
        return f"{valor / 1_000_000:.1f}M"
    if valor >= 1_000:
        return f"{valor / 1_000:.1f}k"
    return str(valor)


def _emitir(
    progress_callback: Callable[[str], None] | None,
    mensagem: str,
) -> None:
    if progress_callback is not None:
        progress_callback(mensagem)
