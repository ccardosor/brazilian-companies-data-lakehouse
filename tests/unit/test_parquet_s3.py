from __future__ import annotations

import io
import json
import re
import unittest
import zipfile
from pathlib import Path

from cnpj_pipeline.parquet_s3 import (
    _formatar_linhas,
    _manifest_key,
    _parquet_key,
    _prefixo_lakehouse,
    converter_csvs_s3_para_parquet,
)
from cnpj_pipeline.sinks import S3Sink

FIXTURES = (
    Path(__file__).resolve().parents[1] / "fixtures" / "cnpj" / "2026-08_unzipped"
)
EMPRESAS = "K3241.K03200Y1.D60808.EMPRECSV"
MOTIVOS = "F.K03200$Z.D60808.MOTICSV"
UNZIPPED = "raw/cnpj/ano_mes=2026-08/unzipped/"
ZIPPED = "raw/cnpj/ano_mes=2026-08/zipped/"
LAKEHOUSE = "lakehouse/raw/cnpj"


class ErroS3Fake(Exception):
    def __init__(self, status_code: int) -> None:
        self.response = {"ResponseMetadata": {"HTTPStatusCode": status_code}}


class ClienteS3Fake:
    """Bucket em memoria com a fatia da API S3 usada pelo sink."""

    def __init__(self, objects: dict[str, bytes] | None = None) -> None:
        self.objects: dict[str, bytes] = dict(objects or {})
        self.downloads: list[str] = []
        self.put_keys: list[str] = []
        self.paginas = 1

    def list_objects_v2(
        self,
        Bucket: str,
        Prefix: str = "",
        ContinuationToken: str | None = None,
    ) -> dict:
        chaves = sorted(chave for chave in self.objects if chave.startswith(Prefix))
        if self.paginas <= 1:
            return {"Contents": [{"Key": chave} for chave in chaves]}

        # Paginacao artificial de um item por pagina.
        inicio = int(ContinuationToken or 0)
        fatia = chaves[inicio : inicio + 1]
        truncado = inicio + 1 < len(chaves)
        return {
            "Contents": [{"Key": chave} for chave in fatia],
            "IsTruncated": truncado,
            "NextContinuationToken": str(inicio + 1),
        }

    def head_object(self, Bucket: str, Key: str) -> dict:
        if Key not in self.objects:
            raise ErroS3Fake(404)
        return {"ContentLength": len(self.objects[Key])}

    def get_object(self, Bucket: str, Key: str) -> dict:
        if Key not in self.objects:
            raise ErroS3Fake(404)
        return {"Body": io.BytesIO(self.objects[Key])}

    def put_object(self, Bucket: str, Key: str, Body: bytes) -> None:
        self.objects[Key] = Body
        self.put_keys.append(Key)

    def download_file(self, Bucket: str, Key: str, Filename: str) -> None:
        if Key not in self.objects:
            raise ErroS3Fake(404)
        self.downloads.append(Key)
        Path(Filename).write_bytes(self.objects[Key])

    def upload_file(self, Filename: str, Bucket: str, Key: str) -> None:
        self.objects[Key] = Path(Filename).read_bytes()

    def head_bucket(self, Bucket: str) -> None:
        return None


class ResultadoFake:
    def __init__(self, linhas: int) -> None:
        self.linhas = linhas

    def fetchone(self) -> tuple[int]:
        return (self.linhas,)


class ConexaoDuckDBFake:
    """Simula o COPY do DuckDB sem depender de duckdb nem de httpfs."""

    PADRAO_ORIGEM = re.compile(r"read_csv\(\s*'((?:[^']|'')*)'")
    PADRAO_DESTINO = re.compile(r"\)\s*to\s*'((?:[^']|'')*)'")
    PADRAO_ARQUIVO = re.compile(r"'((?:[^']|'')*)' as arquivo_origem")
    PADRAO_ENCODING = re.compile(r"encoding = '([^']*)'")

    def __init__(
        self,
        cliente: ClienteS3Fake,
        bucket: str = "bucket",
        linhas: int = 3,
        erros: dict[str, Exception] | None = None,
        erros_permanentes: dict[str, Exception] | None = None,
    ) -> None:
        self.cliente = cliente
        self.bucket = bucket
        self.linhas = linhas
        self.erros = dict(erros or {})
        self.erros_permanentes = dict(erros_permanentes or {})
        self.chamadas: list[dict[str, object]] = []
        self.fechada = False

    def execute(self, sql: str):
        origem = self.PADRAO_ORIGEM.search(sql).group(1)
        destino = self.PADRAO_DESTINO.search(sql).group(1)
        nome_arquivo = self.PADRAO_ARQUIVO.search(sql).group(1)
        encoding = self.PADRAO_ENCODING.search(sql).group(1)

        vizinhos: list[str] = []
        if not origem.startswith("s3://"):
            vizinhos = sorted(item.name for item in Path(origem).parent.iterdir())

        self.chamadas.append(
            {
                "origem": origem,
                "destino": destino,
                "arquivo_origem": nome_arquivo,
                "encoding": encoding,
                "origem_existe": not origem.startswith("s3://")
                and Path(origem).exists(),
                "vizinhos": vizinhos,
            }
        )

        erro_permanente = self.erros_permanentes.get(nome_arquivo)
        if erro_permanente is not None:
            raise erro_permanente

        erro = self.erros.pop(nome_arquivo, None)
        if erro is not None:
            raise erro

        prefixo = f"s3://{self.bucket}/"
        if destino.startswith(prefixo):
            self.cliente.objects[destino[len(prefixo) :]] = b"parquet-fake"

        return ResultadoFake(self.linhas)

    def close(self) -> None:
        self.fechada = True


def criar_sink(cliente: ClienteS3Fake) -> S3Sink:
    return S3Sink("bucket", "raw/cnpj", client=cliente)


def conteudo_fixture(nome: str) -> bytes:
    return (FIXTURES / nome).read_bytes()


def bucket_com_csvs() -> ClienteS3Fake:
    return ClienteS3Fake(
        {
            f"{UNZIPPED}{EMPRESAS}": conteudo_fixture(EMPRESAS),
            f"{UNZIPPED}{MOTIVOS}": conteudo_fixture(MOTIVOS),
            f"{UNZIPPED}leiame.txt": b"fora do layout",
        }
    )


def ler_manifest(cliente: ClienteS3Fake) -> dict:
    return json.loads(cliente.objects[_manifest_key(LAKEHOUSE, "2026-08")])


class ChavesTest(unittest.TestCase):
    def test_monta_caminhos_do_lakehouse_no_bucket(self) -> None:
        prefixo = _prefixo_lakehouse("lakehouse/")

        self.assertEqual(prefixo, "lakehouse/raw/cnpj")
        self.assertEqual(
            _parquet_key(prefixo, "empresas", "2026-08", EMPRESAS),
            f"lakehouse/raw/cnpj/empresas/ano_mes=2026-08/{EMPRESAS}.parquet",
        )
        self.assertEqual(
            _manifest_key(prefixo, "2026-08"),
            "lakehouse/raw/cnpj/_manifests/conversao/ano_mes=2026-08/manifest.json",
        )

    def test_prefixo_vazio_grava_na_raiz_do_bucket(self) -> None:
        self.assertEqual(_prefixo_lakehouse(""), "raw/cnpj")
        self.assertEqual(_prefixo_lakehouse(None), "raw/cnpj")

    def test_formata_quantidade_de_linhas(self) -> None:
        self.assertEqual(_formatar_linhas(4_200_000), "4.2M")
        self.assertEqual(_formatar_linhas(4_200), "4.2k")
        self.assertEqual(_formatar_linhas(42), "42")
        self.assertEqual(_formatar_linhas(None), "?")


class ConversaoCsvTest(unittest.TestCase):
    def test_converte_csvs_do_s3_gravando_parquet_e_manifest(self) -> None:
        cliente = bucket_com_csvs()
        conexao = ConexaoDuckDBFake(cliente, linhas=4_200_000)
        mensagens: list[str] = []

        resultados = converter_csvs_s3_para_parquet(
            sink=criar_sink(cliente),
            month="2026-08",
            progress_callback=mensagens.append,
            conexao=conexao,
        )

        # O arquivo fora do layout e ignorado.
        self.assertEqual(len(resultados), 2)
        self.assertEqual({item.status for item in resultados}, {"ok"})
        self.assertEqual(
            sorted(item.dataset for item in resultados), ["empresas", "motivos"]
        )
        self.assertIn(
            f"lakehouse/raw/cnpj/empresas/ano_mes=2026-08/{EMPRESAS}.parquet",
            cliente.objects,
        )
        self.assertIn(
            f"lakehouse/raw/cnpj/motivos/ano_mes=2026-08/{MOTIVOS}.parquet",
            cliente.objects,
        )

        # A leitura acontece direto no S3, sem baixar nada para disco.
        self.assertEqual(cliente.downloads, [])
        self.assertTrue(
            all(
                str(chamada["origem"]).startswith("s3://bucket/")
                for chamada in conexao.chamadas
            )
        )
        self.assertTrue(conexao.fechada is False)

        manifest = ler_manifest(cliente)
        self.assertEqual(manifest["ano_mes"], "2026-08")
        self.assertEqual(manifest["bucket"], "bucket")
        self.assertEqual(manifest["arquivos_ok"], 2)
        self.assertEqual(manifest["arquivos_falha"], 0)
        self.assertEqual(manifest["linhas_totais"], 8_400_000)
        self.assertIsNotNone(manifest["finalizado_em_utc"])
        entrada = manifest["arquivos"][0]
        self.assertEqual(entrada["status"], "ok")
        self.assertEqual(entrada["linhas"], 4_200_000)
        self.assertEqual(entrada["destino_bytes"], len(b"parquet-fake"))
        self.assertGreater(entrada["origem_bytes"], 0)

        self.assertTrue(
            any("convertido, 4.2M linhas" in mensagem for mensagem in mensagens),
            mensagens,
        )

    def test_pula_arquivo_com_parquet_ja_existente_no_bucket(self) -> None:
        cliente = bucket_com_csvs()
        cliente.objects[
            f"lakehouse/raw/cnpj/empresas/ano_mes=2026-08/{EMPRESAS}.parquet"
        ] = b"parquet-anterior"
        conexao = ConexaoDuckDBFake(cliente)
        mensagens: list[str] = []

        resultados = converter_csvs_s3_para_parquet(
            sink=criar_sink(cliente),
            month="2026-08",
            progress_callback=mensagens.append,
            conexao=conexao,
        )

        por_dataset = {item.dataset: item for item in resultados}
        self.assertEqual(por_dataset["empresas"].status, "pulado")
        self.assertEqual(por_dataset["motivos"].status, "ok")
        # So o arquivo que faltava foi convertido.
        self.assertEqual(len(conexao.chamadas), 1)
        self.assertIn(MOTIVOS, str(conexao.chamadas[0]["origem"]))
        self.assertTrue(
            any(f"{EMPRESAS}: ja convertido, pulado" in item for item in mensagens),
            mensagens,
        )

    def test_recupera_linhas_do_manifest_ao_pular(self) -> None:
        cliente = bucket_com_csvs()
        destino = f"lakehouse/raw/cnpj/empresas/ano_mes=2026-08/{EMPRESAS}.parquet"
        cliente.objects[destino] = b"parquet-anterior"
        cliente.objects[_manifest_key(LAKEHOUSE, "2026-08")] = json.dumps(
            {
                "arquivos": [
                    {
                        "destino": f"s3://bucket/{destino}",
                        "status": "ok",
                        "linhas": 1234,
                    }
                ]
            }
        ).encode("utf-8")

        resultados = converter_csvs_s3_para_parquet(
            sink=criar_sink(cliente),
            month="2026-08",
            conexao=ConexaoDuckDBFake(cliente),
        )

        pulado = next(item for item in resultados if item.status == "pulado")
        self.assertEqual(pulado.linhas, 1234)

    def test_force_reconverte_arquivos_ja_existentes(self) -> None:
        cliente = bucket_com_csvs()
        cliente.objects[
            f"lakehouse/raw/cnpj/empresas/ano_mes=2026-08/{EMPRESAS}.parquet"
        ] = b"parquet-anterior"
        conexao = ConexaoDuckDBFake(cliente)

        resultados = converter_csvs_s3_para_parquet(
            sink=criar_sink(cliente),
            month="2026-08",
            force=True,
            conexao=conexao,
        )

        self.assertEqual({item.status for item in resultados}, {"ok"})
        self.assertEqual(len(conexao.chamadas), 2)

    def test_falha_em_um_arquivo_nao_aborta_o_lote(self) -> None:
        cliente = bucket_com_csvs()
        conexao = ConexaoDuckDBFake(
            cliente,
            erros_permanentes={EMPRESAS: RuntimeError("coluna invalida na linha 10")},
        )
        mensagens: list[str] = []

        resultados = converter_csvs_s3_para_parquet(
            sink=criar_sink(cliente),
            month="2026-08",
            progress_callback=mensagens.append,
            conexao=conexao,
        )

        por_dataset = {item.dataset: item for item in resultados}
        self.assertEqual(por_dataset["empresas"].status, "falha")
        self.assertIn("coluna invalida", por_dataset["empresas"].erro)
        self.assertEqual(por_dataset["motivos"].status, "ok")

        manifest = ler_manifest(cliente)
        self.assertEqual(manifest["arquivos_falha"], 1)
        self.assertEqual(manifest["arquivos_ok"], 1)
        self.assertTrue(any("FALHA" in mensagem for mensagem in mensagens), mensagens)

    def test_grava_manifest_a_cada_arquivo_para_permitir_retomada(self) -> None:
        cliente = bucket_com_csvs()
        chave_manifest = _manifest_key(LAKEHOUSE, "2026-08")

        converter_csvs_s3_para_parquet(
            sink=criar_sink(cliente),
            month="2026-08",
            conexao=ConexaoDuckDBFake(cliente),
        )

        gravacoes = [chave for chave in cliente.put_keys if chave == chave_manifest]
        # Uma gravacao por arquivo mais a gravacao final.
        self.assertEqual(len(gravacoes), 3)

    def test_erro_quando_nao_ha_nada_para_converter(self) -> None:
        cliente = ClienteS3Fake()

        with self.assertRaises(RuntimeError) as contexto:
            converter_csvs_s3_para_parquet(
                sink=criar_sink(cliente),
                month="2026-08",
                conexao=ConexaoDuckDBFake(cliente),
            )

        self.assertIn("Nenhum arquivo conhecido", str(contexto.exception))


class FallbackEncodingTest(unittest.TestCase):
    def test_baixa_transcodifica_e_limpa_temporarios_no_fallback(self) -> None:
        cliente = ClienteS3Fake(
            {f"{UNZIPPED}{EMPRESAS}": conteudo_fixture(EMPRESAS)},
        )
        erro = RuntimeError(
            "Invalid unicode detected, file is not utf-8 nor latin-1 encoded"
        )
        conexao = ConexaoDuckDBFake(cliente, erros={EMPRESAS: erro})

        resultados = converter_csvs_s3_para_parquet(
            sink=criar_sink(cliente),
            month="2026-08",
            conexao=conexao,
        )

        self.assertEqual(resultados[0].status, "ok")
        self.assertEqual(len(conexao.chamadas), 2)

        primeira, segunda = conexao.chamadas
        self.assertEqual(primeira["encoding"], "latin-1")
        self.assertTrue(str(primeira["origem"]).startswith("s3://"))

        # A segunda tentativa le o arquivo transcodificado em disco.
        self.assertEqual(segunda["encoding"], "utf-8")
        self.assertTrue(str(segunda["origem"]).endswith(".utf8"))
        self.assertTrue(segunda["origem_existe"])
        # O CSV latin-1 original ja foi apagado antes da segunda tentativa.
        self.assertEqual(segunda["vizinhos"], [f"{EMPRESAS}.utf8"])
        self.assertEqual(cliente.downloads, [f"{UNZIPPED}{EMPRESAS}"])

        # Nada sobra em disco depois da conversao.
        self.assertFalse(Path(str(segunda["origem"])).exists())
        self.assertFalse(Path(str(segunda["origem"])).parent.exists())

        # O nome de origem gravado no Parquet e o do arquivo real, nao o temporario.
        self.assertEqual(segunda["arquivo_origem"], EMPRESAS)

    def test_erro_que_nao_e_de_encoding_nao_baixa_o_arquivo(self) -> None:
        cliente = ClienteS3Fake({f"{UNZIPPED}{EMPRESAS}": conteudo_fixture(EMPRESAS)})
        conexao = ConexaoDuckDBFake(
            cliente,
            erros_permanentes={EMPRESAS: RuntimeError("bucket inacessivel")},
        )

        resultados = converter_csvs_s3_para_parquet(
            sink=criar_sink(cliente),
            month="2026-08",
            conexao=conexao,
        )

        self.assertEqual(resultados[0].status, "falha")
        self.assertEqual(cliente.downloads, [])


def criar_zip_com_fixtures() -> bytes:
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as archive:
        archive.writestr(EMPRESAS, conteudo_fixture(EMPRESAS))
        archive.writestr(MOTIVOS, conteudo_fixture(MOTIVOS))
        archive.writestr("leiame.txt", b"fora do layout")
    return buffer.getvalue()


class FallbackZipTest(unittest.TestCase):
    def test_converte_a_partir_dos_zips_quando_nao_ha_csvs_extraidos(self) -> None:
        cliente = ClienteS3Fake({f"{ZIPPED}Amostra.zip": criar_zip_com_fixtures()})
        conexao = ConexaoDuckDBFake(cliente, linhas=1_500)
        mensagens: list[str] = []

        resultados = converter_csvs_s3_para_parquet(
            sink=criar_sink(cliente),
            month="2026-08",
            progress_callback=mensagens.append,
            conexao=conexao,
        )

        self.assertEqual(len(resultados), 2)
        self.assertEqual({item.status for item in resultados}, {"ok"})
        self.assertEqual(cliente.downloads, [f"{ZIPPED}Amostra.zip"])
        self.assertIn(
            f"lakehouse/raw/cnpj/empresas/ano_mes=2026-08/{EMPRESAS}.parquet",
            cliente.objects,
        )
        self.assertTrue(
            resultados[0].origem.endswith(f"Amostra.zip!{EMPRESAS}"),
            resultados[0].origem,
        )

        # Cada membro e extraido e apagado antes do proximo.
        for chamada in conexao.chamadas:
            self.assertEqual(
                sorted(chamada["vizinhos"]),
                sorted(["Amostra.zip", str(chamada["arquivo_origem"])]),
            )

        # O diretorio temporario inteiro some ao fim do zip.
        diretorio = Path(str(conexao.chamadas[0]["origem"])).parent
        self.assertFalse(diretorio.exists())

        manifest = ler_manifest(cliente)
        self.assertEqual(manifest["zips_concluidos"], [f"{ZIPPED}Amostra.zip"])
        self.assertEqual(manifest["linhas_totais"], 3_000)
        self.assertTrue(any("baixando zip" in mensagem for mensagem in mensagens))

    def test_retoma_pulando_zip_ja_concluido_no_manifest(self) -> None:
        cliente = ClienteS3Fake({f"{ZIPPED}Amostra.zip": criar_zip_com_fixtures()})
        destino = f"lakehouse/raw/cnpj/empresas/ano_mes=2026-08/{EMPRESAS}.parquet"
        cliente.objects[_manifest_key(LAKEHOUSE, "2026-08")] = json.dumps(
            {
                "zips_concluidos": [f"{ZIPPED}Amostra.zip"],
                "arquivos": [
                    {
                        "dataset": "empresas",
                        "origem": f"s3://bucket/{ZIPPED}Amostra.zip!{EMPRESAS}",
                        "destino": f"s3://bucket/{destino}",
                        "status": "ok",
                        "linhas": 987,
                        "origem_bytes": 100,
                        "destino_bytes": 50,
                    }
                ],
            }
        ).encode("utf-8")
        conexao = ConexaoDuckDBFake(cliente)
        mensagens: list[str] = []

        resultados = converter_csvs_s3_para_parquet(
            sink=criar_sink(cliente),
            month="2026-08",
            progress_callback=mensagens.append,
            conexao=conexao,
        )

        # O zip nao e reaberto...
        self.assertEqual(cliente.downloads, [])
        self.assertEqual(conexao.chamadas, [])
        self.assertTrue(
            any("ja convertido, pulado" in mensagem for mensagem in mensagens),
            mensagens,
        )

        # ...e o historico por arquivo sobrevive a regravacao do manifest.
        self.assertEqual(len(resultados), 1)
        self.assertEqual(resultados[0].status, "pulado")
        self.assertEqual(resultados[0].linhas, 987)
        manifest = ler_manifest(cliente)
        self.assertEqual(len(manifest["arquivos"]), 1)
        self.assertEqual(manifest["arquivos_pulados"], 1)
        self.assertEqual(manifest["zips_concluidos"], [f"{ZIPPED}Amostra.zip"])

    def test_zip_com_membro_que_falha_nao_e_marcado_como_concluido(self) -> None:
        cliente = ClienteS3Fake({f"{ZIPPED}Amostra.zip": criar_zip_com_fixtures()})
        conexao = ConexaoDuckDBFake(
            cliente,
            erros_permanentes={MOTIVOS: RuntimeError("csv corrompido")},
        )

        resultados = converter_csvs_s3_para_parquet(
            sink=criar_sink(cliente),
            month="2026-08",
            conexao=conexao,
        )

        self.assertEqual(
            {item.dataset: item.status for item in resultados},
            {"empresas": "ok", "motivos": "falha"},
        )
        manifest = ler_manifest(cliente)
        self.assertEqual(manifest["zips_concluidos"], [])


if __name__ == "__main__":
    unittest.main()
