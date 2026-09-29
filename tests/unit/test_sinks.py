from __future__ import annotations

import io
import json
import tempfile
import unittest
import zipfile
from pathlib import Path

from cnpj_pipeline.sinks import LocalSink, S3Sink


class ErroS3Fake(Exception):
    def __init__(self, status_code: int) -> None:
        self.response = {"ResponseMetadata": {"HTTPStatusCode": status_code}}


class ClienteS3Fake:
    def __init__(self) -> None:
        self.objects: dict[tuple[str, str], bytes] = {}
        self.head_bucket_calls = []

    def upload_file(self, filename: str, bucket: str, key: str) -> None:
        self.objects[(bucket, key)] = Path(filename).read_bytes()

    def upload_fileobj(self, fileobj, bucket: str, key: str) -> None:
        self.objects[(bucket, key)] = fileobj.read()

    def put_object(self, Bucket: str, Key: str, Body: bytes) -> None:
        self.objects[(Bucket, Key)] = Body

    def head_object(self, Bucket: str, Key: str) -> dict:
        if (Bucket, Key) not in self.objects:
            raise ErroS3Fake(404)
        return {"ContentLength": len(self.objects[(Bucket, Key)])}

    def head_bucket(self, Bucket: str) -> None:
        self.head_bucket_calls.append(Bucket)

    def get_object(self, Bucket: str, Key: str) -> dict:
        if (Bucket, Key) not in self.objects:
            raise ErroS3Fake(404)
        return {"Body": io.BytesIO(self.objects[(Bucket, Key)])}

    def list_objects_v2(
        self,
        Bucket: str,
        Prefix: str = "",
        ContinuationToken: str | None = None,
    ) -> dict:
        chaves = sorted(
            chave
            for bucket, chave in self.objects
            if bucket == Bucket and chave.startswith(Prefix)
        )
        # Devolve uma chave por pagina para exercitar a paginacao.
        inicio = int(ContinuationToken or 0)
        return {
            "Contents": [{"Key": chave} for chave in chaves[inicio : inicio + 1]],
            "IsTruncated": inicio + 1 < len(chaves),
            "NextContinuationToken": str(inicio + 1),
        }


def criar_zip(path: Path) -> None:
    with zipfile.ZipFile(path, "w") as archive:
        archive.writestr("arquivo_a.csv", "a;b\n")
        archive.writestr("pasta/arquivo_b.csv", "c;d\n")


class LocalSinkTest(unittest.TestCase):
    def test_ingere_zip_local_extraindo_conteudo(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base_dir = Path(temp_dir)
            zip_path = base_dir / "2026-08" / "Amostra.zip"
            zip_path.parent.mkdir()
            criar_zip(zip_path)

            sink = LocalSink(base_dir)
            resultado = sink.ingest_zip("2026-08", zip_path)

            self.assertEqual(resultado.zip_location, str(zip_path))
            self.assertEqual(len(resultado.extracted_locations), 2)
            self.assertTrue((base_dir / "2026-08_unzipped" / "arquivo_a.csv").exists())
            self.assertTrue(
                (base_dir / "2026-08_unzipped" / "pasta" / "arquivo_b.csv").exists()
            )

    def test_cria_marcador_de_finalizacao(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            sink = LocalSink(Path(temp_dir))

            self.assertFalse(sink.is_done("2026-08"))
            sink.mark_done("2026-08")

            self.assertTrue(sink.is_done("2026-08"))
            manifest = json.loads(sink.manifest_path("2026-08").read_text())
            self.assertEqual(manifest["ano_mes"], "2026-08")
            self.assertEqual(manifest["destino"], "local")
            self.assertFalse(sink.legacy_done_path("2026-08").exists())


class S3SinkTest(unittest.TestCase):
    def test_monta_chaves_com_prefixo_particionado(self) -> None:
        sink = S3Sink("bucket", "raw/cnpj/", client=ClienteS3Fake())

        self.assertEqual(
            sink.zip_key("2026-08", "Empresas0.zip"),
            "raw/cnpj/ano_mes=2026-08/zipped/Empresas0.zip",
        )
        self.assertEqual(
            sink.extracted_prefix("2026-08"),
            "raw/cnpj/ano_mes=2026-08/unzipped/",
        )
        self.assertEqual(
            sink.manifest_key("2026-08"),
            "raw/cnpj/ano_mes=2026-08/ingestao-manifest.json",
        )

    def test_ingere_zip_para_s3_com_extracao(self) -> None:
        cliente = ClienteS3Fake()
        sink = S3Sink("bucket", "raw/cnpj", client=cliente)

        with tempfile.TemporaryDirectory() as temp_dir:
            zip_path = Path(temp_dir) / "Amostra.zip"
            criar_zip(zip_path)

            resultado = sink.ingest_zip("2026-08", zip_path)

        self.assertEqual(
            resultado.zip_location,
            "s3://bucket/raw/cnpj/ano_mes=2026-08/zipped/Amostra.zip",
        )
        self.assertIn(
            ("bucket", "raw/cnpj/ano_mes=2026-08/unzipped/arquivo_a.csv"),
            cliente.objects,
        )

    def test_verifica_existencia_e_marcador_de_finalizacao(self) -> None:
        cliente = ClienteS3Fake()
        sink = S3Sink("bucket", "raw/cnpj", client=cliente)

        self.assertFalse(sink.has_zip("2026-08", "Empresas0.zip"))
        sink.mark_done("2026-08")

        self.assertTrue(sink.is_done("2026-08"))
        self.assertIn(
            ("bucket", "raw/cnpj/ano_mes=2026-08/ingestao-manifest.json"),
            cliente.objects,
        )
        sink.check_access()
        self.assertEqual(cliente.head_bucket_calls, ["bucket"])


class S3SinkObjectStorageTest(unittest.TestCase):
    def test_lista_chaves_seguindo_a_paginacao(self) -> None:
        cliente = ClienteS3Fake()
        for indice in range(3):
            chave = f"raw/cnpj/unzipped/arquivo_{indice}.csv"
            cliente.objects[("bucket", chave)] = b""
        cliente.objects[("bucket", "raw/cnpj/zipped/Amostra.zip")] = b""

        sink = S3Sink("bucket", "raw/cnpj", client=cliente)

        self.assertEqual(
            sink.list_keys("raw/cnpj/unzipped/"),
            [
                "raw/cnpj/unzipped/arquivo_0.csv",
                "raw/cnpj/unzipped/arquivo_1.csv",
                "raw/cnpj/unzipped/arquivo_2.csv",
            ],
        )

    def test_le_tamanho_e_conteudo_de_objeto(self) -> None:
        cliente = ClienteS3Fake()
        cliente.objects[("bucket", "raw/cnpj/arquivo.csv")] = b"abc"
        sink = S3Sink("bucket", "raw/cnpj", client=cliente)

        self.assertEqual(sink.object_size("raw/cnpj/arquivo.csv"), 3)
        self.assertEqual(sink.get_bytes("raw/cnpj/arquivo.csv"), b"abc")
        self.assertIsNone(sink.object_size("raw/cnpj/ausente.csv"))
        self.assertIsNone(sink.get_bytes("raw/cnpj/ausente.csv"))
        self.assertEqual(
            sink.s3_uri("raw/cnpj/arquivo.csv"), "s3://bucket/raw/cnpj/arquivo.csv"
        )
        self.assertEqual(
            sink.zipped_prefix("2026-08"), "raw/cnpj/ano_mes=2026-08/zipped/"
        )

    def test_configuracao_duckdb_para_minio(self) -> None:
        sink = S3Sink(
            "bucket",
            "raw/cnpj",
            region_name="us-east-1",
            endpoint_url="http://localhost:9000",
            client=ClienteS3Fake(),
        )

        configuracao = sink.duckdb_config()

        self.assertEqual(sink.url_style, "path")
        self.assertFalse(sink.use_ssl)
        self.assertEqual(configuracao["s3_endpoint"], "localhost:9000")
        self.assertEqual(configuracao["s3_url_style"], "path")
        self.assertIs(configuracao["s3_use_ssl"], False)
        self.assertEqual(configuracao["s3_region"], "us-east-1")

    def test_configuracao_duckdb_para_aws_por_padrao(self) -> None:
        sink = S3Sink("bucket", "raw/cnpj", client=ClienteS3Fake())

        configuracao = sink.duckdb_config()

        self.assertEqual(sink.url_style, "vhost")
        self.assertTrue(sink.use_ssl)
        self.assertNotIn("s3_endpoint", configuracao)

    def test_url_style_e_ssl_explicitos_vencem_a_inferencia(self) -> None:
        sink = S3Sink(
            "bucket",
            "raw/cnpj",
            endpoint_url="https://minio.interno:9000",
            url_style="vhost",
            use_ssl=False,
            client=ClienteS3Fake(),
        )

        self.assertEqual(sink.url_style, "vhost")
        self.assertFalse(sink.use_ssl)


if __name__ == "__main__":
    unittest.main()
