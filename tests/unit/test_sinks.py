from __future__ import annotations

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

    def head_object(self, Bucket: str, Key: str) -> None:
        if (Bucket, Key) not in self.objects:
            raise ErroS3Fake(404)

    def head_bucket(self, Bucket: str) -> None:
        self.head_bucket_calls.append(Bucket)


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
        sink.check_access()
        self.assertEqual(cliente.head_bucket_calls, ["bucket"])


if __name__ == "__main__":
    unittest.main()
