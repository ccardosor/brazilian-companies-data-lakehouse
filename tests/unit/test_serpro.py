from __future__ import annotations

import json
import tempfile
import unittest
from pathlib import Path

from cnpj_pipeline.serpro import SerproDomainSource, baixar_dominios_serpro


class RespostaFake:
    def __init__(
        self,
        chunks: list[bytes] | None = None,
        error: Exception | None = None,
    ) -> None:
        self._chunks = chunks or []
        self._error = error

    def raise_for_status(self) -> None:
        if self._error:
            raise self._error

    def iter_content(self, chunk_size: int):
        yield from self._chunks

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        return None


class SessaoSerproFake:
    def __init__(self, responses: dict[str, RespostaFake]) -> None:
        self.responses = responses
        self.urls: list[str] = []

    def get(self, url: str, **kwargs) -> RespostaFake:
        self.urls.append(url)
        response = self.responses.get(url)
        if response is None:
            raise RuntimeError(f"URL inesperada: {url}")
        return response


class SerproDomainTest(unittest.TestCase):
    def test_baixa_dominios_e_grava_manifest(self) -> None:
        responses = {
            "https://bcadastros.serpro.gov.br/documentacao/dominios/pj/pais.csv": (
                RespostaFake(chunks=[b"Codigo;Descricao\n", b"13;AFEGANISTAO\n"])
            ),
            (
                "https://bcadastros.serpro.gov.br/documentacao/dominios/pj/"
                "motivo_situacao_cadastral.csv"
            ): RespostaFake(chunks=[b"Codigo;Descricao\n", b"32;MOTIVO\n"]),
        }
        source = SerproDomainSource(session=SessaoSerproFake(responses))

        with tempfile.TemporaryDirectory() as temp_dir:
            base_dir = Path(temp_dir)
            resultados = baixar_dominios_serpro(base_dir, source=source)

            root = base_dir / "serpro" / "dominios" / "pj"
            self.assertTrue((root / "atual" / "pais.csv").exists())
            self.assertTrue(
                (root / "atual" / "motivo_situacao_cadastral.csv").exists()
            )
            self.assertEqual(
                [item.status for item in resultados],
                ["baixado", "baixado"],
            )

            manifest = json.loads(
                (root / "_manifests" / "ultimo-manifest.json").read_text(
                    encoding="utf-8"
                )
            )
            self.assertFalse(manifest["bloqueia_pipeline_principal"])
            self.assertEqual(len(manifest["arquivos"]), 2)
            self.assertEqual(manifest["arquivos"][0]["linhas_dados"], 1)
            self.assertIsNotNone(manifest["arquivos"][0]["sha256"])

    def test_reutiliza_snapshot_local_quando_download_falha(self) -> None:
        responses = {
            "https://bcadastros.serpro.gov.br/documentacao/dominios/pj/pais.csv": (
                RespostaFake(error=RuntimeError("fora do ar"))
            ),
            (
                "https://bcadastros.serpro.gov.br/documentacao/dominios/pj/"
                "motivo_situacao_cadastral.csv"
            ): RespostaFake(error=RuntimeError("fora do ar")),
        }
        source = SerproDomainSource(session=SessaoSerproFake(responses))

        with tempfile.TemporaryDirectory() as temp_dir:
            base_dir = Path(temp_dir)
            current_dir = base_dir / "serpro" / "dominios" / "pj" / "atual"
            current_dir.mkdir(parents=True)
            (current_dir / "pais.csv").write_text(
                "Codigo;Descricao\n13;AFEGANISTAO\n",
                encoding="utf-8",
            )

            resultados = baixar_dominios_serpro(
                base_dir,
                source=source,
                force=True,
            )

            self.assertEqual(resultados[0].status, "reutilizado")
            self.assertEqual(resultados[1].status, "falhou")
            self.assertIsNotNone(resultados[0].error)
            self.assertIsNone(resultados[1].path)

    def test_nao_baixa_novamente_quando_arquivo_atual_existe_sem_force(self) -> None:
        source = SerproDomainSource(session=SessaoSerproFake({}))

        with tempfile.TemporaryDirectory() as temp_dir:
            base_dir = Path(temp_dir)
            current_dir = base_dir / "serpro" / "dominios" / "pj" / "atual"
            current_dir.mkdir(parents=True)
            (current_dir / "pais.csv").write_text("Codigo;Descricao\n13;A\n")
            (current_dir / "motivo_situacao_cadastral.csv").write_text(
                "Codigo;Descricao\n32;B\n"
            )

            resultados = baixar_dominios_serpro(base_dir, source=source)

            self.assertEqual(
                [item.status for item in resultados],
                ["existente", "existente"],
            )


if __name__ == "__main__":
    unittest.main()
