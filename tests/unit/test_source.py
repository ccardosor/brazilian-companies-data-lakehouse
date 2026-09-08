from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from cnpj_pipeline.source import ReceitaCnpjSource


def resposta_webdav(*hrefs: tuple[str, bool, int | None]) -> bytes:
    linhas = ['<?xml version="1.0" encoding="utf-8"?>', '<d:multistatus xmlns:d="DAV:">']
    for href, is_dir, size in hrefs:
        resourcetype = "<d:collection/>" if is_dir else ""
        size_tag = f"<d:getcontentlength>{size}</d:getcontentlength>" if size else ""
        linhas.extend(
            [
                "<d:response>",
                f"<d:href>{href}</d:href>",
                "<d:propstat><d:prop>",
                f"<d:resourcetype>{resourcetype}</d:resourcetype>",
                size_tag,
                "</d:prop></d:propstat>",
                "</d:response>",
            ]
        )
    linhas.append("</d:multistatus>")
    return "\n".join(linhas).encode("utf-8")


class RespostaFake:
    def __init__(self, content: bytes = b"", chunks: list[bytes] | None = None) -> None:
        self.content = content
        self._chunks = chunks or []

    def raise_for_status(self) -> None:
        return None

    def iter_content(self, chunk_size: int):
        yield from self._chunks

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc, traceback) -> None:
        return None


class SessaoFake:
    def __init__(self) -> None:
        self.request_calls = []
        self.get_calls = []

    def request(self, method: str, url: str, **kwargs) -> RespostaFake:
        self.request_calls.append((method, url, kwargs))
        if url.endswith("/2026-08/"):
            return RespostaFake(
                resposta_webdav(
                    ("/public/2026-08/Empresas1.zip", False, 100),
                    ("/public/2026-08/Cnaes.zip", False, 20),
                    ("/public/2026-08/observacao.txt", False, 10),
                    ("/public/2026-08/subpasta/", True, None),
                )
            )

        return RespostaFake(
            resposta_webdav(
                ("/public/2026-08/", True, None),
                ("/public/2026-07/", True, None),
                ("/public/readme.txt", False, 10),
                ("/public/invalido/", True, None),
            )
        )

    def get(self, url: str, **kwargs) -> RespostaFake:
        self.get_calls.append((url, kwargs))
        return RespostaFake(chunks=[b"abc", b"", b"def"])


class ReceitaCnpjSourceTest(unittest.TestCase):
    def test_lista_meses_validos_ordenados(self) -> None:
        source = ReceitaCnpjSource("https://exemplo.local/base", session=SessaoFake())

        self.assertEqual(source.list_months(), ["2026-07", "2026-08"])

    def test_lista_apenas_arquivos_zip_da_competencia(self) -> None:
        source = ReceitaCnpjSource("https://exemplo.local/base", session=SessaoFake())

        arquivos = source.list_zip_files("2026-08")

        self.assertEqual(
            [arquivo.name for arquivo in arquivos], ["Cnaes.zip", "Empresas1.zip"]
        )

    def test_baixa_arquivo_em_partes_para_destino_final(self) -> None:
        session = SessaoFake()
        source = ReceitaCnpjSource("https://exemplo.local/base/", session=session)

        with tempfile.TemporaryDirectory() as temp_dir:
            destino = Path(temp_dir) / "2026-08" / "Empresas0.zip"
            source.download_file("2026-08", "Empresas0.zip", destino)

            self.assertEqual(destino.read_bytes(), b"abcdef")
            self.assertFalse(destino.with_suffix(".zip.part").exists())

        self.assertEqual(
            session.get_calls[0][0],
            "https://exemplo.local/base/2026-08/Empresas0.zip",
        )


if __name__ == "__main__":
    unittest.main()
