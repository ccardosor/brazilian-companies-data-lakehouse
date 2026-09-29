from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from cnpj_pipeline.layouts import identificar_layout
from cnpj_pipeline.parquet import (
    _listar_csvs_com_layout,
    _transcodificar_para_utf8,
    converter_csvs_extraidos_para_parquet,
    executar_copy_csv_para_parquet,
)

try:
    import duckdb
except ModuleNotFoundError:  # pragma: no cover - ambiente sem duckdb
    duckdb = None

FIXTURES = (
    Path(__file__).resolve().parents[1] / "fixtures" / "cnpj" / "2026-08_unzipped"
)


class ParquetTest(unittest.TestCase):
    def test_lista_csvs_com_layout_sem_reidentificar_no_loop_principal(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base_dir = Path(temp_dir)
            (base_dir / "K3241.K03200Y1.D60808.EMPRECSV").write_text(
                '"00000000";"EMPRESA TESTE";"2062";"49";"123,45";"01";""',
                encoding="latin1",
            )
            (base_dir / "ignorado.txt").write_text("fora do layout", encoding="utf-8")

            arquivos = _listar_csvs_com_layout(base_dir)

        self.assertEqual(len(arquivos), 1)
        caminho, layout = arquivos[0]
        self.assertEqual(caminho.name, "K3241.K03200Y1.D60808.EMPRECSV")
        self.assertEqual(layout.dataset, "empresas")

    def test_transcodifica_latin1_para_utf8(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            base_dir = Path(temp_dir)
            origem = base_dir / "origem.csv"
            destino = base_dir / "destino.csv"
            origem.write_bytes('"bairro";"LOTEAMENTO SUMAR\xc9"'.encode("latin-1"))

            _transcodificar_para_utf8(origem, destino)

            self.assertEqual(
                destino.read_text(encoding="utf-8"),
                '"bairro";"LOTEAMENTO SUMAR\u00c9"',
            )


@unittest.skipIf(duckdb is None, "duckdb nao instalado")
class ConversaoComDuckDBTest(unittest.TestCase):
    def test_copy_devolve_a_quantidade_de_linhas_gravadas(self) -> None:
        origem = FIXTURES / "K3241.K03200Y1.D60808.EMPRECSV"
        layout = identificar_layout(origem)
        linhas_no_csv = len(
            [
                linha
                for linha in origem.read_text(encoding="latin-1").splitlines()
                if linha.strip()
            ]
        )

        with tempfile.TemporaryDirectory() as temp_dir:
            destino = Path(temp_dir) / "empresas.parquet"
            conexao = duckdb.connect(database=":memory:")
            try:
                linhas = executar_copy_csv_para_parquet(
                    conexao=conexao,
                    origem=str(origem),
                    destino=str(destino),
                    layout=layout,
                    encoding="latin-1",
                    nome_arquivo=origem.name,
                )
                origens = conexao.execute(
                    f"select distinct arquivo_origem from read_parquet('{destino}')"
                ).fetchall()
            finally:
                conexao.close()

        self.assertEqual(linhas, linhas_no_csv)
        self.assertEqual(origens, [(origem.name,)])

    def test_conversao_local_continua_gerando_parquet_por_dataset(self) -> None:
        with tempfile.TemporaryDirectory() as temp_dir:
            lakehouse = Path(temp_dir) / "lakehouse"
            resultados = converter_csvs_extraidos_para_parquet(
                input_dir=FIXTURES,
                lakehouse_dir=lakehouse,
                month="2026-08",
            )

            self.assertTrue(resultados)
            self.assertTrue(all(not item.skipped for item in resultados))
            self.assertTrue(
                (
                    lakehouse
                    / "raw"
                    / "cnpj"
                    / "empresas"
                    / "ano_mes=2026-08"
                    / "K3241.K03200Y1.D60808.EMPRECSV.parquet"
                ).exists()
            )
            self.assertTrue(
                (
                    lakehouse
                    / "raw"
                    / "cnpj"
                    / "_manifests"
                    / "conversao"
                    / "ano_mes=2026-08"
                    / "manifest.json"
                ).exists()
            )


if __name__ == "__main__":
    unittest.main()
