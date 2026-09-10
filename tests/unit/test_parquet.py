from __future__ import annotations

import tempfile
import unittest
from pathlib import Path

from cnpj_pipeline.parquet import _listar_csvs_com_layout, _transcodificar_para_utf8


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


if __name__ == "__main__":
    unittest.main()
