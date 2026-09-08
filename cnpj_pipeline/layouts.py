from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class CsvLayout:
    dataset: str
    tokens_arquivo: tuple[str, ...]
    colunas: tuple[str, ...]


LAYOUTS: tuple[CsvLayout, ...] = (
    CsvLayout(
        dataset="empresas",
        tokens_arquivo=("EMPRECSV",),
        colunas=(
            "cnpj_basico",
            "razao_social",
            "natureza_juridica",
            "qualificacao_responsavel",
            "capital_social",
            "porte_empresa",
            "ente_federativo_responsavel",
        ),
    ),
    CsvLayout(
        dataset="estabelecimentos",
        tokens_arquivo=("ESTABELE",),
        colunas=(
            "cnpj_basico",
            "cnpj_ordem",
            "cnpj_dv",
            "identificador_matriz_filial",
            "nome_fantasia",
            "situacao_cadastral",
            "data_situacao_cadastral",
            "motivo_situacao_cadastral",
            "nome_cidade_exterior",
            "pais",
            "data_inicio_atividade",
            "cnae_fiscal_principal",
            "cnae_fiscal_secundaria",
            "tipo_logradouro",
            "logradouro",
            "numero",
            "complemento",
            "bairro",
            "cep",
            "uf",
            "municipio",
            "ddd_1",
            "telefone_1",
            "ddd_2",
            "telefone_2",
            "ddd_fax",
            "fax",
            "correio_eletronico",
            "situacao_especial",
            "data_situacao_especial",
        ),
    ),
    CsvLayout(
        dataset="simples",
        tokens_arquivo=("SIMPLES",),
        colunas=(
            "cnpj_basico",
            "opcao_simples",
            "data_opcao_simples",
            "data_exclusao_simples",
            "opcao_mei",
            "data_opcao_mei",
            "data_exclusao_mei",
        ),
    ),
    CsvLayout(
        dataset="socios",
        tokens_arquivo=("SOCIOCSV",),
        colunas=(
            "cnpj_basico",
            "identificador_socio",
            "nome_socio_razao_social",
            "cnpj_cpf_socio",
            "qualificacao_socio",
            "data_entrada_sociedade",
            "pais",
            "representante_legal",
            "nome_representante_legal",
            "qualificacao_representante_legal",
            "faixa_etaria",
        ),
    ),
    CsvLayout(
        dataset="paises",
        tokens_arquivo=("PAISCSV",),
        colunas=("codigo_pais", "descricao_pais"),
    ),
    CsvLayout(
        dataset="municipios",
        tokens_arquivo=("MUNICCSV",),
        colunas=("codigo_municipio", "descricao_municipio"),
    ),
    CsvLayout(
        dataset="qualificacoes",
        tokens_arquivo=("QUALSCSV",),
        colunas=("codigo_qualificacao", "descricao_qualificacao"),
    ),
    CsvLayout(
        dataset="naturezas_juridicas",
        tokens_arquivo=("NATJUCSV",),
        colunas=("codigo_natureza_juridica", "descricao_natureza_juridica"),
    ),
    CsvLayout(
        dataset="cnaes",
        tokens_arquivo=("CNAECSV",),
        colunas=("codigo_cnae", "descricao_cnae"),
    ),
    CsvLayout(
        dataset="motivos",
        tokens_arquivo=("MOTICSV",),
        colunas=("codigo_motivo", "descricao_motivo"),
    ),
)


def identificar_layout(caminho: Path) -> CsvLayout | None:
    nome = caminho.name.upper()
    for layout in LAYOUTS:
        if any(token in nome for token in layout.tokens_arquivo):
            return layout
    return None
