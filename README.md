# Brazilian Companies Data Lakehouse

Projeto de engenharia de dados usando os dados publicos de CNPJ da Receita Federal.

## Objetivo do projeto

Construir um lakehouse analitico de empresas brasileiras a partir dos arquivos mensais de CNPJ, demonstrando ingestao em lote, modelagem dimensional, qualidade de dados, transformacoes SQL com dbt, orquestracao com Airflow e armazenamento em nuvem.

O produto final recomendado nao e apenas "uma base tratada". A entrega mais forte para portfolio e uma plataforma analitica com:

- dados brutos versionados por competencia mensal;
- tabelas staging limpas e tipadas;
- modelo dimensional para consulta;
- snapshots historicos para acompanhar mudancas cadastrais;
- marts prontos para perguntas de negocio;
- consultas SQL e, opcionalmente, um dashboard.

Exemplos de perguntas que o projeto deve responder:

- Quantas empresas ativas existem por UF, municipio e CNAE?
- Quais municipios mais abriram empresas nos ultimos meses?
- Quais setores concentram empresas MEI ou optantes pelo Simples?
- Quais empresas possuem mais filiais?
- Como a situacao cadastral muda entre competencias mensais?

## Dados

Fonte: dados abertos de CNPJ da Receita Federal.

Arquivos principais:

- Empresas
- Estabelecimentos
- Socios
- Simples Nacional
- CNAEs
- Motivos de situacao cadastral
- Municipios
- Naturezas juridicas
- Paises
- Qualificacoes de socios

Os arquivos originais sao CSV sem cabecalho, separados por `;`, com campos entre aspas e encoding compativel com `latin1`.

## Estrutura atual

```text
.
|-- cnpj_pipeline/
|   |-- source.py      # listagem e download no WebDAV da Receita
|   |-- sinks.py       # destinos local e S3
|   |-- pipeline.py    # orquestracao da ingestao mensal
|   `-- cli.py         # CLI unica para local ou S3
|-- cnpj_monthly_download_local.py
|-- cnpj_monthly_download.py
|-- validate_cnpj_extract.py
|-- profile_cnpj_sample.py
|-- inspect_partitions.py
|-- docs/
|   `-- cnpj_validation_2026_08.md
|-- .env.example
|-- .gitignore
`-- requirements.txt
```

## Como rodar

Instale as dependencias:

```bash
pip install -r requirements.txt
```

Copie `.env.example` para `.env` e ajuste as variaveis.

Ingestao local:

```bash
python -m cnpj_pipeline.cli local --month 2026-08
```

Atalho equivalente:

```bash
python cnpj_monthly_download_local.py
```

Ingestao para S3:

```bash
python -m cnpj_pipeline.cli s3 --month 2026-08
```

Atalho equivalente:

```bash
python cnpj_monthly_download.py
```

## Validacao inicial

O relatorio da validacao local esta em `docs/cnpj_validation_2026_08.md`.

Principais conclusoes:

- os layouts batem com o dicionario oficial;
- a base de 2026-08 tem dezenas de milhoes de registros nas entidades principais;
- ha uma copia duplicada do arquivo de CNAE na pasta extraida localmente;
- os arquivos de Empresas sao shards por faixa de `cnpj_basico`, nao um arquivo geral mais particoes incrementais.

## Proximas etapas

1. Converter a camada raw CSV para Parquet particionado.
2. Criar projeto dbt com sources, staging e marts.
3. Adicionar testes dbt de chaves, dominios, relacionamentos e accepted values.
4. Criar snapshots SCD Tipo 2 depois de carregar pelo menos duas competencias.
5. Criar DAGs no Airflow para download, validacao, carga raw, transformacao dbt e publicacao de artefatos.
6. Subir a arquitetura para nuvem usando S3/GCS/ADLS e um warehouse analitico.
