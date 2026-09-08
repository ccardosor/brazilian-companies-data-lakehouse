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
|   |-- layouts.py     # layouts dos CSVs oficiais da Receita
|   |-- parquet.py     # conversao local CSV -> Parquet
|   |-- source.py      # listagem e download no WebDAV da Receita
|   |-- sinks.py       # destinos local e S3
|   |-- pipeline.py    # orquestracao da ingestao mensal
|   `-- cli.py         # CLI unica para local ou S3
|-- macros/            # macros dbt para ler Parquet local
|-- models/staging/    # modelos staging iniciais em dbt-duckdb
|-- cnpj_monthly_download_local.py
|-- cnpj_monthly_download.py
|-- tests/
|   `-- fixtures/
|       `-- cnpj/
|-- dbt_project.yml
|-- profiles.yml.example
|-- docs/
|   `-- cnpj_validation_2026_08.md
|-- AGENTS.md
|-- .env.example
|-- .gitignore
`-- requirements.txt
```

## Como rodar

Instale as dependencias:

```bash
pip install -r requirements.txt
```

Para desenvolvimento local, instale tambem as dependencias opcionais:

```bash
pip install -e ".[dev]"
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

Conversao local dos CSVs extraidos para Parquet:

```bash
python -m cnpj_pipeline.cli parquet-local --month 2026-08
```

Por padrao, o comando le `downloads/2026-08_unzipped` e grava em:

```text
downloads/lakehouse/raw/cnpj/<entidade>/ano_mes=2026-08/<arquivo>.parquet
```

Tambem e possivel informar os caminhos explicitamente:

```bash
python -m cnpj_pipeline.cli parquet-local \
  --month 2026-08 \
  --input-dir ./downloads/2026-08_unzipped \
  --lakehouse-dir ./downloads/lakehouse
```

## dbt local com DuckDB

Copie o exemplo de profile para o diretorio esperado pelo dbt ou informe o caminho na execucao:

```bash
mkdir -p ~/.dbt
cp profiles.yml.example ~/.dbt/profiles.yml
```

Depois de converter os CSVs para Parquet, rode:

```bash
dbt run
dbt test
```

Os modelos staging leem os Parquets diretamente de `downloads/lakehouse/raw/cnpj`, usando particoes Hive `ano_mes=<YYYY-MM>`.

## Convencoes de trabalho

Use portugues sempre que possivel no projeto, incluindo codigo, comentarios, documentacao e mensagens de commit. Mantenha termos tecnicos em ingles quando forem convencoes consolidadas da ferramenta ou biblioteca, como `source`, `sink`, `staging`, `snapshot`, `fixture`, `raw`, `bronze`, `dbt` e `Airflow`.

As mensagens de commit devem seguir Conventional Commits com escopo explicito:

```text
tipo(escopo): descricao
```

Exemplos:

```text
testes(fixtures): adiciona amostras sinteticas de cnpj
infra(empacotamento): configura pyproject
refactor(ingestao): injeta dependencias nos clientes externos
```

## Desenvolvimento

Rodar testes unitarios:

```bash
python -m unittest discover
```

Rodar testes com pytest, quando as dependencias de desenvolvimento estiverem instaladas:

```bash
pytest
```

Rodar lint com Ruff:

```bash
ruff check .
```

## Validacao inicial

O relatorio exploratorio de validacao local fica fora do Git publico. A pasta `docs/` esta ignorada neste repositorio porque pode conter analises locais, outputs exploratorios e detalhes de contexto que nao precisam ir para o portfolio publico.

Principais conclusoes:

- os layouts batem com o dicionario oficial;
- a base de 2026-08 tem dezenas de milhoes de registros nas entidades principais;
- ha uma copia duplicada do arquivo de CNAE na pasta extraida localmente;
- os arquivos de Empresas sao shards por faixa de `cnpj_basico`, nao um arquivo geral mais particoes incrementais.

## Proximas etapas

1. Rodar a conversao completa da competencia 2026-08 para Parquet local.
2. Executar `dbt run` e `dbt test` sobre a base local.
3. Expandir os testes dbt de chaves, dominios e relacionamentos.
4. Construir marts dimensionais.
5. Criar snapshots SCD Tipo 2 depois de carregar pelo menos duas competencias.
6. Criar DAGs no Airflow para download, validacao, carga raw, transformacao dbt e publicacao de artefatos.
7. Decidir depois a estrategia de hospedagem em nuvem e object storage.
