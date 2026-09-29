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
|   |-- parquet_s3.py  # conversao CSV -> Parquet direto no S3
|   |-- source.py      # listagem e download no WebDAV da Receita
|   |-- sinks.py       # destinos local e S3
|   |-- pipeline.py    # orquestracao da ingestao mensal
|   `-- cli.py         # CLI unica para local, S3 e Parquet
|-- docker/            # imagem da conversao CSV -> Parquet no S3
|-- macros/            # macros dbt para ler Parquet local
|-- models/staging/    # modelos staging iniciais em dbt-duckdb
|-- cnpj_monthly_download.py # wrapper por variavel de ambiente
|-- tests/
|   `-- fixtures/
|       `-- cnpj/
|-- dbt_project.yml
|-- profiles.yml.example
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
python cnpj_monthly_download.py
```

Ingestao para S3:

```bash
python -m cnpj_pipeline.cli s3 --month 2026-08
```

Atalho equivalente:

```bash
CNPJ_DESTINATION=s3
python cnpj_monthly_download.py
```

O wrapper `cnpj_monthly_download.py` usa `CNPJ_DESTINATION=local` por padrao. Para enviar ao S3, defina `CNPJ_DESTINATION=s3` e configure as variaveis `S3_*`.

Ao concluir, a ingestao escreve um manifest JSON em `ingestao-manifest.json` no destino da competencia. Marcadores legados `download-finalizado.txt` ainda sao reconhecidos para compatibilidade com execucoes antigas, mas novas execucoes nao criam esse arquivo.

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

Conversao para Parquet direto no S3:

```bash
python -m cnpj_pipeline.cli parquet-s3 --month 2026-08
```

Esse comando le os arquivos brutos que a ingestao ja enviou ao bucket e grava os Parquets no proprio bucket, sem depender de uma copia local da competencia. Por padrao ele le os CSVs ja extraidos em `raw/cnpj/ano_mes=<YYYY-MM>/unzipped/` e, se nao houver nenhum (ingestao rodada com `--no-extract`), cai para os `.zip` em `raw/cnpj/ano_mes=<YYYY-MM>/zipped/`.

Os Parquets ficam em:

```text
s3://<bucket>/<S3_LAKEHOUSE_PREFIX_ROOT>/raw/cnpj/<entidade>/ano_mes=<YYYY-MM>/<arquivo>.parquet
```

E o manifest da conversao em:

```text
s3://<bucket>/<S3_LAKEHOUSE_PREFIX_ROOT>/raw/cnpj/_manifests/conversao/ano_mes=<YYYY-MM>/manifest.json
```

### Retomada

A conversao e retomavel e pode ser reexecutada a vontade. Antes de converter cada arquivo o comando verifica se o Parquet de destino ja existe no bucket e, se existir, pula o arquivo. O manifest e regravado a cada arquivo, entao uma execucao interrompida no meio nao perde o progresso ja feito. Cada entrada registra `status` (`ok`, `pulado` ou `falha`), `linhas`, `origem_bytes`, `destino_bytes` e `erro`:

```json
{
  "ano_mes": "2026-08",
  "arquivos_ok": 37,
  "arquivos_falha": 0,
  "arquivos_pulados": 2,
  "linhas_totais": 61234567,
  "arquivos": [
    {
      "dataset": "estabelecimentos",
      "origem": "s3://bucket/raw/cnpj/ano_mes=2026-08/unzipped/K3241.K03200Y3.D60808.ESTABELE",
      "destino": "s3://bucket/lakehouse/raw/cnpj/estabelecimentos/ano_mes=2026-08/K3241.K03200Y3.D60808.ESTABELE.parquet",
      "status": "ok",
      "linhas": 4200000,
      "origem_bytes": 2147483648,
      "destino_bytes": 536870912,
      "erro": null
    }
  ]
}
```

Uma falha em um arquivo nao aborta o lote: o erro e registrado no manifest, os demais arquivos seguem sendo convertidos e o comando termina com codigo de saida 1. Use `--force` para reconverter tudo, ignorando o que ja existe no bucket.

O progresso e mostrado arquivo a arquivo no terminal:

```text
Origem: 37 CSVs em s3://bucket/raw/cnpj/ano_mes=2026-08/unzipped/
[3/37] K3241.K03200Y3.D60808.ESTABELE: convertendo (2.0 GB)
[3/37] K3241.K03200Y3.D60808.ESTABELE: convertido, 4.2M linhas, 512.0 MB enviado
[4/37] K3241.K03200Y4.D60808.ESTABELE: ja convertido, pulado
```

### MinIO e outros object storages compativeis

O comando usa a extensao `httpfs` do DuckDB para ler o CSV e gravar o Parquet direto no object storage, sem passar por disco. Disco local so e usado em dois casos, sempre limpo ao fim de cada arquivo: o fallback de encoding quando o CSV nao e latin-1 valido, e a leitura dos `.zip`.

Para MinIO, configure o endpoint, o path-style e o SSL no `.env`:

```text
S3_ENDPOINT_URL=http://localhost:9000
S3_URL_STYLE=path
S3_USE_SSL=false
```

Ou passe direto na linha de comando:

```bash
python -m cnpj_pipeline.cli parquet-s3 \
  --month 2026-08 \
  --bucket meu-bucket \
  --endpoint-url http://localhost:9000 \
  --url-style path \
  --no-ssl
```

Quando `S3_ENDPOINT_URL` esta definido, o path-style ja e o padrao e o SSL segue o esquema do endpoint. As mesmas opcoes valem para o comando `s3` de ingestao.

### Container da conversao

A conversao tambem roda em container, recebendo apenas a competencia. Toda a
configuracao de acesso vem por variavel de ambiente.

```bash
docker build -f docker/Dockerfile -t cnpj-parquet-s3:latest .
```

Crie um arquivo de ambiente (o mesmo conteudo do `.env`, sem aspas):

```text
S3_BUCKET_NAME=meu-bucket
S3_PREFIX_ROOT=raw/cnpj
S3_LAKEHOUSE_PREFIX_ROOT=lakehouse
S3_ENDPOINT_URL=https://meu-endpoint
S3_URL_STYLE=path
S3_USE_SSL=true
AWS_REGION=us-east-1
AWS_ACCESS_KEY_ID=...
AWS_SECRET_ACCESS_KEY=...
```

E rode passando a competencia:

```bash
docker run --rm --env-file s3.env cnpj-parquet-s3:latest 2026-09
```

A competencia tambem pode vir por variavel, sem argumento posicional:

```bash
docker run --rm --env-file s3.env -e CNPJ_TARGET_MONTH=2026-09 cnpj-parquet-s3:latest
```

O progresso sai em tempo real no stdout do container, uma linha por arquivo,
igual a execucao local. Qualquer flag extra do comando e repassada:

```bash
docker run --rm --env-file s3.env cnpj-parquet-s3:latest 2026-09 --force
```

Codigos de saida: `0` sucesso, `1` quando algum arquivo falhou na conversao,
`2` para erro de uso (competencia ausente ou fora do formato `YYYY-MM`,
`S3_BUCKET_NAME` nao definido).

#### Disco e memoria

Lendo a partir dos `.zip`, o container extrai um CSV por vez para o diretorio
temporario e apaga antes do proximo. O pico e o maior par zip + CSV da
competencia, em torno de 10 GB para `Estabelecimentos0`. Monte um volume em
`/tmp` para nao encher a camada de escrita do container:

```bash
docker run --rm --env-file s3.env \
  -v cnpj-tmp:/tmp \
  --memory 4g \
  cnpj-parquet-s3:latest 2026-09
```

Lendo a partir de `unzipped/`, o caminho rapido dispensa disco: o DuckDB le o
CSV direto do object storage e grava o Parquet de volta, sem passar por `/tmp`.

A imagem roda como usuario sem privilegios (uid 10001) e ja traz a extensao
`httpfs` do DuckDB embutida, entao nao baixa extensao em tempo de execucao.

Dominios complementares do Serpro:

```bash
python -m cnpj_pipeline.cli serpro-dominios
```

Esse comando baixa dominios publicos complementares de pessoa juridica do Serpro, como paises e motivos de situacao cadastral, em:

```text
downloads/serpro/dominios/pj/
```

Por padrao, o comando tambem converte os CSVs baixados/reutilizados para Parquet em:

```text
downloads/lakehouse/raw/serpro/dominios_pj/<dataset>/snapshot_id=<snapshot>/<dataset>.parquet
```

Para executar apenas a coleta dos CSVs, use:

```bash
python -m cnpj_pipeline.cli serpro-dominios --no-parquet
```

Essa etapa nao bloqueia a pipeline principal da Receita Federal. Se o Serpro estiver indisponivel e ja existir um arquivo local valido, o comando reutiliza o ultimo snapshot local e registra isso no manifest. Se nao existir snapshot valido, registra a falha e permite que a esteira principal siga apenas com os dominios da Receita.

O manifest da etapa fica em:

```text
downloads/serpro/dominios/pj/_manifests/ultimo-manifest.json
```

O manifest da conversao para Parquet fica em:

```text
downloads/lakehouse/raw/serpro/dominios_pj/_manifests/conversao/snapshot_id=<snapshot>/manifest.json
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

A conversao tambem grava um manifest JSON em `downloads/lakehouse/raw/cnpj/_manifests/conversao/ano_mes=<YYYY-MM>/manifest.json`.

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
8. Apontar os modelos dbt para os Parquets no S3, alem dos locais.
