# Validacao inicial da base CNPJ - 2026-08

## Escopo

Arquivos avaliados:

- Dados extraidos: `downloads/2026-08_unzipped/`
- Dicionario oficial: `downloads/cnpj-metadados.pdf`
- Saidas geradas:
  - `downloads/validation_2026_08.tsv`
  - `downloads/profile_sample_2026_08.txt`

## Resultado executivo

A base esta apta para seguir para uma primeira ingestao raw/staging.

Os arquivos estao em CSV sem cabecalho, separados por ponto e virgula, com campos entre aspas e encoding compativel com `latin1`. A quantidade de campos da amostra validada bate com o dicionario oficial para Empresas, Estabelecimentos, Socios, Simples Nacional e tabelas de dominio.

Unico ponto de atencao imediato: existe uma copia duplicada do arquivo de CNAEs em `downloads/2026-08_unzipped/`. Os hashes SHA256 de `F.K03200$Z.D60808.CNAECSV` e `F.K03200$Z.D60808 - Copia.CNAECSV` sao iguais, entao apenas um deve entrar no pipeline.

## Volumetria

| Entidade | Arquivos | Linhas | Tamanho |
|---|---:|---:|---:|
| Empresas | 10 | 69.523.304 | 5,058 GB |
| Estabelecimentos | 10 | 72.789.645 | 15,811 GB |
| Socios | 10 | 28.146.721 | 2,682 GB |
| Simples Nacional | 1 | 49.872.124 | 2,926 GB |
| CNAEs | 2 | 2.718 | ~0 GB |
| Motivos | 1 | 63 | ~0 GB |
| Municipios | 1 | 5.572 | ~0 GB |
| Naturezas Juridicas | 1 | 91 | ~0 GB |
| Paises | 1 | 255 | ~0 GB |
| Qualificacoes | 1 | 68 | ~0 GB |

Observacao: CNAEs aparece com 2 arquivos porque ha uma copia duplicada. O total correto para carga deve considerar 1.359 linhas.

## Aderencia ao dicionario

Campos esperados por arquivo:

- Empresas: 7 campos
- Estabelecimentos: 30 campos
- Socios: 11 campos
- Simples Nacional: 7 campos
- Dominios: 2 campos

Na validacao dos primeiros 10.000 registros de cada arquivo grande e de todos os registros dos dominios pequenos, nenhuma divergencia de quantidade de campos foi encontrada.

## Perfil rapido por amostragem

Amostra usada:

- Empresas: 1.000.000 linhas
- Estabelecimentos: 1.000.000 linhas
- Socios: 1.000.000 linhas
- Simples Nacional: 100.000 linhas

Achados:

- Porte das empresas: predominio de `01` (microempresa), seguido de `05` (demais) e `03` (empresa de pequeno porte).
- Natureza juridica: predominio de `2135` e `2062`, bons candidatos para joins com a dimensao de naturezas juridicas.
- Situacao cadastral dos estabelecimentos: aparecem `02`, `08`, `04`, `03` e `01`, aderentes ao dicionario.
- UF dos estabelecimentos: SP, MG, RJ, RS e PR lideram a amostra, distribuicao plausivel.
- CNAE principal: nenhum valor vazio na amostra de estabelecimentos.
- Socios: predominio de socio pessoa fisica (`2`), com presenca menor de pessoa juridica (`1`) e estrangeiro (`3`).

## Modelo analitico recomendado

Grao principal recomendado:

- `fct_estabelecimento_snapshot_mensal`: um registro por CNPJ completo por competencia de carga.

Dimensoes:

- `dim_empresa`: atributos do CNPJ basico, razao social, natureza juridica, porte, capital social e ente federativo responsavel.
- `dim_estabelecimento`: CNPJ completo, matriz/filial, nome fantasia, endereco, UF, municipio, CNAE principal e atributos cadastrais.
- `dim_cnae`: codigo e descricao do CNAE.
- `dim_municipio`: codigo e municipio.
- `dim_natureza_juridica`: codigo e descricao.
- `dim_qualificacao_socio`: codigo e descricao.
- `dim_socio`: socio por empresa, com identificador, qualificacao, pais, faixa etaria e datas.
- `dim_simples`: opcao pelo Simples e MEI, com datas de opcao/exclusao.

Fatos/marts uteis para portfolio:

- `mart_empresas_por_uf_cnae`: contagem de estabelecimentos ativos por UF, municipio e CNAE.
- `mart_aberturas_baixas_mensais`: aberturas e baixas por mes, UF e setor.
- `mart_perfil_societario`: quantidade de socios por empresa, tipo de socio, qualificacao e faixa etaria.
- `mart_simples_mei`: empresas optantes pelo Simples/MEI por UF, CNAE e porte.

## SCD Tipo 2

O uso de `dbt snapshots` faz mais sentido depois de carregar pelo menos duas competencias mensais.

Chaves naturais:

- Empresas: `cnpj_basico`
- Estabelecimentos: `cnpj_basico || cnpj_ordem || cnpj_dv`
- Simples: `cnpj_basico`
- Socios: combinar `cnpj_basico`, identificador do socio, documento mascarado, nome e data de entrada na sociedade.

Campos bons para snapshot:

- Estabelecimentos: situacao cadastral, data situacao cadastral, motivo, endereco, municipio, UF, CNAE principal, CNAEs secundarios e situacao especial.
- Empresas: razao social, natureza juridica, qualificacao responsavel, capital social, porte e ente federativo responsavel.
- Simples: opcao Simples/MEI e respectivas datas.

## Proximos passos

1. Criar camada raw mantendo os arquivos originais por competencia em storage: `raw/cnpj/ano_mes=2026-08/tabela=...`.
2. Converter CSV para Parquet particionado, removendo duplicatas operacionais como a copia de CNAE.
3. Criar staging dbt com nomes de colunas oficiais, casts de datas `YYYYMMDD`, decimal brasileiro para capital social e normalizacao de strings vazias para `null`.
4. Criar testes dbt: quantidade de colunas, `not_null` em chaves, unicidade por chave natural, accepted values para situacao cadastral, matriz/filial, porte, opcao Simples/MEI e relacionamentos com dominios.
5. Orquestrar no Airflow: listar competencia disponivel, baixar zips, validar hashes/tamanhos, extrair, carregar raw, converter para Parquet, executar dbt build e publicar artefatos.
6. Subir para nuvem com um desenho simples e demonstravel: S3/GCS/ADLS como data lake, BigQuery/Snowflake/Redshift/Postgres como warehouse e dbt Cloud ou dbt Core no Airflow.
7. Carregar uma segunda competencia mensal para demonstrar snapshots SCD Tipo 2 de verdade.

## Consultas SQL para demonstracao

- Empresas ativas por UF e secao CNAE.
- Ranking de municipios por abertura de empresas nos ultimos 12 meses.
- Taxa de baixa por natureza juridica e porte.
- Empresas com maior quantidade de filiais.
- Distribuicao de socios por faixa etaria e qualificacao.
- Comparativo Simples/MEI por setor e UF.
