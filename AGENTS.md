# AGENTS.md

## Idioma

Use portugues sempre que possivel neste repositorio.

- Prefira nomes, comentarios, documentacao e mensagens de erro em portugues quando isso nao prejudicar compatibilidade tecnica.
- Commits devem ser escritos em portugues sempre que possivel.
- Mantenha nomes tecnicos consagrados em ingles quando forem convencoes da ferramenta ou biblioteca, como `source`, `sink`, `staging`, `snapshot`, `fixture`, `raw`, `bronze`, `dbt` e `Airflow`.

## Dados E Seguranca

- Nao commite `.env`, credenciais, dados brutos completos, arquivos em `downloads/`, zips ou arquivos Parquet gerados localmente.
- Use fixtures pequenas em `tests/fixtures/` para testes versionaveis.
- Preserve o formato original dos arquivos CNPJ nas fixtures: CSV sem cabecalho, separado por `;`, campos entre aspas e encoding compativel com `latin1`.
