# Fixtures CNPJ

Esta pasta contem uma amostra pequena e versionavel dos arquivos publicos de CNPJ da Receita Federal.

As fixtures preservam caracteristicas importantes da origem:

- CSV sem cabecalho;
- separador `;`;
- campos entre aspas;
- datas no formato `YYYYMMDD`;
- capital social com virgula decimal;
- strings vazias representadas como campos vazios;
- nomes de arquivos semelhantes aos arquivos oficiais.

Elas devem ser usadas para testes unitarios, testes de conversao e validacoes locais sem depender do download da base completa.

## Casos cobertos

- Empresa de grande porte, microempresa e empresa de pequeno porte.
- Estabelecimento matriz e filial.
- Situacoes cadastrais ativa, baixada, inapta e suspensa.
- CNAE secundario vazio e com multiplos codigos.
- Opcao pelo Simples e pelo MEI.
- Socio pessoa fisica, pessoa juridica e estrangeiro.
- Campos opcionais vazios em endereco, contato, pais e situacao especial.
