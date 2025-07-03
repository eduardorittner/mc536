Para fazer o projeto, escolhemos utilizar o MongoDB.

# Como rodar

O script `process_data.py` processa os datasets originais (em .csv) e armazena-os como documentos .json, que são então importados para o banco (no mesmo script).
Para executar as queries, rode o script `queries.py`.

## Dependências

As dependências para o script python estão especificadas tanto no `pyproject.toml` quanto no `requirements.txt` e são `csv`, `json` e `pymongo`.

# Justificativa da Escolha do Banco

O MongoDB tem suporte para [replicação](https://www.mongodb.com/docs/manual/replication/), [particionamento](https://www.mongodb.com/docs/manual/sharding/) e [balanceamento de carga](https://www.mongodb.com/docs/manual/core/sharding-balancer-administration/). Além disso, ele tem uma garantia de alta tolerância à falhas através de [eleições](https://www.mongodb.com/resources/basics/high-availability#high-availability-in-mongodb), o que significa que mesmo se a instância primária do banco cair, as outras instâncias são capazes de se organizar e "eleger" uma nova instância primária. Além disso, através de [read concerns](https://www.mongodb.com/docs/manual/reference/read-concern/) e [write concerns](https://www.mongodb.com/docs/manual/reference/write-concern/) a garantia de operações read e write podem ser configuradas para se obter garantias mais fortes, ou operações mais rápidas, dependendo das necessidades do projeto.

## Forma de armazenamento de arquivos

O MongoDB armazena os dados em formato BSON (uma versão binária do JSON), ideal para dados semi-estruturados e com esquemas dinâmicos. Isso permite armazenar documentos com campos variados sem precisar migrar ou alterar a estrutura da base.

## Linguagem e processamento de consultas

MongoDB utiliza a linguagem MongoDB Query Language (MQL), baseada em JSON, que facilita a manipulação de documentos completos, e oferece funcionalidade muito completas. Ele também oferece o Aggregation Framework, poderoso para transformações, filtros, joins e análises complexas.

## Processamento e controle de transações

MongoDB possui suporte a transações ACID multi-documentos, com boa performance para workloads de leitura/escrita em grande escala. É adequado para aplicações que precisam de consistência eventual e alta disponibilidade.

## Mecanismos de recuperação e segurança

- Recuperação: MongoDB possui replicação automática (replica sets), tolerância e falha da instância primária e journaling para recuperação de falhas.

- Segurança: Suporta autenticação via SCRAM, TLS/SSL, controle de acesso baseado em funções (RBAC), auditoria e criptografia em repouso.

## Escalabilidade e desempenho

MongoDB é projetado para escalabilidade horizontal com particionamento automático (sharding), além de replicação nativa para tolerância a falhas e balanceamento de carga. Ideal para APIs com acesso de alta concorrência.

# Modelo lógico

/home/c-ec2022/ra244185/Pictures/Lógico

# Modelo físico

# Consultas

## Rank countries by education increase

Essa consulta visa ordenar os países com o maior aumento percentual da métrica de educação fornecida, conjuntamente com as métricas de energia fornecidas. Somente países com pelo menos uma das métricas de energia presentes serão mostrados, por isso essa query retorna mais resultados quando mais de uma métrica de energia é fornecida.

Parâmetros:

- `db`: Conexão ativa ao banco de dados
- `energy_collection_name`: Nome da coleção de energia no banco
- `education_collection_name`: Nome da coleção de educação no banco
- `education_metric`: Métrica de educação utilizada para ordenar os países
- `start_year`: Início do período a ser considerado
- `end_year`: Fim do período a ser considerado
- `energy_metrics`: Métricas de energia para visualizar
- `n_matches`: Número máximo de países a serem mostrados

## Sort countries by energy and education

Essa consulta ordena países pelo consumo total de energia no ano especificado e exibe dados educacionais associados.

Parâmetros:

- `db`: Conexão ativa ao banco de dados
- `energy_collection_name`: Nome da coleção de energia no banco
- `education_collection_name`: Nome da coleção de educação no banco
- `year`: Ano da consulta
- `matches`: Número máximo de países a serem mostrados
