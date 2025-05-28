Para fazer o projeto, escolhemos utilizar o MongoDB.

# Justificativa da Escolha do Banco
## Forma de armazenamento de arquivos

O MongoDB armazena os dados em formato BSON (uma versão binária do JSON), ideal para dados semi-estruturados e com esquemas dinâmicos. Isso permite armazenar documentos com campos variados sem precisar migrar ou alterar a estrutura da base.

## Linguagem e processamento de consultas

MongoDB utiliza a linguagem MongoDB Query Language (MQL), baseada em JSON, que facilita a manipulação de documentos completos. Ele também oferece o Aggregation Framework, poderoso para transformações, filtros, joins e análises complexas.

## Processamento e controle de transações

MongoDB possui suporte a transações ACID multi-documentos, com boa performance para workloads de leitura/escrita em grande escala. É adequado para aplicações que precisam de consistência eventual e alta disponibilidade.

## Mecanismos de recuperação e segurança

- Recuperação: MongoDB possui replicação automática (replica sets) e journaling para recuperação de falhas.

- Segurança: Suporta autenticação via SCRAM, TLS/SSL, controle de acesso baseado em funções (RBAC), auditoria e criptografia em repouso.

## Escalabilidade e desempenho

MongoDB é projetado para escalabilidade horizontal com particionamento automático (sharding), além de replicação nativa para tolerância a falhas e balanceamento de carga. Ideal para APIs com acesso de alta concorrência.

# Modelo lógico

# Modelo físico

# Consultas

