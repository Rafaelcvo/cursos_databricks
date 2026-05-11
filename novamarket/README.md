# NovaMarket Lakehouse Platform

## Visão Geral

Este projeto simula a construção de uma plataforma moderna de engenharia de dados utilizando Databricks Community Edition para uma empresa fictícia de varejo omnichannel.

O objetivo é desenvolver um ambiente próximo de cenários enterprise reais, explorando arquitetura Lakehouse, Delta Lake, Spark avançado, pipelines batch e streaming, otimização de performance, observabilidade e governança.

O projeto foi desenhado para estudo prático profundo de Databricks e engenharia de dados moderna.

---

# Cenário de Negócio

A empresa fictícia NovaMarket atua no segmento de varejo omnichannel.

## Canais de Venda

* E-commerce
* Aplicativo Mobile
* Lojas físicas
* Marketplace

## Áreas Integradas

* ERP
* CRM
* Logística
* Gateway de pagamento
* Antifraude
* Fidelidade

---

# Problemas Atuais da Empresa

A empresa cresceu rapidamente e começou a enfrentar problemas estruturais relacionados a dados.

## Principais Problemas

### Relatórios inconsistentes

Diferentes áreas apresentam métricas conflitantes.

### Lentidão operacional

Pipelines demoram várias horas para concluir.

### Falta de dados em tempo real

Fraudes e rupturas são detectadas tarde.

### Custos elevados

Processamentos redundantes e pipelines ineficientes.

### Ausência de governança

Não existe controle claro de:

* origem dos dados
* qualidade
* lineage
* ownership
* versionamento

---

# Objetivos da Plataforma

Construir uma arquitetura Lakehouse moderna utilizando Databricks.

## Objetivos Técnicos

* Centralizar os dados
* Padronizar regras de negócio
* Criar pipelines escaláveis
* Permitir processamento batch e streaming
* Garantir confiabilidade dos dados
* Melhorar performance
* Criar camada analítica confiável
* Permitir reprocessamento e auditoria

---

# Arquitetura da Solução

```text
Fontes -> Bronze -> Silver -> Gold -> Analytics
```

---

# Arquitetura Medallion

## Bronze Layer

Camada responsável por armazenar os dados brutos exatamente como foram recebidos.

### Características

* Append only
* Sem transformação de negócio
* Reprocessável
* Auditável
* Controle de ingestão

### Objetivos

* Preservar dado original
* Permitir replay
* Garantir rastreabilidade

### Conceitos Estudados

* Ingestão incremental
* Schema evolution
* Checkpoints
* Metadata columns
* Auto Loader conceitual
* Streaming ingestion

---

## Silver Layer

Camada responsável pela padronização e confiabilidade dos dados.

### Processos

* Deduplicação
* Normalização
* CDC
* MERGE INTO
* SCD Type 1
* SCD Type 2
* Data Quality
* Regras de negócio

### Objetivos

* Garantir consistência
* Criar visão confiável
* Consolidar entidades de negócio

### Conceitos Estudados

* Window Functions
* Merge incremental
* Change Data Capture
* Slowly Changing Dimensions
* Idempotência
* Data validation

---

## Gold Layer

Camada analítica utilizada para BI, métricas e dashboards.

### Entregas

* KPIs executivos
* Data marts
* Métricas operacionais
* Indicadores financeiros
* Indicadores logísticos

### KPIs Exemplos

#### Financeiro

* GMV
* Receita líquida
* Ticket médio
* Margem

#### Operacional

* SLA de entrega
* Taxa de ruptura
* Tempo médio de separação

#### Marketing

* CAC
* Retenção
* Conversão

---

# Fontes de Dados

## ERP

### Dados

* Produtos
* Estoque
* Fornecedores
* Lojas

### Tipo

Batch CSV

### Problemas Simulados

* Arquivos duplicados
* Schema drift
* Dados atrasados

---

## E-commerce

### Dados

* Pedidos
* Carrinho
* Navegação
* Pagamentos

### Tipo

Streaming + CDC

### Problemas Simulados

* Eventos duplicados
* Eventos fora de ordem
* Retries

---

## CRM

### Dados

* Leads
* Campanhas
* Segmentação

---

## Logística

### Dados

* Entregas
* Tracking
* SLA

---

## Gateway de Pagamento

### Dados

* Autorizações
* Antifraude
* Chargeback
* Fonte: https://www.kaggle.com/datasets/olistbr/brazilian-ecommerce
---

# Estrutura do Projeto

```text
/databricks_project/
│
├── notebooks/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   ├── streaming/
│   ├── monitoring/
│   └── sandbox/
│
├── data/
│   ├── raw/
│   ├── bronze/
│   ├── silver/
│   ├── gold/
│   ├── checkpoints/
│   └── logs/
│
├── sql/
│   ├── ddl/
│   ├── marts/
│   └── quality/
│
├── docs/
│
└── README.md
```

---

# Estrutura Lógica de Catálogo

```text
catalog: novamarket

schemas:
- bronze
- silver
- gold
- monitoring
```

---

# Tecnologias Utilizadas

## Databricks

* Databricks Community Edition
* Databricks Notebooks
* Databricks SQL
* Workflows conceituais

---

## Apache Spark

* PySpark
* Spark SQL
* Structured Streaming

---

## Delta Lake

* Delta Tables
* Time Travel
* OPTIMIZE
* VACUUM
* MERGE INTO
* ZORDER

---

# Conceitos Técnicos Explorados

## Engenharia de Dados

* Arquitetura Lakehouse
* Medallion Architecture
* CDC
* Streaming
* Batch processing
* Incremental processing
* Idempotência
* Observabilidade
* Data Quality

---

## Spark Internals

* Catalyst Optimizer
* DAG
* Lazy evaluation
* Shuffle
* Skew
* AQE
* Broadcast joins
* Partition pruning
* Predicate pushdown

---

## Delta Lake Internals

* Transaction log
* Snapshot isolation
* File compaction
* Small files problem
* Schema enforcement
* Schema evolution

---

# Estrutura das Entidades

## Clientes

```text
cliente_id
nome
email
cidade
estado
data_cadastro
status
```

---

## Produtos

```text
produto_id
categoria
marca
preco
estoque
status
```

---

## Pedidos

```text
pedido_id
cliente_id
data_pedido
status
valor_total
canal_venda
```

---

## Itens Pedido

```text
pedido_id
produto_id
quantidade
valor_unitario
```

---

## Pagamentos

```text
pagamento_id
pedido_id
status_pagamento
valor
metodo_pagamento
```

---

# Estratégia de Desenvolvimento

O projeto será desenvolvido em fases.

---

# Fase 1 — Fundação Lakehouse

## Objetivos

* Configurar ambiente
* Estruturar diretórios
* Criar primeiras tabelas Delta
* Entender internals do Delta Lake

## Entregas

* Setup Databricks
* Estrutura DBFS
* Primeiras ingestões
* Delta tables iniciais

---

# Fase 2 — Bronze Layer

## Objetivos

Criar pipelines robustos de ingestão.

## Entregas

* Ingestão batch
* Ingestão incremental
* Schema evolution
* Auditoria de ingestão
* Controle de arquivos processados

---

# Fase 3 — Silver Layer

## Objetivos

Construir pipelines confiáveis e incrementalizados.

## Entregas

* CDC
* MERGE incremental
* Deduplicação
* SCD Type 2
* Data Quality
* Regras de negócio

---

# Fase 4 — Gold Layer

## Objetivos

Criar camada analítica.

## Entregas

* Data marts
* KPIs
* Dashboards SQL
* Tabelas executivas

---

# Fase 5 — Streaming

## Objetivos

Implementar pipelines near real-time.

## Entregas

* Structured Streaming
* Checkpoints
* Watermark
* Exactly-once semantics
* Agregações incrementais

---

# Fase 6 — Performance e Observabilidade

## Objetivos

Analisar e otimizar workloads.

## Entregas

* Explain plan
* AQE
* Optimize
* ZORDER
* Análise de shuffle
* Tabelas de monitoramento
* Logs operacionais

---

# Estratégia de Observabilidade

## Tabela de Execução

### monitoring.pipeline_execution

```text
pipeline_name
start_time
end_time
status
records_processed
duration_seconds
error_message
```

---

## Tabela de Data Quality

### monitoring.data_quality

```text
table_name
rule_name
invalid_records
severity
execution_time
```

---

# Estratégia de CDC

Simularemos cenários reais de alteração de dados.

## Exemplos

### Pedido

```text
PENDENTE -> PAGO -> ENVIADO -> ENTREGUE
```

### Cliente

```text
Mudança de endereço
Mudança de segmento
Mudança de status
```

---

# Estratégia de Streaming

Eventos serão simulados para:

* pedidos
* pagamentos
* rastreamento logístico

## Conceitos Explorados

* Watermark
* Event time
* Processing time
* Late arriving data
* Stateful processing
* Exactly-once

---

# Principais Problemas Reais Simulados

## Small Files

Grande quantidade de arquivos pequenos degradando performance.

---

## Data Skew

Chaves desbalanceadas causando gargalo.

---

## Merge Lento

MERGE INTO em grandes volumes.

---

## Shuffle Excessivo

Transformações wide gerando alto custo.

---

## Out Of Memory

Joins inadequados.

---

## Schema Drift

Mudanças inesperadas nas fontes.

---

# Estratégias de Otimização

## Spark

* Broadcast join
* Repartition
* Coalesce
* Cache persist
* AQE

---

## Delta Lake

* OPTIMIZE
* ZORDER
* VACUUM
* Compactação

---

# Conceitos de Governança

Mesmo com limitações do Community Edition, conceitos enterprise serão simulados.

## Conceitos

* Unity Catalog conceitual
* RBAC conceitual
* Lineage
* Ownership
* Data contracts
* Versionamento
* Ambientes DEV/QA/PROD

---

# Objetivo Final do Projeto

Ao final do projeto, o ambiente deverá possuir:

* Plataforma Lakehouse funcional
* Pipelines batch e streaming
* Delta Lake operacional
* Camadas Bronze, Silver e Gold
* Processamento incremental
* CDC
* Data marts
* Observabilidade
* Tuning de performance
* Arquitetura próxima de ambientes enterprise

---

# Competências Desenvolvidas

## Databricks

* Desenvolvimento de notebooks
* Arquitetura Lakehouse
* Delta Lake
* Streaming
* SQL Analytics

---

## Engenharia de Dados

* Pipelines escaláveis
* CDC
* Incremental processing
* Governança
* Performance tuning
* Observabilidade

---

## Apache Spark

* Spark SQL
* DAG analysis
* Shuffle tuning
* AQE
* Joins avançados
* Window functions

---

# Roadmap Futuro

Possíveis evoluções futuras do projeto.

## Evoluções

* Integração com Airflow
* Integração com Kafka
* Unity Catalog real
* Terraform
* CI/CD
* Data contracts automatizados
* Testes automatizados
* ML pipelines
* Feature Store

---

# Observações Importantes

Este projeto tem foco educacional avançado.

A proposta não é apenas aprender comandos do Databricks, mas entender:

* arquitetura
* trade-offs
* performance
* comportamento interno do Spark
* problemas reais de produção
* estratégias enterprise

---

# Licença

Projeto criado exclusivamente para fins educacionais e estudo prático de engenharia de dados moderna com Databricks.
