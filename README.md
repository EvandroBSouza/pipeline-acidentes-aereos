
# ✈️ Pipeline de Engenharia de Dados — Acidentes Aéreos (Base Pública Brasileira)

Projeto de engenharia de dados desenvolvido com PySpark no Databricks Community Edition, aplicando a arquitetura Lakehouse no padrão Medalhão (Bronze → Silver → Gold).

O objetivo é transformar dados públicos brasileiros de acidentes aeronáuticos em um pipeline estruturado, rastreável e pronto para análises estratégicas.

---

## 📊 Sobre os Dados

Este projeto utiliza uma **base pública brasileira de acidentes aeronáuticos**, disponibilizada por órgão oficial de investigação e prevenção de acidentes aéreos (ex: CENIPA — Centro de Investigação e Prevenção de Acidentes Aeronáuticos).

A base contém registros históricos de ocorrências aeronáuticas no Brasil, incluindo informações como:

- Data da ocorrência
- Estado e município
- Tipo e modelo da aeronave
- Operador
- Fase do voo
- Classificação da ocorrência
- Número de ocupantes
- Número de fatalidades
- Relatório descritivo do acidente

Por se tratar de dados governamentais públicos, o dataset apresenta desafios típicos de engenharia de dados:

- Inconsistência de padronização entre períodos
- Campos com valores nulos
- Divergências de nomenclatura
- Necessidade de tipagem adequada
- Tratamento de colunas textuais extensas

O pipeline implementado neste projeto trata essas inconsistências de forma estruturada, garantindo qualidade, rastreabilidade e confiabilidade analítica.

---

## 🏗️ Arquitetura

O projeto segue o padrão **Medallion Architecture**, amplamente utilizado em ambientes Lakehouse:

Bronze → Silver → Gold


### 🥉 Bronze
- Ingestão dos dados brutos
- Preservação da estrutura original
- Garantia de rastreabilidade

### 🥈 Silver
- Tratamento de valores nulos
- Padronização de nomes de colunas
- Conversão de tipos (datas e numéricos)
- Limpeza e normalização de dados
- Criação de colunas derivadas

### 🥇 Gold
- Agregações analíticas
- Cálculo de métricas estratégicas
- Consolidação para consumo por BI ou dashboards

---

## 📂 Estrutura do Projeto

pipeline-acidentes-aereos/
│
├── notebooks/
│ ├── 01_bronze_ingest.py
│ ├── 02_silver_transform.py
│ └── 03_gold_analytics.py
│
├── utils/
│ └── funções auxiliares e helpers
│
└── README.md


---

## ⚙️ Tecnologias Utilizadas

- Python
- PySpark
- Databricks Community Edition
- Arquitetura Lakehouse
- Git & GitHub

---

## 🔄 Fluxo do Pipeline

### 1️⃣ Ingestão (Bronze)
Leitura da base pública e armazenamento da versão original para garantir governança e rastreabilidade.

### 2️⃣ Transformação (Silver)
Aplicação de regras de limpeza e padronização, garantindo consistência estrutural e tipagem adequada.

### 3️⃣ Camada Analítica (Gold)
Criação de métricas como:

- Evolução anual de acidentes no Brasil
- Estados com maior número de ocorrências
- Taxa de fatalidade por período
- Distribuição por fase do voo
- Comparativo entre ocorrências fatais e não fatais

---

## 🚀 Como Executar

1. Importar os notebooks no Databricks Community Edition
2. Executar na ordem:

01_bronze_ingest
02_silver_transform
03_gold_analytics


---

## 🎯 Objetivos do Projeto

Este projeto foi desenvolvido com foco em:

- Aplicação prática de arquitetura Medalhão
- Tratamento de dados públicos reais
- Estruturação de pipelines escaláveis
- Boas práticas de versionamento
- Preparação para ambientes produtivos

---

## 🔮 Próximos Passos

- Persistência em Delta Lake
- Parametrização do pipeline
- Orquestração com Jobs
- Implementação de testes automatizados
- Integração com ferramenta de BI

---

## 👨‍💻 Autor

Evandro Souza  
Engenharia de Dados | Lakehouse | PySpark

