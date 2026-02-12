# ✈️ Pipeline de Engenharia de Dados — Acidentes Aéreos

Projeto de engenharia de dados utilizando arquitetura Lakehouse com processamento em PySpark no Databricks Community Edition.

O objetivo do projeto é construir um pipeline estruturado no padrão Medalhão (Bronze → Silver → Gold), transformando dados brutos de acidentes aéreos em camadas analíticas prontas para consumo.

---

## 🏗️ Arquitetura

O pipeline segue o padrão **Medallion Architecture**:

Bronze → Silver → Gold


- **Bronze**: ingestão de dados brutos
- **Silver**: limpeza, padronização e enriquecimento
- **Gold**: agregações e métricas analíticas

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

### 1️⃣ Camada Bronze
- Leitura dos dados brutos
- Armazenamento sem transformações estruturais significativas
- Garantia de rastreabilidade

### 2️⃣ Camada Silver
- Tratamento de valores nulos
- Padronização de colunas
- Tipagem correta
- Enriquecimento de dados

### 3️⃣ Camada Gold
- Criação de métricas analíticas
- Agregações
- Preparação para dashboards ou consumo por BI

---

## 🚀 Como Executar

1. Importar os notebooks no Databricks Community Edition
2. Executar na ordem:

01_bronze_ingest
02_silver_transform
03_gold_analytics


---

## 🎯 Objetivo do Projeto

Este projeto foi desenvolvido com foco em:

- Prática de arquitetura de dados moderna
- Organização de pipelines em camadas
- Boas práticas de versionamento
- Preparação para ambientes produtivos

---

## 📌 Próximos Passos

- Implementar testes automatizados
- Parametrização do pipeline
- Orquestração com jobs
- Persistência em Delta Lake
- Integração com ferramenta de BI

---

## 👨‍💻 Autor

Evandro Souza  
Engenharia de Dados | Lakehouse | PySpark
