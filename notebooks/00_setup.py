# Databricks notebook source
# =====================================================
# PROJETO PIPELINE ACIDENTES AEREOS
# Notebook: 00_setup
# =====================================================

import sys
sys.path.append("/Workspace/Users/evandro.b.souz@gmail.com/pipeline_acidentes_aereos")

from utils.log import log, configurar_log

configurar_log()
log("===== INÍCIO DO SETUP DO PROJETO =====")

try:
    # -------------------------------------------------
    # 1. Definições do projeto
    # -------------------------------------------------
    CATALOG = "lakehouse"
    SCHEMA = "acidentes_aereos"

    # -------------------------------------------------
    # 2. Criação do Schema (database no UC)
    # -------------------------------------------------
    spark.sql(f"""
        CREATE SCHEMA IF NOT EXISTS {CATALOG}.{SCHEMA}
    """)

    spark.sql(f"USE {CATALOG}.{SCHEMA}")

    log(f"Schema '{CATALOG}.{SCHEMA}' criado/selecionado com sucesso")

    # -------------------------------------------------
    # 3. Criação dos Volumes
    # -------------------------------------------------
    volumes = ["raw", "bronze", "silver", "gold"]

    for volume in volumes:
        spark.sql(f"""
            CREATE VOLUME IF NOT EXISTS {CATALOG}.{SCHEMA}.{volume}
        """)
        log(f"Volume '{volume}' criado com sucesso")

    # -------------------------------------------------
    # 4. Validação do ambiente
    # -------------------------------------------------
    versao = spark.sql("SELECT version() as v").collect()[0]["v"]
    log(f"Versão do ambiente: {versao}")

    schema_ativo = spark.sql("SELECT current_schema() as s").collect()[0]["s"]
    log(f"Schema ativo: {schema_ativo}")

    log("===== SETUP FINALIZADO COM SUCESSO =====")

except Exception as e:
    log(f"ERRO NO SETUP: {str(e)}")
    raise
