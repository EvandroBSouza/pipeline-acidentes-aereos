# Databricks notebook source
#=====================================================
# PROJETO PIPELINE ACIDENTES AEREOS
# Notebook: 03_gold_analytics
# 
# =====================================================

from pyspark.sql.functions import col, count, when, coalesce, lit, current_timestamp, collect_set
import sys


# -----------------------------------------------------
# Path do projeto
# -----------------------------------------------------
sys.path.append("/Workspace/Users/evandro.b.souz@gmail.com/pipeline_acidentes_aereos")
from utils.log import log, configurar_log

# Silver
SILVER_OCORRENCIA_FILE = "/Volumes/lakehouse/acidentes_aereos/silver/ocorrencia_silver/"
SILVER_OCORRENCIA_TIPO_FILE = "/Volumes/lakehouse/acidentes_aereos/silver/ocorrencia_tipo_silver/"
SILVER_AERONAVE_FILE = "/Volumes/lakehouse/acidentes_aereos/silver/aeronave_silver/"
SILVER_FATOR_CONTRIBUINTE_FILE = "/Volumes/lakehouse/acidentes_aereos/silver/fator_contribuinte_silver/"

#Gold
GOLD_OCORRENCIA_PATH = "lakehouse.acidentes_aereos.gold_ocorrencia"
GOLD_FATOR_PATH = "lakehouse.acidentes_aereos.gold_ocorrencia_fator"
GOLD_AERONAVE_PATH = "lakehouse.acidentes_aereos.gold_ocorrencia_aeronave"

 



# COMMAND ----------


#=====================================================
# PROJETO PIPELINE ACIDENTES AEREOS
# Notebook: 03_gold_analytics
# Leitura tabelas Silver
# ===================================================
configurar_log()
log("===== INICIO CAMADA GOLD =====")

try:

    log("Lendo tabelas SILVER...")

    df_ocorrencia = spark.read.format("delta").load(SILVER_OCORRENCIA_FILE)
    df_tipo = spark.read.format("delta").load(SILVER_OCORRENCIA_TIPO_FILE)
    df_aeronave = spark.read.format("delta").load(SILVER_AERONAVE_FILE)
    df_fator = spark.read.format("delta").load(SILVER_FATOR_CONTRIBUINTE_FILE)

    log("Tabelas SILVER lidas com sucesso!\n")
except Exception as e:
    log(f"Erro leitura SILVER: {e}")
    raise e

# COMMAND ----------

#=====================================================
# PROJETO PIPELINE ACIDENTES AEREOS
# Notebook: 03_gold_analytics
# Agregando dados Silver
# ===================================================

try:
    log("Agregando tipo...")
    df_tipo_agg = (
        df_tipo
            .groupBy("CODIGO_OCORRENCIA1")
            .agg(
                collect_set("OCORRENCIA_TIPO").alias("tipos_ocorrencia")
            )
    )

    log("Tipos agregados com sucesso!\n")
except Exception as e:
    log(f"Erro agregando tipos: {e}")
df_tipo_agg.createOrReplaceTempView("tipo_ocorrencia")


# COMMAND ----------

#=====================================================
# PROJETO PIPELINE ACIDENTES AEREOS
# Notebook: 03_gold_analytics
# Agregando dados Silver
# ===================================================

try:
    log("Agregando fatores contribuintes...")
    df_fator_agg = (
        df_fator
            .groupBy("CODIGO_OCORRENCIA3")
            .agg(
                count("*").alias("qtd_fatores")
            )
    )

    log("Fatores agregados com sucesso!\n")
except Exception as e:
    log(f"Erro agregando fatores: {e}")
df_fator_agg.createOrReplaceTempView("fatores_contribuintes")


# COMMAND ----------

# DBTITLE 1,Untitled
#=====================================================
# PROJETO PIPELINE ACIDENTES AEREOS
# Notebook: 03_gold_analytics
# Construção tabela Gold
# ===================================================

try:    
    log("Construindo tabela GOLD...")

    df_gold = (
        df_ocorrencia.alias("o")

            .join(df_tipo_agg.alias("t"),
                  "CODIGO_OCORRENCIA1",
                  "left")

            .join(df_fator_agg.alias("f"),
                  "CODIGO_OCORRENCIA3",
                  "left")

            # Tratando nulos
            .withColumn(
                "qtd_fatores",
                coalesce(col("qtd_fatores"), lit(0))
            )

            .withColumn(
                "possui_fator",
                when(col("qtd_fatores") > 0, 1).otherwise(0)
            )

            .withColumn(
                "gold_ingestao_timestamp",
                current_timestamp()
            )
    )
    log("Tabela GOLD construída com sucesso!\n")
except Exception as e:
    log(f"Erro construindo GOLD: {e}")
    


# COMMAND ----------

#=====================================================
# PROJETO PIPELINE ACIDENTES AEREOS
# Notebook: 03_gold_analytics
# Gravação tabela Gold particionada por ano
# ===================================================

try:
    colunas_exclusao = [
                        "ingestao_file",
                        "ingestao_timestamp", 
                        "CODIGO_OCORRENCIA1",
                        "CODIGO_OCORRENCIA2",
                        "CODIGO_OCORRENCIA3",
                        "CODIGO_OCORRENCIA4"
                    ]

    df_gold = df_gold.drop(*colunas_exclusao)
    
    log("Gravando GOLD particionada por OCORRENCIA_ANO...")

    (
        df_gold
            .write
            .format("delta")
            .mode("overwrite")
            .option("mergeSchema","true")
            .partitionBy("OCORRENCIA_ANO")
            .saveAsTable(GOLD_OCORRENCIA_PATH)
    )

    log("GOLD criada com sucesso!")
    
   
except Exception as e:
    log(f"Erro gravação tabela GOLD particionada: {e}")
    raise e

finally:
    log("===== FIM CAMADA GOLD =====")




# COMMAND ----------

#=====================================================
# PROJETO PIPELINE ACIDENTES AEREOS
# Notebook: 03_gold_analytics
# Construção tabela gold 2 - ocorrencia + fator
# ===================================================

try:
    log("Construindo GOLD 2 - OCORRENCIA + FATOR...")

    df_gold_fator = (
    df_ocorrencia.alias("o")

        .join(
            df_fator.alias("f"),
            "CODIGO_OCORRENCIA3",
            "inner"   #Traz somente as ocorrências que possuem fatores
        )

        .select(
            col("o.CODIGO_OCORRENCIA"),
            col("o.OCORRENCIA_ANO"),
            col("o.OCORRENCIA_MES"),
            col("o.OCORRENCIA_UF"),
            col("o.OCORRENCIA_CLASSIFICACAO"),

            col("f.FATOR_NOME"),
            col("f.FATOR_ASPECTO"),
            col("f.FATOR_AREA")
        )

        .withColumn(
            "gold_ingestao_timestamp",
            current_timestamp()
        )
)

    log("GOLD 2 construída com sucesso!\n")
except Exception as e:
    log(f"Erro construindo GOLD 2: {e}")
    raise e
 

# COMMAND ----------

#=====================================================
# PROJETO PIPELINE ACIDENTES AEREOS
# Notebook: 03_gold_analytics
# Gravação tabela gold 2 - ocorrencia + fator
# ===================================================

try:
    log("Gravando GOLD 2 - OCORRENCIA + FATOR...")
    
    (
        df_gold_fator
            .write
            .format("delta")
            .mode("overwrite")
            .partitionBy("OCORRENCIA_ANO")
            .saveAsTable(GOLD_FATOR_PATH)
    )
except Exception as e:
    log(f"Erro gravação tabela GOLD 2: {e}")
    raise e

finally:    
    log("===== FIM CAMADA GOLD 2 =====")



# COMMAND ----------

#=====================================================
# PROJETO PIPELINE ACIDENTES AEREOS
# Notebook: 03_gold_analytics
# Construindo tabela gold 3 - ocorrencia + aeronave
# ===================================================

try:
    log("Construindo GOLD 3 - OCORRENCIA + AERONAVE...")

    df_gold_aeronave = (
        df_ocorrencia.alias("o")

            .join(
                df_aeronave.alias("a"),
                "CODIGO_OCORRENCIA2",
                "inner"  # toda ocorrência tem aeronave
            )

            .select(
                col("o.CODIGO_OCORRENCIA"),
                col("o.OCORRENCIA_ANO"),
                col("o.OCORRENCIA_MES"),
                col("o.OCORRENCIA_UF"),
                col("o.OCORRENCIA_CLASSIFICACAO"),

                col("a.AERONAVE_FABRICANTE"),
                col("a.AERONAVE_MODELO"),
                col("a.AERONAVE_TIPO_VEICULO"),
                col("a.AERONAVE_FASE_OPERACAO"),
                col("a.AERONAVE_PAIS_FABRICANTE")
            )

            .withColumn(
                "gold_ingestao_timestamp",
                current_timestamp()
            )
    )

    log("GOLD 3 construída com sucesso!\n")
except Exception as e:
    log(f"Erro construindo GOLD 3: {e}")
    raise e



# COMMAND ----------

#=====================================================
# PROJETO PIPELINE ACIDENTES AEREOS
# Notebook: 03_gold_analytics
# Gravação tabela gold 3 - ocorrencia + aeronave
# ===================================================

try:
    log("Gravando GOLD 3 - OCORRENCIA + AERONAVE...")

    (
        df_gold_aeronave
            .write
            .format("delta")
            .mode("overwrite")
            .partitionBy("OCORRENCIA_ANO")
            .saveAsTable(GOLD_AERONAVE_PATH)
    )
except Exception as e:
    log(f"Erro gravação tabela GOLD 3: {e}")
    raise e

log("GOLD 3 gravada com sucesso!\n")
