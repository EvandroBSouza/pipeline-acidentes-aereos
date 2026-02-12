# Databricks notebook source
dbutils.library.restartPython()

# COMMAND ----------

#=====================================================
# PROJETO PIPELINE ACIDENTES AEREOS
# Notebook: 01_ingestion_bronze
# Ingestão CSV ocorrencia
# =====================================================
  
from pyspark.sql.functions import col, current_timestamp, input_file_name
import sys
# -----------------------------------------------------
# Path do projeto
# -----------------------------------------------------
sys.path.append("/Workspace/Users/evandro.b.souz@gmail.com/pipeline_acidentes_aereos")
from utils.log import log, configurar_log


RAW_PATH = "/Volumes/lakehouse/acidentes_aereos/raw" 
RAW_FILE = f"{RAW_PATH}/ocorrencia.csv"



configurar_log()
log("===== INÍCIO INGESTÃO ARQUIVO OCORRENCIA  =====")

# Verificação dos
try:
    df_ocorrencia = (
    spark.read
    .option("header", "true")
    .option("sep", ";")
    .option("encoding", "ISO-8859-1")
    .option("inferSchema", "false")
    .csv("/Volumes/lakehouse/acidentes_aereos/raw/ocorrencia.csv")
    )   
    
    log("(===== INGESTÃO ARQUIVO OCORRENCIA CONCLUÍDA COM SUCESSO =====")
    
except Exception as e:
    log(f"ERRO NA INGESTÃO : {str(e)}")
    raise


# Logs de informações do arquivo
log("RAW OK")
log(f"Linhas RAW: {df_ocorrencia.count()}")
log(f"Colunas RAW: {df_ocorrencia.columns}")







# COMMAND ----------

#=====================================================
# PROJETO PIPELINE ACIDENTES AEREOS
# Notebook: 01_ingestion_bronze
# Gravação camada bronze ocorrencia_bronze
# =====================================================
from utils.writer import salvar_tabela_delta

BRONZE_PATH = "/Volumes/lakehouse/acidentes_aereos/bronze/ocorrrencia_bronze"
    
configurar_log()
log("===== INÍCIO GRAVAÇÃO CAMADA BRONZE OCORRENCIA =====")

try:
    df_ocorrencia_bronze = df_ocorrencia.withColumn("ingestao_timestamp", current_timestamp()) \
    .withColumn("ingestao_file", col("_metadata.file_path"))

    salvar_tabela_delta(spark,df_ocorrencia_bronze, BRONZE_PATH,)
    log("GRAVAÇÃO CAMADA BRONZE OCORRENCIA CONCLUÍDA COM SUCESSO")

except Exception as e:
    log(f"ERRO NA GRAVAÇÃO: {str(e)}")
    raise

log(f"Linhas BRONZE: {df_ocorrencia_bronze.count()}")
display(df_ocorrencia_bronze.limit(10))


    

# COMMAND ----------

#=====================================================
# PROJETO PIPELINE ACIDENTES AEREOS
# Notebook: 01_ingestion_bronze
# Ingestão CSV ocorrencia_tipo
# =====================================================


RAW_PATH = "/Volumes/lakehouse/acidentes_aereos/raw" 
RAW_FILE = f"{RAW_PATH}/ocorrencia_tipo.csv"

configurar_log()
log("===== INÍCIO INGESTÃO ARQUIVO OCORRENCIA_TIPO =====")


try:
    df_ocorrencia_tipo = (
    spark.read
    .option("header", "true")
    .option("sep", ";")
    .option("encoding", "ISO-8859-1")
    .option("inferSchema", "false")
    .csv("/Volumes/lakehouse/acidentes_aereos/raw/ocorrencia_tipo.csv")
    )   

    log("INGESTÃO ARQUIVO OCORRENCIA_TIPO CONCLUÍDA COM SUCESSO")
    
except Exception as e:
    log(f"ERRO NA INGESTÃO : {str(e)}")
    raise


# Logs equivalentes ao Airflow
log("RAW OK")
log(f"Linhas RAW: {df_ocorrencia_tipo.count()}")
log(f"Colunas RAW: {df_ocorrencia_tipo.columns}")





# COMMAND ----------

#=====================================================
# PROJETO PIPELINE ACIDENTES AEREOS
# Notebook: 01_ingestion_bronze
# Gravação camada bronze ocorrencia_tipo_bronze
# =====================================================
from utils.writer import salvar_tabela_delta

BRONZE_PATH = f"/Volumes/lakehouse/acidentes_aereos/bronze/ocorrrencia_tipo_bronze"
    
configurar_log()
log("===== INÍCIO GRAVAÇÃO CAMADA BRONZE OCORRENCIA_TIPO =====")

try:
    df_ocorrencia_tipo_bronze = df_ocorrencia_tipo.withColumn("ingestao_timestamp", current_timestamp()) \
    .withColumn("ingestao_file", col("_metadata.file_path"))

    salvar_tabela_delta(spark,df_ocorrencia_tipo_bronze, BRONZE_PATH,)
    log("GRAVAÇÃO CAMADA BRONZE OCORRENCIA_TIPO CONCLUÍDA COM SUCESSO")

except Exception as e:
    log(f"ERRO NA GRAVAÇÃO: {str(e)}")
    raise

log(f"Linhas BRONZE: {df_ocorrencia_tipo_bronze.count()}")
display(df_ocorrencia_tipo_bronze.limit(10))

    

# COMMAND ----------

#=====================================================
# PROJETO PIPELINE ACIDENTES AEREOS
# Notebook: 01_ingestion_bronze
# Ingestão CSV aeronave
# =====================================================


RAW_PATH = "/Volumes/lakehouse/acidentes_aereos/raw" 
RAW_FILE = f"{RAW_PATH}/aeronave.csv"

configurar_log()
log("===== INÍCIO INGESTÃO ARQUIVO AERONAVE =====")

# Verificação dos
try:
    df_aeronave = (
    spark.read
    .option("header", "true")
    .option("sep", ";")
    .option("encoding", "ISO-8859-1")
    .option("inferSchema", "false")
    .csv("/Volumes/lakehouse/acidentes_aereos/raw/aeronave.csv")
    )   

    log("INGESTÃO ARQUIVO AERONAVE CONCLUÍDA COM SUCESSO")
    
except Exception as e:
    log(f"ERRO NA INGESTÃO: {str(e)}")
    raise


# Logs equivalentes ao Airflow
log("RAW OK")
log(f"Linhas RAW: {df_aeronave.count()}")
log(f"Colunas RAW: {df_aeronave.columns}")





# COMMAND ----------

#=====================================================
# PROJETO PIPELINE ACIDENTES AEREOS
# Notebook: 01_ingestion_bronze
# Gravação camada bronze aeronave_bronze
# =====================================================
from utils.writer import salvar_tabela_delta

BRONZE_PATH = f"/Volumes/lakehouse/acidentes_aereos/bronze/aeronave_bronze"
    
configurar_log()
log("===== INÍCIO GRAVAÇÃO CAMADA BRONZE AERONAVE =====")

try:
    df_aeronave_bronze = df_aeronave.withColumn("ingestao_timestamp", current_timestamp()) \
    .withColumn("ingestao_file", col("_metadata.file_path"))

    salvar_tabela_delta(spark,df_aeronave_bronze, BRONZE_PATH,)
    log("GRAVAÇÃO CAMADA BRONZE AERONAVE CONCLUÍDA COM SUCESSO")

except Exception as e:
    log(f"ERRO NA GRAVAÇÃO: {str(e)}")
    raise

log(f"Linhas BRONZE: {df_aeronave_bronze.count()}")
display(df_aeronave_bronze.limit(10))

    

# COMMAND ----------

#=====================================================
# PROJETO PIPELINE ACIDENTES AEREOS
# Notebook: 01_ingestion_bronze
# Ingestão CSV fator_contribuinte
# =====================================================


RAW_PATH = "/Volumes/lakehouse/acidentes_aereos/raw" 
RAW_FILE = f"{RAW_PATH}/fator_contribuinte.csv"

configurar_log()
log("===== INÍCIO INGESTÃO ARQUIVO FATOR_CONTRIBUINTE =====")

# Verificação dos
try:
    df_fator_contribuinte = (
    spark.read
    .option("header", "true")
    .option("sep", ";")
    .option("encoding", "ISO-8859-1")
    .option("inferSchema", "false")
    .csv("/Volumes/lakehouse/acidentes_aereos/raw/fator_contribuinte.csv")
    )   

    log("INGESTÃO ARQUIVO FATOR_CONTRIBUINTE CONCLUÍDA COM SUCESSO")
    
except Exception as e:
    log(f"ERRO NA INGESTÃO: {str(e)}")
    raise


# Logs equivalentes ao Airflow
log("RAW OK")
log(f"Linhas RAW: {df_fator_contribuinte.count()}")
log(f"Colunas RAW: {df_fator_contribuinte.columns}")



# COMMAND ----------

#=====================================================
# PROJETO PIPELINE ACIDENTES AEREOS
# Notebook: 01_ingestion_bronze
# Gravação camada bronze fator_contribuinte_bronze
# =====================================================
from utils.writer import salvar_tabela_delta

BRONZE_PATH = f"/Volumes/lakehouse/acidentes_aereos/bronze/fator_contribuinte_bronze"
    
configurar_log()
log("===== INÍCIO GRAVAÇÃO CAMADA BRONZE FATOR CONTRIBUINTE =====")

try:
    df_fator_contribuinte_bronze = df_fator_contribuinte.withColumn("ingestao_timestamp", current_timestamp()) \
    .withColumn("ingestao_file", col("_metadata.file_path"))

    salvar_tabela_delta(spark,df_fator_contribuinte_bronze, BRONZE_PATH,)
    log("GRAVAÇÃO CAMADA BRONZE FATOR CONTRIBUINTE CONCLUÍDA COM SUCESSO")

except Exception as e:
    log(f"ERRO NA GRAVAÇÃO: {str(e)}")
    raise

log(f"Linhas BRONZE: {df_fator_contribuinte_bronze.count()}")
display(df_fator_contribuinte_bronze.limit(10))

    