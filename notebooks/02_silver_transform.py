# Databricks notebook source
dbutils.library.restartPython()

# COMMAND ----------

#=====================================================
# PROJETO PIPELINE ACIDENTES AEREOS
# Notebook: 02_silver_transform
# Paths e imports
# =====================================================

 

from pyspark.sql.functions import col, current_timestamp, input_file_name
import sys

# -----------------------------------------------------
# Path do projeto
# -----------------------------------------------------
sys.path.append("/Workspace/Users/evandro.b.souz@gmail.com/pipeline_acidentes_aereos")
from utils.log import log, configurar_log

BRONZE_OCORRENCIA_FILE = "/Volumes/lakehouse/acidentes_aereos/bronze/ocorrrencia_bronze/"
BRONZE_OCORRENCIA_TIPO_FILE = "/Volumes/lakehouse/acidentes_aereos/bronze/ocorrrencia_tipo_bronze/"
BRONZE_AERONAVE_FILE = "/Volumes/lakehouse/acidentes_aereos/bronze/aeronave_bronze/"
BRONZE_FATOR_CONTRIBUINTE_FILE = "/Volumes/lakehouse/acidentes_aereos/bronze/fator_contribuinte_bronze/"
 

configurar_log()
log("===== INICIO CAMADA SILVER VALIDAÇÃO DE CHAVES =====")



# COMMAND ----------

#=====================================================
# PROJETO PIPELINE ACIDENTES AEREOS
# Notebook: 02_silver_transform
# normalização e limpeza dos dados
# =====================================================

# Normalizar colunas de todos os arquivos   

from utils.normalizar import normalizar_dados_spark



configurar_log()
log("===== INÍCIO NORMALIZAÇÃO DE DADOS - CAMADA SILVER =====\n")

try:
    

    log("Normalização de dados - tabela ocorrencia")
    # Leitura bronze 
    df_ocorrencia_silver = (
        spark.read.format("delta")
            .load(BRONZE_OCORRENCIA_FILE)
            .withColumn("ingestao_timestamp", current_timestamp())
            .withColumn("ingestao_file",col("_metadata.file_path"))
    )

    #Validação quantidade de registros antes da validação
    log(f'quantidade de registros antes da normalização: {df_ocorrencia_silver.count()}')

    #Normalização
    df_ocorrencia_silver = normalizar_dados_spark(df_ocorrencia_silver)

    #validação quantidade de registros depois da validação
    log(f'quantidade de registros depois da normalização: {df_ocorrencia_silver.count()}\n')

    
    
    #---------------------------------------------------------------------------------------------------



    log('Normalização de dados - tabela ocorrencia_tipo')
    #Leitura arquivo ocorrencia_tipo bronze
    df_ocorrencia_tipo_silver = (
        spark.read.format("delta")
            .load(BRONZE_OCORRENCIA_TIPO_FILE)
            .withColumn("ingestao_timestamp", current_timestamp())
            .withColumn("ingestao_file",col("_metadata.file_path"))
    )

    
    #Validação quantidade de registros antes da validação
    log(f"Quantidade de registros antes da normalização: {df_ocorrencia_tipo_silver.count()}")

    #Normalização
    df_ocorrencia_tipo_silver = normalizar_dados_spark(df_ocorrencia_tipo_silver)

    #Validação quantidade de registros depois da validação
    log(f"Quantidade de registros depois da normalização: {df_ocorrencia_tipo_silver.count()}\n")


    #--------------------------------------------------------------------------------------------------------

    log('Normalização de dados - tabela aeronave')
    #Leitura arquivo aeronave bronze
    df_aeronave_silver = (
        spark.read.format("delta")
            .load(BRONZE_AERONAVE_FILE)
            .withColumn("ingestao_timestamp", current_timestamp())
            .withColumn("ingestao_file",col("_metadata.file_path"))
    )
    #Validação quantidade de registros antes da validação
    log(f"Quantidade de registros antes da normalização: {df_aeronave_silver.count()}")

    #Normalização
    df_aeronave_silver = normalizar_dados_spark(df_aeronave_silver)

    #Validação quantidade de registros depois da validação
    log(f"Quantidade de registros depois da normalização: {df_aeronave_silver.count()}\n")
    
    
    #----------------------------------------------------------------------------------------------------------

    log('Normalização de dados - tabela fator_contribuinte')
    #Leitura arquivo fator_contribuinte bronze
    df_fator_contribuinte_silver = (
        spark.read.format("delta")
            .load(BRONZE_FATOR_CONTRIBUINTE_FILE)
            .withColumn("ingestao_timestamp", current_timestamp())
            .withColumn("ingestao_file",col("_metadata.file_path"))
    )
    #Validação quantidade de registros antes da validação
    log(f"Quantidade de registros antes da normalização: {df_fator_contribuinte_silver.count()}")

    #Normalização
    df_fator_contribuinte_silver = normalizar_dados_spark(df_fator_contribuinte_silver)

    #Validação quantidade de registros depois da validação
    log(f"Quantidade de registros depois da normalização: {df_fator_contribuinte_silver.count()}\n")


    
except Exception as e:
    log(f"Erro normalização de dados camada silver: {e}")
    raise e
finally:
    log("===== FIM NORMALIZAÇÃO DE DADOS - CAMADA SILVER =====")



# COMMAND ----------


#=====================================================
# PROJETO PIPELINE ACIDENTES AEREOS
# Notebook: 02_silver_transform
# Gravação tabelas silver
# =====================================================
from pyspark.sql.functions import to_date, year, col, expr, trim, dayofweek,month, quarter
from utils.writer import salvar_tabela_delta

# PATH SILVER
OCORRENCIA_SILVER_PATH = "/Volumes/lakehouse/acidentes_aereos/silver/ocorrencia_silver"
OCORRENCIA_TIPO_SILVER_PATH = "/Volumes/lakehouse/acidentes_aereos/silver/ocorrencia_tipo_silver"
AERONAVE_SILVER_PATH = "/Volumes/lakehouse/acidentes_aereos/silver/aeronave_silver"
FATOR_CONTRIBUINTE_SILVER_PATH = "/Volumes/lakehouse/acidentes_aereos/silver/fator_contribuinte_silver"
SILVER_PATH = "/Volumes/lakehouse/acidentes_aereos/silver"



#Colunas obrigatórias:

#Tabela ocorrência
colunas_ocorrencia = ['CODIGO_OCORRENCIA',
                      'CODIGO_OCORRENCIA1',
                      'CODIGO_OCORRENCIA2',
                      'CODIGO_OCORRENCIA3', 
                      'OCORRENCIA_CLASSIFICACAO', 
                      'OCORRENCIA_LATITUDE', 
                      'OCORRENCIA_LONGITUDE', 
                      'OCORRENCIA_CIDADE', 
                      'OCORRENCIA_UF', 
                      'OCORRENCIA_PAIS', 
                      'OCORRENCIA_AERODROMO', 
                      'OCORRENCIA_DIA', 
                      'OCORRENCIA_HORA', 
                      'INVESTIGACAO_STATUS', 
                      'TOTAL_AERONAVES_ENVOLVIDAS', 
                      'OCORRENCIA_SAIDA_PISTA',
                      'OCORRENCIA_ANO',
                      'OCORRENCIA_MES',
                      'OCORRENCIA_DIA_SEMANA',
                      'OCORRENCIA_TRIMESTRE'
                    ]

#Tabela ocorrencia_tipo
colunas_ocorrencia_tipo = ['CODIGO_OCORRENCIA1',
                           'OCORRENCIA_TIPO'
                        ]



#Tabela aeronave
colunas_aeronave = ['CODIGO_OCORRENCIA2',
                    'AERONAVE_TIPO_VEICULO',
                    'AERONAVE_FABRICANTE',
                    'AERONAVE_MODELO',
                    'AERONAVE_ASSENTOS',
                    'AERONAVE_ANO_FABRICACAO',
                    'AERONAVE_PAIS_FABRICANTE',
                    'AERONAVE_VOO_ORIGEM',
                    'AERONAVE_VOO_DESTINO',
                    'AERONAVE_FASE_OPERACAO'
                ]   

#Tabela fator_contribuinte
colunas_fator_contribuinte = ['CODIGO_OCORRENCIA3',
                              'FATOR_NOME',
                              'FATOR_ASPECTO',
                              'FATOR_AREA'
                            ]   



configurar_log()
log("===== INÍCIO GRAVAÇÃO TABELAS - CAMADA SILVER =====\n")

try:
    #transformando a coluna OCORRENCIA_DIA em date e adicionando colunas analiticas na tabela ocorrencia
    df_ocorrencia_silver = (
        df_ocorrencia_silver
            .withColumn(
                "OCORRENCIA_DIA",
                to_date(col("OCORRENCIA_DIA"), "dd/MM/yyyy")
            )
            .withColumn(
                "OCORRENCIA_ANO",
                year(col("OCORRENCIA_DIA"))
            )
            .withColumn(
                "OCORRENCIA_MES",
                month(col("OCORRENCIA_DIA"))
            )
            .withColumn(
                "OCORRENCIA_DIA_SEMANA",
                dayofweek(col("OCORRENCIA_DIA"))
            )
            .withColumn(
                "OCORRENCIA_TRIMESTRE",
                quarter(col("OCORRENCIA_DIA"))
            )
            
    )

    #Gravando a tabela ocorrencia na camada silver
    log('Gravando tabela ocorrencia camada silver')
    df_ocorrencia_silver = df_ocorrencia_silver.select(colunas_ocorrencia)

    df_ocorrencia_silver = (
        df_ocorrencia_silver
            .withColumn("ingestao_timestamp", current_timestamp())
            .withColumn("ingestao_file", col("_metadata.file_path"))
    )
    salvar_tabela_delta(spark,df_ocorrencia_silver, OCORRENCIA_SILVER_PATH)
    log('Gravação tabela ocorrencia camada silver realizada com sucesso\n')

    #Gravando a tabela ocorrencia_tipo na camada silver
    log('Gravando tabela ocorrencia_tipo camada silver')
    df_ocorrencia_tipo_silver = df_ocorrencia_tipo_silver.select(colunas_ocorrencia_tipo)

    df_ocorrencia_Tipo_silver = (
        df_ocorrencia_tipo_silver
            .withColumn("ingestao_timestamp", current_timestamp())
            .withColumn("ingestao_file", col("_metadata.file_path"))
    )
    salvar_tabela_delta(spark,df_ocorrencia_Tipo_silver, OCORRENCIA_TIPO_SILVER_PATH)
    log('Gravação tabela ocorrencia_tipo camada silver realizada com sucesso\n')

    #Gravando a tabela aeronave na camada silver
    log('Gravando tabela aeronave camada silver')
    df_aeronave_silver = df_aeronave_silver.select(colunas_aeronave)

    df_aeronave_silver = (
        df_aeronave_silver
            .withColumn("ingestao_timestamp", current_timestamp())
            .withColumn("ingestao_file", col("_metadata.file_path"))
    )
    salvar_tabela_delta(spark,df_aeronave_silver, AERONAVE_SILVER_PATH)
    log('Gravação tabela aeronave camada silver realizada com sucesso\n')

    #Gravando a tabela fator_contribuinte na camada silver
    log('Gravando tabela fator_contribuinte camada silver')
    df_fator_contribuinte_silver = df_fator_contribuinte_silver.select(colunas_fator_contribuinte)

    df_fator_contribuinte_silver = (
        df_fator_contribuinte_silver
            .withColumn("ingestao_timestamp", current_timestamp())
            .withColumn("ingestao_file", col("_metadata.file_path"))
    )
    salvar_tabela_delta(spark,df_fator_contribuinte_silver, FATOR_CONTRIBUINTE_SILVER_PATH)
    log('Gravação tabela fator_contribuinte camada silver realizada com sucesso\n')


except Exception as e:
    log(f"Erro gravação tabelas silver: {e}")
    raise e
finally:
    log("===== FIM GRAVAÇÃO TABELAS - CAMADA SILVER =====")

# COMMAND ----------

# MAGIC %md
# MAGIC