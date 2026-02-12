import re
from pyspark.sql.functions import col, upper, trim

def normalizar_dados_spark(df):
    """
    Normaliza DataFrame Spark:
    - Padroniza nomes das colunas
    - Remove duplicados
    - Remove linhas totalmente nulas
    - Padroniza valores string (trim + upper)

    Retorna DataFrame normalizado
    """

    # ---------------------------
    # Normalizar nomes das colunas
    # ---------------------------
    for c in df.columns:
        novo_nome = (
            c.strip()
             .upper()
        )
        novo_nome = re.sub(r'[^A-Z0-9_]+', '_', novo_nome)
        df = df.withColumnRenamed(c, novo_nome)

    # ---------------------------
    # Remover linhas duplicadas
    # ---------------------------
    df = df.dropDuplicates()

    # ---------------------------
    # Remover linhas totalmente nulas
    # ---------------------------
    df = df.dropna(how="all")

    # ---------------------------
    # Padronizar colunas STRING
    # ---------------------------
    for field in df.schema.fields:
        if field.dataType.simpleString() == "string":
            df = df.withColumn(
                field.name,
                upper(trim(col(field.name)))
            )

    return df