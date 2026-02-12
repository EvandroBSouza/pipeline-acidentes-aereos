#FUNÇÃO PARA GRAVAÇÃO DA TABELA/ARQUIVO


def salvar_tabela_delta(spark, df, name):

    df.write \
        .format("delta") \
        .mode("overwrite") \
        .option("mergeSchema", "true")\
        .save(name)