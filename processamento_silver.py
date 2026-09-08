# Databricks notebook source
# DBTITLE 1,Célula 1
# Carregando os dois conjuntos de dados
df1 = spark.table("challenge_nuclea.bronze.base_auxiliar")
df2 = spark.table("challenge_nuclea.bronze.base_boletos")

display(df1)
display(df2)

# COMMAND ----------

# DBTITLE 1,Avaliação da Qualidade dos Dados
# Avaliação da Qualidade dos Dados
from pyspark.sql.functions import col, count, when, isnan, isnull, sum as spark_sum

print("=== Verificação de Qualidade DF1 (Base Auxiliar) ===")
print(f"Total de linhas: {df1.count()}")
print("\nContagem de nulos por coluna:")
df1_null_counts = df1.select([spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in df1.columns])
display(df1_null_counts)

print("\n=== Verificação de Qualidade DF2 (Base Boletos) ===")
print(f"Total de linhas: {df2.count()}")
print("\nContagem de nulos por coluna:")
df2_null_counts = df2.select([spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in df2.columns])
display(df2_null_counts)

print("\nTipos de dados:")
print("DF1:", df1.dtypes)
print("DF2:", df2.dtypes)

# COMMAND ----------

# DBTITLE 1,Summary & Preview
# Resumo dos Dados Limpos
print("=== RESUMO DOS CONJUNTOS DE DADOS LIMPOS ===")
print(f"\nDF1 (Base Auxiliar) - Limpo: {df1_clean.count()} linhas, {len(df1_clean.columns)} colunas")
print(f"DF2 (Base Boletos) - Limpo: {df2_clean.count()} linhas, {len(df2_clean.columns)} colunas")

print("\n=== Amostra DF1 (primeiras 5 linhas) ===")
display(df1_clean.limit(5))

print("\n=== Amostra DF2 (primeiras 5 linhas) ===")
display(df2_clean.limit(5))

print("\n=== Chaves Potenciais para Join ===")
print("- DF1 possui 'id_cnpj' (identificador da empresa)")
print("- DF2 possui 'id_pagador' e 'id_beneficiario' (identificadores de pagador e beneficiário)")
print("- Estes podem ser potencialmente unidos para enriquecer dados de boleto com métricas da empresa")

# COMMAND ----------

# DBTITLE 1,Verify Silver Tables
# Verificar tabelas da camada Silver
print("=== Verificação: Lendo da Camada Silver ===")

# Ler as tabelas de volta para verificar
silver_auxiliar = spark.table("challenge_nuclea.silver.processed_base_auxiliar")
silver_boletos = spark.table("challenge_nuclea.silver.processed_base_boletos")

print(f"\nprocessed_base_auxiliar: {silver_auxiliar.count()} linhas, {len(silver_auxiliar.columns)} colunas")
print(f"processed_base_boletos: {silver_boletos.count()} linhas, {len(silver_boletos.columns)} colunas")

print("\n=== Amostra de processed_base_auxiliar ===")
display(silver_auxiliar.limit(3))

print("\n=== Amostra de processed_base_boletos ===")
display(silver_boletos.limit(3))

# COMMAND ----------

# DBTITLE 1,Clean Base Boletos (DF2)
# Limpar DF2 (Base Boletos)
from pyspark.sql.functions import datediff

# Começar com todas as linhas (nenhum ID nulo detectado)
df2_clean = df2

# Criar flag de status de pagamento
df2_clean = df2_clean.withColumn(
    "is_paid",
    when(col("dt_pagamento").isNotNull(), lit(True)).otherwise(lit(False))
)

# Criar flag de status de baixa
df2_clean = df2_clean.withColumn(
    "is_cleared",
    when((col("vlr_baixa").isNotNull()) & (col("tipo_baixa").isNotNull()), lit(True)).otherwise(lit(False))
)

# Calcular dias até o pagamento (negativo = pago antecipado, positivo = pago atrasado, nulo = não pago)
df2_clean = df2_clean.withColumn(
    "dias_atraso_pagamento",
    when(
        col("dt_pagamento").isNotNull(),
        datediff(col("dt_pagamento"), col("dt_vencimento"))
    ).otherwise(lit(None))
)

# Para boletos não pagos, preencher vlr_baixa com 0 e tipo_baixa com 'NAO PAGO'
df2_clean = df2_clean.withColumn(
    "vlr_baixa",
    when(col("is_paid") == False, lit(0.0)).otherwise(col("vlr_baixa"))
)

df2_clean = df2_clean.withColumn(
    "tipo_baixa",
    when(col("tipo_baixa").isNull(), lit("NAO PAGO")).otherwise(col("tipo_baixa"))
)

# Validar datas: emissão deve ser antes ou igual à data de vencimento
df2_clean = df2_clean.withColumn(
    "valid_dates",
    when(col("dt_emissao") <= col("dt_vencimento"), lit(True)).otherwise(lit(False))
)

print(f"DF2 - Linhas originais: {df2.count()}, Linhas limpas: {df2_clean.count()}")
print(f"\nStatus de pagamento:")
df2_clean.groupBy("is_paid").count().show()
print(f"\nSequências de datas inválidas:")
df2_clean.groupBy("valid_dates").count().show()

# Verificar que não restam nulos críticos
null_check = df2_clean.select([spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in df2_clean.columns])
display(null_check)

# COMMAND ----------

# DBTITLE 1,Clean Base Auxiliar (DF1)
# Limpar DF1 (Base Auxiliar)
from pyspark.sql.functions import coalesce, lit, when, mean

# Remover linhas com nulo em colunas de identificador crítico
df1_clean = df1.filter(col("id_cnpj").isNotNull())

# Tratar nulos de UF - marcar como 'UNKNOWN'
df1_clean = df1_clean.withColumn(
    "uf",
    coalesce(col("uf"), lit("UNKNOWN"))
)

# Preencher nulos numéricos com 0 para índices e scores (assumindo que 0 significa sem dados/não aplicável)
numeric_cols_to_fill = [
    "sacado_indice_liquidez_1m",
    "cedente_indice_liquidez_1m",
    "indicador_liquidez_quantitativo_3m",
    "share_vl_inad_pag_bol_6_a_15d"
]

for col_name in numeric_cols_to_fill:
    df1_clean = df1_clean.withColumn(
        col_name,
        coalesce(col(col_name), lit(0.0))
    )

# Preencher colunas de score com valores medianos
score_cols = [
    "score_materialidade_evolucao",
    "score_quantidade_v2",
    "score_materialidade_v2"
]

for col_name in score_cols:
    median_val = df1_clean.approxQuantile(col_name, [0.5], 0.01)[0]
    df1_clean = df1_clean.withColumn(
        col_name,
        coalesce(col(col_name), lit(int(median_val)))
    )

# Preencher nulos restantes
df1_clean = df1_clean.fillna({
    "media_atraso_dias": 0.0,
    "cd_cnae_prin": 0
})

print(f"DF1 - Linhas originais: {df1.count()}, Linhas limpas: {df1_clean.count()}")
print(f"Linhas removidas: {df1.count() - df1_clean.count()}")

# Verificar que não restam nulos
null_check = df1_clean.select([spark_sum(when(col(c).isNull(), 1).otherwise(0)).alias(c) for c in df1_clean.columns])
display(null_check)