# Databricks notebook source
from pyspark.sql.functions import col, year, month, sum, avg, count

# Carregar tabelas
aux_df = spark.table("challenge_nuclea.silver.processed_base_auxiliar")
boletos_df = spark.table("challenge_nuclea.silver.processed_base_boletos")

# Adicionar coluna de mês e ano nos boletos
boletos_df = boletos_df.withColumn("ano", year(col("dt_emissao"))) \
                       .withColumn("mes", month(col("dt_emissao")))

# Agregar boletos por id_pagador (cnpj) + ano + mes
boletos_agg = boletos_df.groupBy("id_pagador", "ano", "mes").agg(
    count("id_boleto").alias("qtde_boletos"),
    sum("vlr_nominal").alias("vlr_nominal_total"),
    avg("dias_atraso_pagamento").alias("media_atraso_pagamento")
)

# Juntar com auxiliar para trazer scores e indicadores
gold_df = boletos_agg.join(
    aux_df,
    boletos_agg["id_pagador"] == aux_df["id_cnpj"],
    "left"
).select(
    boletos_agg["id_pagador"].alias("id_cnpj"),
    "ano",
    "mes",
    "qtde_boletos",
    "vlr_nominal_total",
    "media_atraso_pagamento",
    "cd_cnae_prin",
    "uf",
    "sacado_indice_liquidez_1m",
    "cedente_indice_liquidez_1m",
    "score_materialidade_evolucao",
    "media_atraso_dias",
    "indicador_liquidez_quantitativo_3m",
    "share_vl_inad_pag_bol_6_a_15d",
    "score_quantidade_v2",
    "score_materialidade_v2"
)

# Salvar tabela gold
gold_df.write.format("delta").mode("overwrite").saveAsTable("challenge_nuclea.gold.evolucao_cliente_mes")

display(gold_df)