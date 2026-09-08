# Databricks notebook source
# DBTITLE 1,Verificação dos dados armazenados na tabela gold
# MAGIC %sql
# MAGIC -- Verifica os dados armazenados e mostra estatísticas
# MAGIC SELECT 
# MAGIC   COUNT(*) as total_cnpjs,
# MAGIC   ROUND(AVG(media_atraso_dias), 2) as media_atraso_geral,
# MAGIC   ROUND(SUM(volume_transacionado_brl), 2) as volume_total_brl,
# MAGIC   MIN(score_fluxo) as score_minimo,
# MAGIC   MAX(score_fluxo) as score_maximo,
# MAGIC   ROUND(AVG(score_fluxo), 2) as score_medio,
# MAGIC   COUNT(CASE WHEN score_fluxo = 10 THEN 1 END) as cnpjs_score_10,
# MAGIC   COUNT(CASE WHEN score_fluxo > 10 AND score_fluxo < 85 THEN 1 END) as cnpjs_score_intermediario,
# MAGIC   COUNT(CASE WHEN score_fluxo = 85 THEN 1 END) as cnpjs_score_85
# MAGIC FROM challenge_nuclea.gold.metricas_cnpj_12m

# COMMAND ----------

# DBTITLE 1,Agregação por CNPJ com métricas e score_fluxo
# MAGIC %sql
# MAGIC -- Agregação por CNPJ dos últimos 12 meses
# MAGIC -- Calcula média de atraso, volume transacionado e score_fluxo
# MAGIC
# MAGIC SELECT 
# MAGIC   id_pagador as cnpj,
# MAGIC   COUNT(*) as total_boletos,
# MAGIC   AVG(dias_atraso_pagamento) as media_atraso_dias,
# MAGIC   SUM(vlr_nominal) as volume_transacionado_brl,
# MAGIC   CASE 
# MAGIC     WHEN AVG(dias_atraso_pagamento) > 20 THEN 85
# MAGIC     WHEN AVG(dias_atraso_pagamento) < 5 THEN 10
# MAGIC     ELSE 10 + (AVG(dias_atraso_pagamento) - 5) * (85 - 10) / (20 - 5)  -- interpolação linear entre 5 e 20 dias
# MAGIC   END as score_fluxo
# MAGIC FROM challenge_nuclea.silver.processed_base_boletos
# MAGIC WHERE 
# MAGIC   dt_vencimento >= '2023-06-01'  -- últimos 12 meses a partir de 2024-05-31
# MAGIC   AND dt_vencimento <= '2024-05-31'
# MAGIC   AND is_paid = true  -- considera apenas boletos pagos para cálculo do atraso
# MAGIC GROUP BY id_pagador
# MAGIC ORDER BY volume_transacionado_brl DESC

# COMMAND ----------

# DBTITLE 1,Exemplos de CNPJs por categoria de score
# MAGIC %sql
# MAGIC -- Mostra exemplos de CNPJs para cada categoria de score_fluxo
# MAGIC (
# MAGIC   SELECT 'Score 10 (atraso < 5 dias)' as categoria, *
# MAGIC   FROM challenge_nuclea.gold.metricas_cnpj_12m
# MAGIC   WHERE score_fluxo = 10
# MAGIC   ORDER BY volume_transacionado_brl DESC
# MAGIC   LIMIT 5
# MAGIC )
# MAGIC UNION ALL
# MAGIC (
# MAGIC   SELECT 'Score Intermediário (5-20 dias)' as categoria, *
# MAGIC   FROM challenge_nuclea.gold.metricas_cnpj_12m
# MAGIC   WHERE score_fluxo > 10 AND score_fluxo < 85
# MAGIC   ORDER BY volume_transacionado_brl DESC
# MAGIC   LIMIT 5
# MAGIC )
# MAGIC UNION ALL
# MAGIC (
# MAGIC   SELECT 'Score 85 (atraso > 20 dias)' as categoria, *
# MAGIC   FROM challenge_nuclea.gold.metricas_cnpj_12m
# MAGIC   WHERE score_fluxo = 85
# MAGIC   ORDER BY volume_transacionado_brl DESC
# MAGIC   LIMIT 5
# MAGIC )

# COMMAND ----------

# DBTITLE 1,Cria tabela gold com métricas por CNPJ
# MAGIC %sql
# MAGIC -- Cria ou substitui a tabela gold com as métricas agregadas por CNPJ
# MAGIC CREATE OR REPLACE TABLE challenge_nuclea.gold.metricas_cnpj_12m AS
# MAGIC SELECT 
# MAGIC   id_pagador as cnpj,
# MAGIC   COUNT(*) as total_boletos,
# MAGIC   ROUND(AVG(dias_atraso_pagamento), 2) as media_atraso_dias,
# MAGIC   ROUND(SUM(vlr_nominal), 2) as volume_transacionado_brl,
# MAGIC   CAST(
# MAGIC     CASE 
# MAGIC       WHEN AVG(dias_atraso_pagamento) > 20 THEN 85
# MAGIC       WHEN AVG(dias_atraso_pagamento) < 5 THEN 10
# MAGIC       ELSE 10 + (AVG(dias_atraso_pagamento) - 5) * (85 - 10) / (20 - 5)
# MAGIC     END AS INT
# MAGIC   ) as score_fluxo,
# MAGIC   CURRENT_TIMESTAMP() as dt_processamento
# MAGIC FROM challenge_nuclea.silver.processed_base_boletos
# MAGIC WHERE 
# MAGIC   dt_vencimento >= '2023-06-01'  -- últimos 12 meses
# MAGIC   AND dt_vencimento <= '2024-05-31'
# MAGIC   AND is_paid = true
# MAGIC GROUP BY id_pagador
# MAGIC ORDER BY volume_transacionado_brl DESC