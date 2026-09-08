# challenge_nuclea
Projeto acadêmico desenvolvido em parceria com a **Núclea**, empresa líder em dados e tecnologia financeira no Brasil. O objetivo geral foi construir uma **engine de precificação dinâmica em tempo real** para Fundos de Investimento em Direitos Creditórios (FIDC) substituindo o processamento em lote (batch) legado por pipelines preditivos de baixa latência para otimização de risco e receita.
 
A arquitetura completa da solução envolve:
 
- **Feature Store NoSQL em memória (Redis Cloud)** para armazenamento e consumo de perfis preditivos e vetores de decisão, com latência P99 inferior a 1ms.
- **Microsserviços assíncronos (FastAPI)** com sanitização e validação estrita de dados via Regex e Pydantic.
- **Fluxo de Data Exhaust**, capturando logs operacionais estruturados que alimentam um ETL reverso integrado a um Data Warehouse corporativo para BI e backtesting.
- **Pipeline de dados históricos no Databricks** (documentado neste repositório), responsável por processar, limpar e agregar a base histórica de boletos e informações cadastrais que fundamentam os modelos preditivos e os scores de risco utilizados pela engine.
Este README cobre especificamente a **camada de processamento de dados no Databricks**, seguindo a arquitetura medallion (Bronze → Silver → Gold).
 
## Arquitetura de dados
 
```
challenge_nuclea.bronze.base_auxiliar        challenge_nuclea.bronze.base_boletos
                │                                          │
                ▼                                          ▼
        processamento_silver.py  (limpeza, validação e enriquecimento de flags)
                │                                          │
                ▼                                          ▼
challenge_nuclea.silver.processed_base_auxiliar   challenge_nuclea.silver.processed_base_boletos
                │                                          │
        ┌───────┴──────────────────────────────────────────┘
        ▼                                                   ▼
metricas_gold.py                                   serietemporal_gold.py
        │                                                   │
        ▼                                                   ▼
challenge_nuclea.gold.metricas_cnpj_12m       challenge_nuclea.gold.evolucao_cliente_mes
```
 
## Scripts
 
### 1. `processamento_silver.py` — Bronze → Silver
 
Responsável pela ingestão, avaliação de qualidade e limpeza das duas bases brutas:
 
**`base_auxiliar` (dados cadastrais/indicadores por CNPJ)**
- Remove registros sem `id_cnpj` (chave crítica).
- Trata nulos de UF com valor `UNKNOWN`.
- Preenche indicadores de liquidez e inadimplência nulos com `0.0` (ausência de dado).
- Preenche colunas de score (`score_materialidade_evolucao`, `score_quantidade_v2`, `score_materialidade_v2`) com a **mediana** da coluna.
- Preenche `media_atraso_dias` e `cd_cnae_prin` remanescentes.
**`base_boletos` (transações/títulos)**
- Cria a flag `is_paid` (pagamento identificado por `dt_pagamento` não nulo).
- Cria a flag `is_cleared` (baixa identificada por `vlr_baixa` e `tipo_baixa` preenchidos).
- Calcula `dias_atraso_pagamento` = `dt_pagamento - dt_vencimento` (negativo = pagamento antecipado, positivo = atraso, nulo = não pago).
- Para boletos não pagos, preenche `vlr_baixa = 0.0` e `tipo_baixa = 'NAO PAGO'`.
- Valida consistência de datas (`dt_emissao <= dt_vencimento`) na flag `valid_dates`.
**Saídas:**
- `challenge_nuclea.silver.processed_base_auxiliar`
- `challenge_nuclea.silver.processed_base_boletos`
### 2. `metricas_gold.py` — Silver → Gold (métricas agregadas de 12 meses)
 
Agrega o histórico de boletos pagos por CNPJ (`id_pagador`) em uma janela de 12 meses (`dt_vencimento` entre `2023-06-01` e `2024-05-31`), calculando:
 
- `total_boletos`: quantidade de boletos no período.
- `media_atraso_dias`: média de `dias_atraso_pagamento`.
- `volume_transacionado_brl`: soma de `vlr_nominal`.
- `score_fluxo`: score de risco de 10 a 85, calculado por **interpolação linear** sobre a média de atraso:
  - atraso médio `< 5` dias → score `10`
  - atraso médio `> 20` dias → score `85`
  - entre 5 e 20 dias → interpolação linear: `10 + (média_atraso - 5) * (85 - 10) / (20 - 5)`
**Saída:** `challenge_nuclea.gold.metricas_cnpj_12m` (usada como referência para checagem de qualidade e exemplos por faixa de score).
 
### 3. `serietemporal_gold.py` — Silver → Gold (série temporal mensal)
 
Constrói uma visão evolutiva mensal por cliente, útil para acompanhamento de tendência e alimentação de features temporais dos modelos preditivos:
 
- Agrega `base_boletos` por `id_pagador`, `ano` e `mês` de emissão (`qtde_boletos`, `vlr_nominal_total`, `media_atraso_pagamento`).
- Faz **left join** com `processed_base_auxiliar` para trazer indicadores cadastrais e scores (UF, CNAE, índices de liquidez, scores de materialidade/quantidade, share de inadimplência).
**Saída:** `challenge_nuclea.gold.evolucao_cliente_mes`.
 
## Stack utilizada nesta camada
 
- **Databricks** (notebooks PySpark)
- **Apache Spark / PySpark** (`pyspark.sql.functions`)
- **Delta Lake** (formato das tabelas Silver/Gold, `saveAsTable`)
- **Spark SQL** (agregações e criação de tabelas gold via `%sql`)
## Como executar
 
1. Garantir acesso ao catálogo `challenge_nuclea` (camadas `bronze`, `silver`, `gold`) no workspace Databricks.
2. Executar `processamento_silver.py` para gerar as tabelas Silver a partir das tabelas Bronze.
3. Executar `metricas_gold.py` para gerar a tabela agregada de métricas de 12 meses por CNPJ.
4. Executar `serietemporal_gold.py` para gerar a série temporal mensal por cliente.
> Os notebooks assumem que as tabelas Bronze (`base_auxiliar`, `base_boletos`) já existem no catálogo Unity Catalog do Databricks.
 
## Papel deste pipeline na engine de precificação
 
As tabelas Gold geradas aqui (`metricas_cnpj_12m` e `evolucao_cliente_mes`) representam a base histórica de treino/calibração dos modelos preditivos e do `score_fluxo`, que são posteriormente publicados na Feature Store em Redis para consumo de baixa latência pela engine de precificação em tempo real, além de servirem de insumo para o backtesting via o pipeline de Data Exhaust / ETL reverso.
 


