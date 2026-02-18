# update_dim_filters.py
# Recria a tabela auxiliar de filtros do dashboard
# Executado automaticamente após cada coleta de dados

from google.cloud import bigquery
import os

# Pega o project ID da variável de ambiente (igual ao main.py)
project_id = os.environ.get("GCP_PROJECT_ID", "brawl-sandbox")

client = bigquery.Client(project=project_id)

print("🔄 Atualizando dim_filters...")

query = """
CREATE OR REPLACE TABLE `brawl-sandbox.brawl_stats.dim_filters` AS
SELECT DISTINCT
    source_player_region,
    type,
    mode,
    map,
    player_team,
    player_name,
    brawler_name,
    DATE(battle_time) as battle_date
FROM `brawl-sandbox.brawl_stats.vw_battles_python`
ORDER BY 
    source_player_region, 
    type, 
    mode, 
    map, 
    player_team, 
    player_name, 
    brawler_name
"""

job = client.query(query)
job.result()  # Aguarda terminar

print("✅ dim_filters atualizada com sucesso!")
