#!/usr/bin/env python3
"""
Coletor Brawl Stars → BigQuery (GitHub Actions)
Lê players ativos de dim_source_players, busca batalhas recentes via API,
faz dedup por game_id e carrega na fact_battle_players.
"""
import os
import sys
from datetime import datetime, timezone
import requests
from google.cloud import bigquery
from google.api_core import exceptions as gcp_exceptions

# Configuração
BRAWL_API_BASE = "https://bsproxy.royaleapi.dev/v1"
BRAWL_API_TOKEN = os.environ.get("BRAWL_API_TOKEN")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BQ_DATASET = "brawl_stats"
BQ_TABLE_FACT = "fact_battle_players"
BQ_TABLE_SOURCE = "dim_source_players"

def fetch_active_players():
    """Retorna lista de dicts com players ativos (tag, name, region, team)."""
    client = bigquery.Client(project=GCP_PROJECT_ID)
    query = f"""
        SELECT player_tag, player_name, region, team
        FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE_SOURCE}`
        WHERE active = TRUE
    """
    rows = client.query(query).result()
    players = [dict(row) for row in rows]
    print(f"[INFO] {len(players)} players ativos encontrados")
    return players

def fetch_player_battles(player_tag: str):
    """Busca batalhas recentes do player via API Brawl Stars."""
    tag_clean = player_tag.replace("#", "")
    url = f"{BRAWL_API_BASE}/players/%23{tag_clean}/battlelog"
    headers = {"Authorization": f"Bearer {BRAWL_API_TOKEN}"}
    
    try:
        resp = requests.get(url, headers=headers, timeout=10)
        resp.raise_for_status()
        data = resp.json()
        return data.get("items", [])
    except requests.RequestException as e:
        print(f"[WARN] Erro ao buscar {player_tag}: {e}")
        return []

def parse_battle_to_rows(battle, source_player):
    """
    Transforma uma batalha em lista de dicts (uma linha por participante).
    
    Args:
        battle: dict da API (item do battlelog)
        source_player: dict {player_tag, player_name, region, team}
    
    Returns:
        list de dicts (formato fact_battle_players)
    """
    rows = []
    battle_time_str = battle.get("battleTime", "")
    if not battle_time_str:
        return rows
    
    # Converter battleTime ISO 8601 para timestamp
    try:
        battle_time = datetime.strptime(battle_time_str, "%Y%m%dT%H%M%S.%fZ").replace(tzinfo=timezone.utc)
    except ValueError:
        return rows
    
    event = battle.get("event", {})
    map_id = event.get("id")
    map_name = event.get("map")
    game_mode = event.get("mode", "")
    
    battle_info = battle.get("battle", {})
    battle_type = battle_info.get("type", "")
    battle_result = battle_info.get("result", "")
    battle_duration = battle_info.get("duration")
    battle_rank = battle_info.get("rank")
    battle_trophy_change = battle_info.get("trophyChange")
    
    # Gerar game_id único (battleTime + map_id)
    game_id = f"{battle_time_str}_{map_id}" if map_id else battle_time_str
    
    # Processar todos os participantes (teams ou players)
    all_participants = []
    
    # Estrutura 3v3 ou duo/solo
    if "teams" in battle_info:
        for team_list in battle_info["teams"]:
            for p in team_list:
                all_participants.append(p)
    elif "players" in battle_info:
        all_participants = battle_info.get("players", [])
    
    for participant in all_participants:
        p_tag = participant.get("tag", "")
        p_name = participant.get("name", "")
        
        brawler = participant.get("brawler", {})
        brawler_id = brawler.get("id")
        brawler_name = brawler.get("name", "")
        brawler_power = brawler.get("power")
        brawler_trophies = brawler.get("trophies")
        
        # Team tag/name (se existir)
        team_tag = participant.get("team", {}).get("tag") if isinstance(participant.get("team"), dict) else None
        team_name = participant.get("team", {}).get("name") if isinstance(participant.get("team"), dict) else None
        
        row = {
            "game_id": game_id,
            "battle_time": battle_time.isoformat(),
            "player_tag": p_tag,
            "player_name": p_name,
            "team_tag": team_tag,
            "team_name": team_name,
            "brawler_id": brawler_id,
            "brawler_name": brawler_name,
            "brawler_power": brawler_power,
            "brawler_trophies": brawler_trophies,
            "map_id": map_id,
            "map_name": map_name,
            "game_mode": game_mode,
            "battle_type": battle_type,
            "battle_result": battle_result,
            "battle_duration": battle_duration,
            "battle_rank": battle_rank,
            "battle_trophy_change": battle_trophy_change,
            "source_player_tag": source_player["player_tag"],
            "source_player_name": source_player.get("player_name"),
            "source_player_team": source_player.get("team"),
            "source_player_region": source_player.get("region"),
            "collected_at": datetime.now(timezone.utc).isoformat()
        }
        rows.append(row)
    
    return rows

def get_existing_game_ids(client, game_ids):
    """Retorna set de game_ids que já existem na fato."""
    if not game_ids:
        return set()
    
    game_ids_str = ", ".join([f"'{gid}'" for gid in game_ids])
    
    # Filtro de partition obrigatório (últimos 7 dias para cobrir coletas atrasadas)
    query = f"""
        SELECT DISTINCT game_id
        FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE_FACT}`
        WHERE game_id IN ({game_ids_str})
          AND battle_time >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    """
    
    try:
        rows = client.query(query).result()
        existing = {row.game_id for row in rows}
        print(f"[INFO] {len(existing)} game_ids já existem no BigQuery")
        return existing
    except Exception as e:
        print(f"[WARN] Erro ao consultar game_ids existentes: {e}")
        return set()

def load_to_bigquery(rows):
    """Carrega linhas na fact_battle_players usando load job (dedup por game_id)."""
    if not rows:
        print("[INFO] Nenhuma linha para carregar")
        return
    
    client = bigquery.Client(project=GCP_PROJECT_ID)
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE_FACT}"
    
    # Dedup intra-run: manter apenas 1 source por game_id (menor source_player_tag)
    game_map = {}
    for row in rows:
        gid = row["game_id"]
        if gid not in game_map:
            game_map[gid] = []
        game_map[gid].append(row)
    
    deduped_rows = []
    for gid, group in game_map.items():
        # Ordenar por source_player_tag e pegar todas as linhas do primeiro source
        group_sorted = sorted(group, key=lambda r: r["source_player_tag"])
        first_source = group_sorted[0]["source_player_tag"]
        deduped_rows.extend([r for r in group_sorted if r["source_player_tag"] == first_source])
    
    print(f"[INFO] Dedup intra-run: {len(rows)} → {len(deduped_rows)} linhas")
    
    # Dedup contra BigQuery: filtrar game_ids já existentes
    all_game_ids = list({r["game_id"] for r in deduped_rows})
    min_battle_time = min([r["battle_time"] for r in deduped_rows]) if deduped_rows else None
    existing_ids = get_existing_game_ids(client, all_game_ids, min_battle_time)

    
    final_rows = [r for r in deduped_rows if r["game_id"] not in existing_ids]
    
    if not final_rows:
        print("[INFO] Todas as batalhas já estão carregadas (dedup contra BQ)")
        return
    
    print(f"[INFO] Carregando {len(final_rows)} linhas novas no BigQuery...")
    
    # Load job
    job_config = bigquery.LoadJobConfig(
        schema=[
            bigquery.SchemaField("game_id", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("battle_time", "TIMESTAMP", mode="REQUIRED"),
            bigquery.SchemaField("player_tag", "STRING", mode="REQUIRED"),
            bigquery.SchemaField("player_name", "STRING"),
            bigquery.SchemaField("team_tag", "STRING"),
            bigquery.SchemaField("team_name", "STRING"),
            bigquery.SchemaField("brawler_id", "INT64"),
            bigquery.SchemaField("brawler_name", "STRING"),
            bigquery.SchemaField("brawler_power", "INT64"),
            bigquery.SchemaField("brawler_trophies", "INT64"),
            bigquery.SchemaField("map_id", "INT64"),
            bigquery.SchemaField("map_name", "STRING"),
            bigquery.SchemaField("game_mode", "STRING"),
            bigquery.SchemaField("battle_type", "STRING"),
            bigquery.SchemaField("battle_result", "STRING"),
            bigquery.SchemaField("battle_duration", "INT64"),
            bigquery.SchemaField("battle_rank", "INT64"),
            bigquery.SchemaField("battle_trophy_change", "INT64"),
            bigquery.SchemaField("source_player_tag", "STRING"),
            bigquery.SchemaField("source_player_name", "STRING"),
            bigquery.SchemaField("source_player_team", "STRING"),
            bigquery.SchemaField("source_player_region", "STRING"),
            bigquery.SchemaField("collected_at", "TIMESTAMP"),
        ],
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    
    try:
        job = client.load_table_from_json(final_rows, table_id, job_config=job_config)
        job.result()  # Espera completar
        print(f"[SUCCESS] {len(final_rows)} linhas carregadas com sucesso!")
    except Exception as e:
        print(f"[ERROR] Falha ao carregar no BigQuery: {e}")
        sys.exit(1)

def main():
    """Fluxo principal do coletor."""
    print(f"[START] Coletor Brawl Stars → BigQuery ({datetime.now(timezone.utc).isoformat()})")
    
    if not BRAWL_API_TOKEN:
        print("[ERROR] BRAWL_API_TOKEN não configurado")
        sys.exit(1)
    
    if not GCP_PROJECT_ID:
        print("[ERROR] GCP_PROJECT_ID não configurado")
        sys.exit(1)
    
    # 1. Buscar players ativos
    players = fetch_active_players()
    if not players:
        print("[WARN] Nenhum player ativo encontrado")
        return
    
    # 2. Coletar batalhas de todos os players
    all_rows = []
    for player in players:
        tag = player["player_tag"]
        print(f"[INFO] Buscando batalhas de {tag}...")
        battles = fetch_player_battles(tag)
        
        for battle in battles:
            rows = parse_battle_to_rows(battle, player)
            all_rows.extend(rows)
    
    print(f"[INFO] Total de linhas coletadas (bruto): {len(all_rows)}")
    
    # 3. Carregar no BigQuery (com dedup)
    load_to_bigquery(all_rows)
    
    print("[END] Coleta concluída")

if __name__ == "__main__":
    main()
