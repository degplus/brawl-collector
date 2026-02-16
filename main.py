import os
import requests
from datetime import datetime
from google.cloud import bigquery

BRAWL_API_TOKEN = os.environ.get("BRAWL_API_TOKEN")
GCP_PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BQ_DATASET = "brawl_stats"
BQ_TABLE_DIM = "dim_source_players"
BQ_TABLE_FACT = "fact_battle_players"

BRAWL_API_BASE = "https://bsproxy.royaleapi.dev/v1"
HEADERS = {"Authorization": f"Bearer {BRAWL_API_TOKEN}"}

ALLOWED_TYPES = ["friendly", "tournament", "championshipChallenge"]
BLOCKED_MODES = [
    "soloShowdown", "duoShowdown", "trioShowdown",
    "knockout5V5", "gemGrab5V5", "brawlBall5V5", "wipeout5V5",
    "duels", "basketBrawl", "unknown"
]

def load_players_dict(client):
    query = f"""
        SELECT PL_TAG, PL_NAME, PL_CTEAM, PL_REGION, PL_NATION, PL_LINK
        FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE_DIM}`
        WHERE is_active = TRUE
    """
    rows = client.query(query).result()
    
    players_dict = {}
    for row in rows:
        players_dict[row.PL_TAG] = {
            "PL_NAME": row.PL_NAME,
            "PL_CTEAM": row.PL_CTEAM,
            "PL_REGION": row.PL_REGION,
            "PL_NATION": row.PL_NATION,
            "PL_LINK": row.PL_LINK
        }
    
    print(f"[INFO] {len(players_dict)} players carregados para enriquecimento")
    return players_dict

def get_active_players(client):
    query = f"""
        SELECT PL_TAG, PL_NAME, PL_CTEAM, PL_REGION
        FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE_DIM}`
        WHERE is_active = TRUE
    """
    rows = client.query(query).result()
    
    players = []
    for row in rows:
        players.append({
            "tag": row.PL_TAG,
            "name": row.PL_NAME,
            "team": row.PL_CTEAM,
            "region": row.PL_REGION
        })
    
    print(f"[INFO] {len(players)} players ativos encontrados")
    return players

def fetch_battles(player_tag):
    url = f"{BRAWL_API_BASE}/players/{player_tag.replace('#', '%23')}/battlelog"
    
    try:
        response = requests.get(url, headers=HEADERS, timeout=10)
        response.raise_for_status()
        data = response.json()
        return data.get("items", [])
    except Exception as e:
        print(f"[WARN] Erro ao buscar batalhas de {player_tag}: {e}")
        return []

def parse_battles(battles, source_player_tag, source_player_name, source_player_team, source_player_region, players_dict):
    rows = []
    
    for item in battles:
        try:
            battle = item.get("battle", {})
            event = item.get("event", {})
            battle_time_str = item.get("battleTime", "")
            
            if battle.get("type") not in ALLOWED_TYPES:
                continue
            
            if event.get("mode") in BLOCKED_MODES:
                continue
            
            if event.get("id") == 0:
                continue
            
            teams = battle.get("teams", [])
            if len(teams) != 2 or len(teams[0]) != 3 or len(teams[1]) != 3:
                continue
            
            battle_time = datetime.strptime(battle_time_str, "%Y%m%dT%H%M%S.%fZ")
            date_formatted = battle_time.strftime("%d/%m/%Y %H:%M:%S")
            
            game_id = f"{battle_time_str}_{event.get('id', 0)}"
            api_result = battle.get("result", "")
            
            star_player = battle.get("starPlayer")
            star_tag = star_player.get("tag") if star_player else None
            star_name = star_player.get("name") if star_player else None
            star_brawler_id = star_player.get("brawler", {}).get("id") if star_player else None
            star_brawler_name = star_player.get("brawler", {}).get("name") if star_player else None
            
            source_team_number = None
            for idx, team in enumerate(teams):
                for player in team:
                    if player.get("tag") == source_player_tag:
                        source_team_number = idx + 1
                        break
                if source_team_number:
                    break
            
            if not source_team_number:
                continue
            
            for team_idx, team in enumerate(teams):
                team_number = team_idx + 1
                
                for player in team:
                    player_tag = player.get("tag", "")
                    player_name = player.get("name", "")
                    brawler = player.get("brawler", {})
                    brawler_id = brawler.get("id")
                    brawler_name = brawler.get("name", "")
                    
                    pl_place = source_team_number if player_tag == source_player_tag else None
                    
                    if api_result == "draw":
                        pl_result = "draw"
                    else:
                        if team_number == source_team_number:
                            pl_result = api_result
                        else:
                            pl_result = "defeat" if api_result == "victory" else "victory"
                    
                    b_img = f"https://cdn.brawlify.com/brawlers/borderless/{brawler_id}.png" if brawler_id else None
                    m_img = f"https://cdn.brawlify.com/maps/regular/{event.get('id')}.png"
                    
                    player_data = players_dict.get(player_tag, {})
                    pl_team = player_data.get("PL_CTEAM")
                    pl_img = player_data.get("PL_LINK")
                    
                    row = {
                        "game_id": game_id,
                        "Game": None,
                        "Battle Time": battle_time.isoformat(),
                        "Event ID": event.get("id"),
                        "Mode": event.get("mode"),
                        "Map": event.get("map"),
                        "Type": battle.get("type"),
                        "Result": api_result,
                        "Duration": battle.get("duration"),
                        "Star Player Tag": star_tag,
                        "Star Player Name": star_name,
                        "Star Player Brawler ID": star_brawler_id,
                        "Star Player Brawler Name": star_brawler_name,
                        "TAG": player_tag,
                        "NAME": player_name,
                        "B_ID": brawler_id,
                        "B_Name": brawler_name,
                        "PL_NAME": source_player_name,
                        "PL_TAG": source_player_tag,
                        "PL_CTEAM": source_player_team,
                        "DATE": date_formatted,
                        "T_NUM": team_number,
                        "PL_PLACE": pl_place,
                        "PL_RESULT": pl_result,
                        "B_IMG": b_img,
                        "M_IMG": m_img,
                        "PL_TEAM": pl_team,
                        "PL_IMG": pl_img,
                        "source_player_region": source_player_region,
                        "collected_at": datetime.utcnow().isoformat()
                    }
                    rows.append(row)
        
        except Exception as e:
            print(f"[WARN] Erro ao processar batalha: {e}")
            continue
    
    return rows

def get_existing_game_ids(client, game_ids):
    if not game_ids:
        return set()
    
    game_ids_str = ", ".join([f"'{gid}'" for gid in game_ids])
    
    query = f"""
        SELECT DISTINCT game_id
        FROM `{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE_FACT}`
        WHERE game_id IN ({game_ids_str})
          AND `Battle Time` >= TIMESTAMP_SUB(CURRENT_TIMESTAMP(), INTERVAL 7 DAY)
    """
    
    try:
        rows = client.query(query).result()
        existing = {row.game_id for row in rows}
        print(f"[INFO] {len(existing)} game_ids ja existem no BigQuery")
        return existing
    except Exception as e:
        print(f"[WARN] Erro ao consultar game_ids existentes: {e}")
        return set()

def load_to_bigquery(all_rows):
    if not all_rows:
        print("[INFO] Nenhuma linha para carregar")
        return
    
    client = bigquery.Client(project=GCP_PROJECT_ID)
    
    seen = set()
    deduped_rows = []
    for row in all_rows:
        key = (row["game_id"], row["TAG"])
        if key not in seen:
            seen.add(key)
            deduped_rows.append(row)
    
    print(f"[INFO] Dedup intra-run: {len(all_rows)} -> {len(deduped_rows)} linhas")
    
    all_game_ids = list({r["game_id"] for r in deduped_rows})
    existing_ids = get_existing_game_ids(client, all_game_ids)
    
    new_rows = [r for r in deduped_rows if r["game_id"] not in existing_ids]
    
    if not new_rows:
        print("[INFO] Nenhuma linha nova para carregar")
        return
    
    print(f"[INFO] Carregando {len(new_rows)} linhas novas no BigQuery...")
    
    schema = [
        bigquery.SchemaField("game_id", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("Game", "INTEGER"),
        bigquery.SchemaField("Battle Time", "TIMESTAMP", mode="REQUIRED"),
        bigquery.SchemaField("Event ID", "INTEGER"),
        bigquery.SchemaField("Mode", "STRING"),
        bigquery.SchemaField("Map", "STRING"),
        bigquery.SchemaField("Type", "STRING"),
        bigquery.SchemaField("Result", "STRING"),
        bigquery.SchemaField("Duration", "INTEGER"),
        bigquery.SchemaField("Star Player Tag", "STRING"),
        bigquery.SchemaField("Star Player Name", "STRING"),
        bigquery.SchemaField("Star Player Brawler ID", "INTEGER"),
        bigquery.SchemaField("Star Player Brawler Name", "STRING"),
        bigquery.SchemaField("TAG", "STRING", mode="REQUIRED"),
        bigquery.SchemaField("NAME", "STRING"),
        bigquery.SchemaField("B_ID", "INTEGER"),
        bigquery.SchemaField("B_Name", "STRING"),
        bigquery.SchemaField("PL_NAME", "STRING"),
        bigquery.SchemaField("PL_TAG", "STRING"),
        bigquery.SchemaField("PL_CTEAM", "STRING"),
        bigquery.SchemaField("DATE", "STRING"),
        bigquery.SchemaField("T_NUM", "INTEGER"),
        bigquery.SchemaField("PL_PLACE", "INTEGER"),
        bigquery.SchemaField("PL_RESULT", "STRING"),
        bigquery.SchemaField("B_IMG", "STRING"),
        bigquery.SchemaField("M_IMG", "STRING"),
        bigquery.SchemaField("PL_TEAM", "STRING"),
        bigquery.SchemaField("PL_IMG", "STRING"),
        bigquery.SchemaField("source_player_region", "STRING"),
        bigquery.SchemaField("collected_at", "TIMESTAMP"),
    ]
    
    table_id = f"{GCP_PROJECT_ID}.{BQ_DATASET}.{BQ_TABLE_FACT}"
    
    job_config = bigquery.LoadJobConfig(
        schema=schema,
        write_disposition=bigquery.WriteDisposition.WRITE_APPEND,
    )
    
    job = client.load_table_from_json(new_rows, table_id, job_config=job_config)
    job.result()
    
    print(f"[SUCCESS] {len(new_rows)} linhas carregadas com sucesso!")

def main():
    print(f"[START] Coletor Brawl Stars -> BigQuery ({datetime.utcnow().isoformat()})")
    
    client = bigquery.Client(project=GCP_PROJECT_ID)
    
    players_dict = load_players_dict(client)
    
    players = get_active_players(client)
    
    all_rows = []
    for player in players:
        print(f"[INFO] Buscando batalhas de {player['tag']}...")
        battles = fetch_battles(player['tag'])
        
        if battles:
            rows = parse_battles(
                battles,
                player['tag'],
                player['name'],
                player['team'],
                player['region'],
                players_dict
            )
            all_rows.extend(rows)
    
    print(f"[INFO] Total de linhas coletadas (bruto): {len(all_rows)}")
    
    load_to_bigquery(all_rows)
    
    print("[END] Coleta concluida")

if __name__ == "__main__":
    main()
