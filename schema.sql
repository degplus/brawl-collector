-- Schema para BigQuery (brawl_stats)
-- Sandbox/free tier: use LOAD (CSV/JSON) para preencher dims, não INSERT/MERGE

-- Dimensão: jogadores monitorados (fonte: CSV upload com WRITE_TRUNCATE)
CREATE TABLE IF NOT EXISTS brawl_stats.dim_source_players (
  player_tag STRING NOT NULL,
  player_name STRING,
  region STRING NOT NULL,     -- APAC, EMEA, SA, NA
  team STRING,
  nation STRING,
  active BOOL NOT NULL,
  player_img_url STRING
)
OPTIONS(
  description='Jogadores monitorados para coleta (sem DML, carga via CSV)'
);

-- Dimensão: times (populado pelo coletor via streaming/append)
CREATE TABLE IF NOT EXISTS brawl_stats.dim_teams (
  team_tag STRING NOT NULL,
  team_name STRING,
  first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Dimensão: brawlers (populado pelo coletor)
CREATE TABLE IF NOT EXISTS brawl_stats.dim_brawlers (
  brawler_id INT64 NOT NULL,
  brawler_name STRING,
  first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Dimensão: mapas (populado pelo coletor)
CREATE TABLE IF NOT EXISTS brawl_stats.dim_maps (
  map_id INT64 NOT NULL,
  map_name STRING,
  game_mode STRING,
  first_seen_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
);

-- Fato: batalhas por jogador (particionada por dia, expiração 60 dias)
CREATE TABLE IF NOT EXISTS brawl_stats.fact_battle_players (
  game_id STRING NOT NULL,
  battle_time TIMESTAMP NOT NULL,
  player_tag STRING NOT NULL,
  player_name STRING,
  team_tag STRING,
  team_name STRING,
  brawler_id INT64,
  brawler_name STRING,
  brawler_power INT64,
  brawler_trophies INT64,
  map_id INT64,
  map_name STRING,
  game_mode STRING,
  battle_type STRING,
  battle_result STRING,
  battle_duration INT64,
  battle_rank INT64,
  battle_trophy_change INT64,
  source_player_tag STRING,
  source_player_name STRING,
  source_player_team STRING,
  source_player_region STRING,
  collected_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(battle_time)
OPTIONS(
  partition_expiration_days=60,
  description='Batalhas coletadas por jogador (últimos 60 dias)'
);
