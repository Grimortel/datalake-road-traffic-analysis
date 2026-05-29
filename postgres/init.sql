-- ============================================================
-- Initialisation de la base de données trafficdb
-- Exécuté automatiquement au premier démarrage de PostgreSQL
-- ============================================================

-- Zone Staging : données temps réel TomTom
CREATE TABLE IF NOT EXISTS traffic_realtime (
    id          SERIAL PRIMARY KEY,
    segment_id  VARCHAR(64)    NOT NULL,
    timestamp   TIMESTAMPTZ    NOT NULL,
    speed       FLOAT,
    free_flow_speed FLOAT,
    confidence  FLOAT,
    created_at  TIMESTAMPTZ    DEFAULT NOW()
);

-- Zone Staging : historique Zenodo
CREATE TABLE IF NOT EXISTS traffic_historical (
    id          SERIAL PRIMARY KEY,
    sensor_id   VARCHAR(64)    NOT NULL,
    timestamp   TIMESTAMPTZ    NOT NULL,
    occupancy   FLOAT,
    created_at  TIMESTAMPTZ    DEFAULT NOW()
);

-- Zone Curated : scores de congestion
CREATE TABLE IF NOT EXISTS traffic_curated (
    id          SERIAL PRIMARY KEY,
    segment_id  VARCHAR(64)    NOT NULL,
    hour        TIMESTAMPTZ    NOT NULL,
    score       FLOAT,
    category    VARCHAR(16),   -- 'fluide', 'ralenti', 'saturé'
    baseline    FLOAT,
    delta       FLOAT,
    created_at  TIMESTAMPTZ    DEFAULT NOW()
);

-- Index pour les requêtes fréquentes
CREATE INDEX IF NOT EXISTS idx_realtime_segment  ON traffic_realtime(segment_id);
CREATE INDEX IF NOT EXISTS idx_realtime_ts       ON traffic_realtime(timestamp);
CREATE INDEX IF NOT EXISTS idx_historical_sensor ON traffic_historical(sensor_id);
CREATE INDEX IF NOT EXISTS idx_curated_segment   ON traffic_curated(segment_id);
CREATE INDEX IF NOT EXISTS idx_curated_hour      ON traffic_curated(hour);
CREATE INDEX IF NOT EXISTS idx_curated_category  ON traffic_curated(category);