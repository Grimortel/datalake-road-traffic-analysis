-- ============================================================
-- Initialisation de la base de données weatherdb
-- Exécuté automatiquement au premier démarrage de PostgreSQL
-- ============================================================

-- Zone Staging : données temps réel (prévisions Open-Meteo)
CREATE TABLE IF NOT EXISTS weather_realtime (
    id          SERIAL PRIMARY KEY,
    timestamp   TIMESTAMPTZ    NOT NULL,
    latitude    FLOAT          NOT NULL,
    longitude   FLOAT          NOT NULL,
    temperature FLOAT,
    humidity    FLOAT,
    wind_speed  FLOAT,
    precipitation FLOAT,
    created_at  TIMESTAMPTZ    DEFAULT NOW()
);

-- Zone Staging : historique Open-Meteo Archive
CREATE TABLE IF NOT EXISTS weather_historical (
    id          SERIAL PRIMARY KEY,
    timestamp   TIMESTAMPTZ    NOT NULL,
    latitude    FLOAT          NOT NULL,
    longitude   FLOAT          NOT NULL,
    temperature FLOAT,
    humidity    FLOAT,
    wind_speed  FLOAT,
    precipitation FLOAT,
    created_at  TIMESTAMPTZ    DEFAULT NOW()
);

-- Zone Curated : scores d'anomalie météo
CREATE TABLE IF NOT EXISTS weather_curated (
    id          SERIAL PRIMARY KEY,
    timestamp   TIMESTAMPTZ    NOT NULL,
    latitude    FLOAT          NOT NULL,
    longitude   FLOAT          NOT NULL,
    variable    VARCHAR(32)    NOT NULL,
    value       FLOAT,
    mean        FLOAT,
    stddev      FLOAT,
    zscore      FLOAT,
    is_anomaly  BOOLEAN        DEFAULT FALSE,
    created_at  TIMESTAMPTZ    DEFAULT NOW()
);

-- Index pour les requêtes fréquentes
CREATE INDEX IF NOT EXISTS idx_realtime_ts        ON weather_realtime(timestamp);
CREATE INDEX IF NOT EXISTS idx_realtime_coords    ON weather_realtime(latitude, longitude);
CREATE INDEX IF NOT EXISTS idx_historical_ts      ON weather_historical(timestamp);
CREATE INDEX IF NOT EXISTS idx_historical_coords  ON weather_historical(latitude, longitude);
CREATE INDEX IF NOT EXISTS idx_curated_ts         ON weather_curated(timestamp);
CREATE INDEX IF NOT EXISTS idx_curated_coords     ON weather_curated(latitude, longitude);
CREATE INDEX IF NOT EXISTS idx_curated_anomaly    ON weather_curated(is_anomaly);

-- Contraintes d'unicité pour l'upsert idempotent
CREATE UNIQUE INDEX IF NOT EXISTS ux_realtime_ts_coords ON weather_realtime(timestamp, latitude, longitude);
CREATE UNIQUE INDEX IF NOT EXISTS ux_historical_ts_coords ON weather_historical(timestamp, latitude, longitude);
CREATE UNIQUE INDEX IF NOT EXISTS ux_curated_ts_coords_var ON weather_curated(timestamp, latitude, longitude, variable);
