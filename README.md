# Datalake Weather Analysis

Projet de datalake de bout en bout pour l'analyse météo et la détection d'anomalies climatiques.

## Objectif

Construire un datalake avec une décomposition en plusieurs zones et des contraintes techniques :

- **Zone Raw** : stockage objet MinIO (blob S3-compatible)
- **Zone Staging** : PostgreSQL + snapshots Parquet
- **Zone Curated** : PostgreSQL (scores d'anomalie z-score)

## Sources de données

| Source | Type | Description |
|--------|------|-------------|
| Open-Meteo Forecast API | **API** (appel HTTP temps réel) | Prévisions horaires, 100% gratuite, sans clé |
| Fichier CSV historique | **Fichier** (import local ou téléchargement) | Données historiques au format CSV |

## Architecture

- **Open-Meteo Forecast API** (`/v1/forecast`) — source par API
- **Fichier CSV** (export Open-Meteo bulk, ou tout dataset météo CSV) — source par fichier
- **MinIO** pour la zone raw (stockage blob S3-compatible)
- **PostgreSQL** pour staging et curated
- **Parquet** pour les snapshots intermédiaires
- **Prefect** pour l'orchestration

## Démarrage rapide

```bash
docker compose up -d --build
```

## Endpoints API Gateway (GET pour chaque zone)

| Endpoint | Zone | Description |
|----------|------|-------------|
| `GET /health` | - | Health check |
| `GET /raw/forecast` | Raw | Liste les objets bruts forecast (JSON) |
| `GET /raw/historical` | Raw | Liste les objets bruts historiques (CSV) |
| `GET /raw/forecast/object?name=...` | Raw | Récupère un objet brut |
| `GET /raw/historical/object?name=...` | Raw | Récupère un objet brut |
| `GET /staging/weather_realtime` | Staging | Données normalisées temps réel |
| `GET /staging/weather_historical` | Staging | Données normalisées historiques |
| `GET /curated/anomalies` | Curated | Anomalies détectées (z-score) |
| `POST /ingest` | Pipeline | Ingestion séquentielle complète |
| `POST /ingest-fast` | Pipeline | Ingestion parallèle |

## Ingestion raw

### Forecast — Source par API

Le script interroge l'API Open-Meteo Forecast et sauvegarde le JSON brut dans MinIO.

```bash
docker compose exec -T api python /app/src/ingestion/fetch_forecast_to_minio.py \
  --latitude 48.8566 --longitude 2.3522 --mock-on-error
```

L'option `--mock-on-error` génère un JSON mock compatible si l'API est inaccessible.

### Historical — Source par fichier (CSV)

Le script accepte un fichier CSV local, une URL distante, ou un mode mock :

```bash
# Depuis un fichier local
docker compose exec -T api python /app/src/ingestion/fetch_historical_to_minio.py \
  --path /app/data/historical_weather.csv

# Depuis une URL
docker compose exec -T api python /app/src/ingestion/fetch_historical_to_minio.py \
  --url https://bulk.open-meteo.com/export.csv

# Mode mock (génère un CSV de 720 lignes)
docker compose exec -T api python /app/src/ingestion/fetch_historical_to_minio.py --mock
```

## Pipeline

1. `raw/forecast` (JSON) et `raw/historical` (CSV) sont listés depuis MinIO
2. Les objets sont normalisés (température, humidité, vent, précipitations)
3. Les lignes sont insérées dans `weather_realtime` et `weather_historical`
4. Les données curated sont recalculées dans `weather_curated` (z-score vs moyenne historique)
5. Les snapshots Parquet sont écrits dans la zone staging MinIO

Le mode séquentiel et le mode parallèle sont exposés via l'API :

```bash
curl -X POST http://localhost:8000/ingest
curl -X POST http://localhost:8000/ingest-fast
```

## Score d'anomalie (curated)

Pour chaque point géographique, on calcule l'écart entre la valeur courante et la moyenne historique sur la même heure/mois, normalisé en z-score. Un score supérieur à 2 signale une anomalie.

## Contraintes techniques respectées

| Contrainte | Solution |
|------------|----------|
| Zone Raw : blob/elastic | MinIO (stockage objet S3-compatible) |
| Zone Staging : libre | PostgreSQL + Parquet |
| Zone Curated : libre | PostgreSQL |
| Orchestration : airflow/prefect/kubeflow | Prefect |
| Endpoints GET par zone | API FastAPI avec GET /raw, /staging, /curated |
| Source par API | Open-Meteo Forecast (appel HTTP) |
| Source par fichier | CSV historique (import fichier) |

## Notes

- Le projet fonctionne **entièrement sans clé API** grâce à Open-Meteo (licence CC BY 4.0).
- Les tables PostgreSQL sont idempotentes via des clés uniques `(timestamp, latitude, longitude)`.
- Le worker Prefect exécute le flow localement et le planifie toutes les 6 minutes.
- Open-Meteo couvre le monde entier — il suffit de changer les coordonnées.

## Validation

```bash
docker compose exec -T api python -m unittest discover -s tests -p 'test_*.py'
```

## Architecture détaillée

Voir [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) pour le schéma du flux de données.

## Démonstration

Pour un script de démo prêt à l'emploi, voir [docs/GUIDE_LANCEMENT_SOUTENANCE.md](docs/GUIDE_LANCEMENT_SOUTENANCE.md).
