# Prompt — Projet Datalake : Analyse du trafic routier & prédiction de congestion

## Contexte général

Tu es assistant technique sur un projet étudiant de niveau ING2 (école d'ingénieurs, 4e année). Le projet s'intitule **"Datalake & Data Integration — Analyse du trafic routier & prédiction de congestion"**. L'objectif est de construire un datalake complet, de l'ingestion brute jusqu'aux données enrichies, avec une API gateway exposant chaque zone, une orchestration automatisée, et un mode avancé avec parallélisme mesuré.

---

## Sources de données (primaires uniquement)

| Type | Source | Détail |
|------|---------|--------|
| API temps réel | **TomTom Traffic Flow API** | Endpoint `/flowSegmentData` — vitesse courante, vitesse free-flow, confidence par tronçon. Free tier : 2 500 req/jour. Clé API requise (developer.tomtom.com). |
| Fichier open data | **Zenodo — Monash Traffic Hourly** | DOI `10.5281/zenodo.4656132` — 862 séries temporelles horaires, taux d'occupation des autoroutes de la baie de San Francisco (2015-2016), format CSV. |

---

## Architecture du datalake — 3 zones

### Zone Raw
- Stockage **BLOB** via **MinIO** (compatible S3)
- JSON brut TomTom horodaté : `raw/tomtom/YYYY-MM-DD/HH-MM.json`
- CSV Zenodo brut : `raw/zenodo/traffic_hourly.csv`
- Aucune transformation — données telles quelles

### Zone Staging
- Stockage **PostgreSQL** + fichiers **Parquet** (via pyarrow)
- Normalisation TomTom : extraction de `speed`, `freeFlowSpeed`, `confidence`, `segment_id`, `timestamp`
- Normalisation Zenodo : reshape en `(timestamp, sensor_id, occupancy)`, dédoublonnage
- Tables : `traffic_realtime`, `traffic_historical`

### Zone Curated
- Stockage **PostgreSQL** (vue matérialisée)
- Calcul du **congestion score** par tronçon et par heure : `score = speed / freeFlowSpeed × 100`
- Catégories : `fluide (>80)` / `ralenti (40-80)` / `saturé (<40)`
- Jointure temps réel × baseline historique Zenodo → détection d'anomalies
- Table : `traffic_curated(segment_id, hour, score, category, baseline, delta)`

---

## Stack technique

| Couche | Outil |
|--------|-------|
| Langage | Python 3.11+ |
| Stockage objet (raw) | MinIO (S3-compatible) |
| Base de données | PostgreSQL |
| Sérialisation | Parquet (pyarrow), JSON |
| ORM / requêtes | SQLAlchemy |
| API gateway | FastAPI |
| Orchestration | Prefect (server Docker + agent local) |
| Conteneurisation | Docker + Docker Compose |
| Parallelisme | `concurrent.futures.ThreadPoolExecutor` |
| Gestion dépendances | pip + requirements.txt + venv |

---

## Endpoints API (FastAPI)

### GET — lecture par zone
| Endpoint | Description |
|----------|-------------|
| `GET /raw/{zone}` | Liste des objets MinIO (nom, taille, date) pour la zone `tomtom` ou `zenodo` |
| `GET /staging/{table}` | 100 dernières lignes de `traffic_realtime` ou `traffic_historical`. Filtres : `?date=&segment=` |
| `GET /curated/congestion` | Scores de congestion par segment. Filtres : `?hour=&category=&segment=` |

### POST — mode avancé
| Endpoint | Description |
|----------|-------------|
| `POST /ingest` | Déclenche la pipeline complète en séquentiel, retourne le temps total en ms |
| `POST /ingest-fast` | Pipeline parallèle via `ThreadPoolExecutor` sur N tronçons simultanés, retourne le temps + speedup vs séquentiel |

---

## Orchestration Prefect

Pipeline schedulée toutes les **6 minutes** :
```
fetch_tomtom → store_raw (MinIO) → normalize → store_staging (PostgreSQL) → compute_curated
```
- Retry ×3 sur les appels TomTom
- Logs persistants Prefect Server
- Déployé via Docker Compose avec Prefect Server + agent local

---

## Todo — phases de développement

Les phases sont à réaliser dans l'ordre. Coche mentalement les tâches terminées pour savoir où en est le projet.

### Phase 0 — Setup & environnement
- [ ] Créer le repo GitHub (structure : `/src`, `/docs`, `/notebooks`, README, .gitignore)
- [ ] Environnement Python : venv, requirements.txt (requests, pandas, pyarrow, fastapi, prefect, sqlalchemy, minio)
- [ ] Docker Compose : services MinIO, PostgreSQL, FastAPI, Prefect Server
- [ ] Créer compte TomTom Developer, récupérer clé API, tester `/flowSegmentData` sur un tronçon
- [ ] Télécharger le dataset Zenodo (DOI 10.5281/zenodo.4656132)

### Phase 1 — Zone Raw
- [ ] Configurer MinIO : bucket `raw-traffic/`, vérifier accès S3
- [ ] Script d'ingestion TomTom → sauvegarde JSON horodaté dans MinIO
- [ ] Script d'upload CSV Zenodo → MinIO
- [ ] Endpoint `GET /raw/{zone}`

### Phase 2 — Zone Staging
- [ ] Normalisation JSON TomTom → Parquet + insertion PostgreSQL (`traffic_realtime`)
- [ ] Normalisation CSV Zenodo → Parquet + insertion PostgreSQL (`traffic_historical`)
- [ ] Endpoint `GET /staging/{table}`

### Phase 3 — Zone Curated
- [ ] Calcul congestion score (speed / freeFlowSpeed × 100) + catégorisation
- [ ] Jointure temps réel × baseline historique
- [ ] Vue matérialisée `traffic_curated` dans PostgreSQL
- [ ] Endpoint `GET /curated/congestion`

### Phase 4 — Orchestration Prefect
- [ ] Installer Prefect Server (Docker), créer workspace + agent local
- [ ] Flow Prefect complet avec schedule 6 min
- [ ] Gestion erreurs, retry ×3, alertes sur échec

### Phase 5 — Mode avancé (endpoints POST)
- [ ] `POST /ingest` séquentiel avec mesure du temps
- [ ] `POST /ingest-fast` parallèle avec `ThreadPoolExecutor`, mesure speedup
- [ ] Benchmark sur 10/25/50 tronçons, graphe comparatif

### Phase 6 — Rapport & documentation
- [ ] Schéma d'architecture (zones + flux) pour le README
- [ ] README complet : installation Docker Compose, exemples curl
- [ ] Rapport technique PDF : choix techniques justifiés + résultats benchmark

---

## Consignes importantes

- Ne jamais transformer les données en zone Raw — stockage brut uniquement
- Le mode avancé doit **mesurer et retourner** le temps de traitement dans la réponse API
- Le rapport final est rendu sur le **repo GitHub** (rapport technique + code)
- Chaque zone doit avoir **au moins un endpoint GET** accessible
- Utiliser **Prefect** (pas Airflow ni autre) pour l'orchestration

---

## Comment utiliser ce prompt

Quand tu me poses une question ou me demandes de générer du code, réfère-toi à ce contexte pour :
1. Utiliser les bonnes technologies (pas d'Airflow, pas d'autre ORM que SQLAlchemy, etc.)
2. Respecter le nommage des tables, buckets et endpoints définis ci-dessus
3. Situer la tâche dans la bonne phase et vérifier que les phases précédentes sont supposées terminées
4. Adapter le niveau de détail à un projet étudiant ING2 documenté sur GitHub