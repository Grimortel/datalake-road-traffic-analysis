# Rapport technique : Datalake Weather Analysis

## Le but

Construire un datalake de bout en bout qui ingère de la météo depuis deux sources différentes, la range dans trois zones, détecte des anomalies, et expose tout via une API.

## Pourquoi ce sujet ?

Simple : les données météo sont gratuites, sans clé API, et disponibles partout grâce à Open-Meteo.

## Les sources

- **API** : Open-Meteo Forecast — appel HTTP, JSON en retour, prévisions horaires sur 24h.
- **Fichier** : un CSV historique (généré en mock ou téléchargé depuis Open-Meteo bulk).

## L'architecture en 3 zones

On a repris la structure classique **raw / staging / curated**

| Zone | Techno | Pourquoi |
|------|--------|----------|
| Raw | **MinIO** | S3-compatible, gratuit, local. Si demain on passe sur AWS S3, on change juste l'endpoint. |
| Staging | **PostgreSQL + Parquet** | Postgres pour requêter facilement, Parquet pour garder un snapshot compressé et rejouable. |
| Curated | **PostgreSQL** | Les résultats analytiques (z-scores) sont structurés, autant les mettre en SQL. |

L'énoncé impose blob ou Elasticsearch pour la raw → MinIO correspond à la partie blob.

## Le pipeline

C'est du Python classique dans `src/pipeline/service.py`. Le flow :

1. On liste les objets bruts dans MinIO
2. On les normalise (température, humidité, vent, précipitations → colonnes unifiées)
3. On sauvegarde un snapshot Parquet
4. On upsert dans Postgres (`ON CONFLICT DO UPDATE` → idempotent, on peut rejouer sans doublon)
5. On calcule les z-scores pour la zone curated

Pour la détection d'anomalies, on a choisi le **z-score** plutôt qu'un Isolation Forest ou un autoencoder. Parce que c'est simple, interprétable, et ça ne demande pas d'entraînement. Pour un projet portfolio, on préfère quelque chose qu'on peut expliquer en soutenance en 30 secondes.

## L'orchestration : Prefect (pas Airflow)

On a choisi **Prefect** parce que :
- Un seul container au lieu de 3 (Airflow = webserver + scheduler + worker)
- Python natif, pas de DSL
- Retries et logging intégrés
- Plus léger à démarrer pour un projet étudiant

Le flow tourne toutes les 6 minutes et déclenche `ingest_all()`.

## L'API

FastAPI, parce que c'est rapide à écrire, auto-documenté (Swagger sur `/docs`). Les endpoints demandés sont tous là :

- `GET /health`, `GET /stats`
- `GET /raw/{zone}`, `GET /staging/{table}`, `GET /curated/anomalies`
- `POST /ingest`, `POST /ingest-fast` (niveau avancé)

## Le niveau avancé : `/ingest` vs `/ingest-fast`

L'énoncé demande une accélération d'au moins 30% donc notre approche est :

- **`/ingest`** : boucle séquentielle simple, un objet après l'autre.
- **`/ingest-fast`** : `ThreadPoolExecutor` avec 8 workers.

Pourquoi du threading et pas de l'asyncio ou du multiprocessing ? Parce que notre pipeline est **I/O-bound** (attente réseau MinIO + PostgreSQL). Les threads suffisent, et c'est infiniment plus simple à écrire que de tout convertir en async. Le gain observé dépasse les 30% demandés dès qu'il y a plusieurs objets à traiter.

## Ce qu'on a évité

- **Kafka / Spark** : overkill pour ce volume de données.
- **Un CNN ou LSTM pour les anomalies** : Un peu plus complexe et demandait plus de temps pour bien comprendre l'ensemble.
- **Airflow** : trop lourd à installer et configurer pour ce qu'on en fait.