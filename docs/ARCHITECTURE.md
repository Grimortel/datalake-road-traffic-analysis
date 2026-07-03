# Architecture

```mermaid
flowchart LR
    Forecast[Open-Meteo Forecast API\n source API] --> RawForecast[MinIO raw/forecast\n JSON]
    HistFile[Fichier CSV historique\n source fichier] --> RawHistorical[MinIO raw/historical\n CSV]

    RawForecast --> Normalize[Normalization service]
    RawHistorical --> Normalize

    Normalize --> StagingDB[(PostgreSQL staging)]
    Normalize --> StagingParquet[Parquet snapshots\n MinIO staging]

    StagingDB --> Anomaly[Anomaly detection\n z-score]
    Anomaly --> CuratedDB[(PostgreSQL curated)]

    API[FastAPI gateway\n GET endpoints] --> RawForecast
    API --> RawHistorical
    API --> StagingDB
    API --> CuratedDB

    Prefect[Prefect flow\n toutes les 6 min] --> Normalize
    Prefect --> Anomaly
```

## Sources de données

| Source | Type | Format brut |
|--------|------|-------------|
| Open-Meteo Forecast API | **Appel API HTTP** | JSON |
| Fichier CSV historique | **Import fichier** | CSV |

## Zones de données

| Zone | Stockage | Contenu |
|------|----------|---------|
| Raw | MinIO (`raw-weather`) | JSON (forecast) + CSV (historical) bruts |
| Staging | PostgreSQL + Parquet | Données normalisées (température, humidité, vent, précipitations) |
| Curated | PostgreSQL | Z-scores et détection d'anomalies |

## Notes

- Raw est immutable et stocké tel quel.
- Staging stocke les enregistrements normalisés et les snapshots Parquet.
- Curated calcule le z-score par variable pour chaque point géographique (heure + mois).
- Un z-score > 2 signale une anomalie climatique.
