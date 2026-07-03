from __future__ import annotations

import io
import json
import math
import os
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import lru_cache
from typing import Any

import pandas as pd
from dotenv import load_dotenv
from minio import Minio
from sqlalchemy import create_engine, text
from sqlalchemy.engine import Engine

load_dotenv()


@dataclass(frozen=True)
class Settings:
    database_url: str
    minio_endpoint: str
    minio_access_key: str
    minio_secret_key: str
    minio_secure: bool
    raw_bucket: str
    staging_bucket: str


def get_settings() -> Settings:
    return Settings(
        database_url=os.getenv("DATABASE_URL", "postgresql://weather:weather123@postgres:5432/weatherdb"),
        minio_endpoint=os.getenv("MINIO_ENDPOINT", "minio:9000"),
        minio_access_key=os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        minio_secret_key=os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
        minio_secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
        raw_bucket=os.getenv("MINIO_RAW_BUCKET", "raw-weather"),
        staging_bucket=os.getenv("MINIO_STAGING_BUCKET", "staging-weather"),
    )


@lru_cache(maxsize=1)
def get_minio_client() -> Minio:
    settings = get_settings()
    return Minio(
        settings.minio_endpoint,
        access_key=settings.minio_access_key,
        secret_key=settings.minio_secret_key,
        secure=settings.minio_secure,
    )


@lru_cache(maxsize=1)
def get_engine() -> Engine:
    return create_engine(get_settings().database_url, pool_pre_ping=True, future=True)


def ensure_bucket(client: Minio, bucket: str) -> None:
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)


def ensure_schema() -> None:
    engine = get_engine()
    statements = [
        """
        CREATE TABLE IF NOT EXISTS weather_realtime (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMPTZ NOT NULL,
            latitude FLOAT NOT NULL,
            longitude FLOAT NOT NULL,
            temperature FLOAT,
            humidity FLOAT,
            wind_speed FLOAT,
            precipitation FLOAT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS weather_historical (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMPTZ NOT NULL,
            latitude FLOAT NOT NULL,
            longitude FLOAT NOT NULL,
            temperature FLOAT,
            humidity FLOAT,
            wind_speed FLOAT,
            precipitation FLOAT,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
        """,
        """
        CREATE TABLE IF NOT EXISTS weather_curated (
            id SERIAL PRIMARY KEY,
            timestamp TIMESTAMPTZ NOT NULL,
            latitude FLOAT NOT NULL,
            longitude FLOAT NOT NULL,
            variable VARCHAR(32) NOT NULL,
            value FLOAT,
            mean FLOAT,
            stddev FLOAT,
            zscore FLOAT,
            is_anomaly BOOLEAN DEFAULT FALSE,
            created_at TIMESTAMPTZ DEFAULT NOW()
        )
        """,
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_realtime_ts_coords ON weather_realtime(timestamp, latitude, longitude)",
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_historical_ts_coords ON weather_historical(timestamp, latitude, longitude)",
        "CREATE UNIQUE INDEX IF NOT EXISTS ux_curated_ts_coords_var ON weather_curated(timestamp, latitude, longitude, variable)",
        "CREATE INDEX IF NOT EXISTS idx_realtime_ts ON weather_realtime(timestamp)",
        "CREATE INDEX IF NOT EXISTS idx_historical_ts ON weather_historical(timestamp)",
        "CREATE INDEX IF NOT EXISTS idx_curated_ts ON weather_curated(timestamp)",
        "CREATE INDEX IF NOT EXISTS idx_curated_anomaly ON weather_curated(is_anomaly)",
    ]
    with engine.begin() as connection:
        for statement in statements:
            connection.execute(text(statement))


def parse_timestamp(value: Any) -> datetime:
    if value is None or value == "":
        return datetime.now(timezone.utc)
    if isinstance(value, datetime):
        return value.astimezone(timezone.utc) if value.tzinfo else value.replace(tzinfo=timezone.utc)
    text_value = str(value).strip()
    if text_value.endswith("Z"):
        text_value = text_value[:-1] + "+00:00"
    parsed = datetime.fromisoformat(text_value)
    return parsed.astimezone(timezone.utc) if parsed.tzinfo else parsed.replace(tzinfo=timezone.utc)


# ─── Raw zone helpers ───────────────────────────────────────────────────────


def get_raw_objects(zone: str) -> list[dict[str, Any]]:
    if zone not in {"forecast", "historical"}:
        raise ValueError("zone must be forecast or historical")
    settings = get_settings()
    client = get_minio_client()
    rows: list[dict[str, Any]] = []
    for obj in client.list_objects(settings.raw_bucket, prefix=f"raw/{zone}/", recursive=True):
        rows.append(
            {
                "name": obj.object_name,
                "size": obj.size,
                "last_modified": obj.last_modified.isoformat() if getattr(obj, "last_modified", None) else None,
            }
        )
    return rows


def get_raw_object_bytes(name: str) -> bytes:
    client = get_minio_client()
    response = client.get_object(get_settings().raw_bucket, name)
    try:
        return response.read()
    finally:
        response.close()
        response.release_conn()


# ─── Mock data builders ─────────────────────────────────────────────────────


def build_mock_forecast_payload(latitude: float = 48.8566, longitude: float = 2.3522) -> dict[str, Any]:
    now = datetime.now(timezone.utc)
    hours = pd.date_range(now.replace(minute=0, second=0, microsecond=0), periods=24, freq="h")
    return {
        "latitude": latitude,
        "longitude": longitude,
        "generationtime_ms": 0.5,
        "utc_offset_seconds": 0,
        "hourly_units": {
            "time": "iso8601",
            "temperature_2m": "°C",
            "relative_humidity_2m": "%",
            "wind_speed_10m": "km/h",
            "precipitation": "mm",
        },
        "hourly": {
            "time": [h.strftime("%Y-%m-%dT%H:%M") for h in hours],
            "temperature_2m": [15.0 + i * 0.3 for i in range(24)],
            "relative_humidity_2m": [65.0 - i * 0.5 for i in range(24)],
            "wind_speed_10m": [10.0 + i * 0.2 for i in range(24)],
            "precipitation": [0.0 if i % 6 != 0 else 1.2 for i in range(24)],
        },
        "source": "mock",
    }


def upload_mock_forecast(latitude: float = 48.8566, longitude: float = 2.3522) -> str:
    client = get_minio_client()
    settings = get_settings()
    ensure_bucket(client, settings.raw_bucket)
    payload = build_mock_forecast_payload(latitude, longitude)
    now = datetime.now(timezone.utc)
    object_name = f"raw/forecast/{now.strftime('%Y-%m-%d')}/{now.strftime('%H-%M-%S')}.json"
    body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    client.put_object(settings.raw_bucket, object_name, io.BytesIO(body), len(body))
    return object_name


def build_mock_historical_payload(latitude: float = 48.8566, longitude: float = 2.3522) -> dict[str, Any]:
    hours = pd.date_range("2024-01-01", periods=720, freq="h")
    return {
        "latitude": latitude,
        "longitude": longitude,
        "hourly_units": {
            "time": "iso8601",
            "temperature_2m": "°C",
            "relative_humidity_2m": "%",
            "wind_speed_10m": "km/h",
            "precipitation": "mm",
        },
        "hourly": {
            "time": [h.strftime("%Y-%m-%dT%H:%M") for h in hours],
            "temperature_2m": [5.0 + 10.0 * math.sin(i * math.pi / 12) for i in range(720)],
            "relative_humidity_2m": [70.0 + 10.0 * math.cos(i * math.pi / 12) for i in range(720)],
            "wind_speed_10m": [12.0 + 5.0 * math.sin(i * math.pi / 6) for i in range(720)],
            "precipitation": [0.0 if i % 8 != 0 else 2.5 for i in range(720)],
        },
        "source": "mock",
    }


def upload_mock_historical(latitude: float = 48.8566, longitude: float = 2.3522) -> str:
    client = get_minio_client()
    settings = get_settings()
    ensure_bucket(client, settings.raw_bucket)
    payload = build_mock_historical_payload(latitude, longitude)
    now = datetime.now(timezone.utc)
    object_name = f"raw/historical/{now.strftime('%Y-%m-%d')}/{now.strftime('%H-%M-%S')}.json"
    body = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    client.put_object(settings.raw_bucket, object_name, io.BytesIO(body), len(body))
    return object_name


# ─── Normalization ──────────────────────────────────────────────────────────


def normalize_forecast_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    latitude = payload.get("latitude")
    longitude = payload.get("longitude")
    hourly = payload.get("hourly", {})
    times = hourly.get("time", [])
    temperatures = hourly.get("temperature_2m", [])
    humidities = hourly.get("relative_humidity_2m", [])
    winds = hourly.get("wind_speed_10m", [])
    precipitations = hourly.get("precipitation", [])
    rows: list[dict[str, Any]] = []
    for i, t in enumerate(times):
        rows.append({
            "timestamp": parse_timestamp(t),
            "latitude": float(latitude),
            "longitude": float(longitude),
            "temperature": temperatures[i] if i < len(temperatures) else None,
            "humidity": humidities[i] if i < len(humidities) else None,
            "wind_speed": winds[i] if i < len(winds) else None,
            "precipitation": precipitations[i] if i < len(precipitations) else None,
        })
    return rows


def normalize_historical_csv(raw_bytes: bytes) -> list[dict[str, Any]]:
    """Normalize a CSV file from the historical zone (file-based source)."""
    frame = pd.read_csv(io.BytesIO(raw_bytes))
    columns = {c.lower().strip(): c for c in frame.columns}
    ts_col = columns.get("timestamp") or columns.get("time") or list(frame.columns)[0]
    lat_col = columns.get("latitude")
    lon_col = columns.get("longitude")
    temp_col = columns.get("temperature_2m") or columns.get("temperature")
    hum_col = columns.get("relative_humidity_2m") or columns.get("humidity")
    wind_col = columns.get("wind_speed_10m") or columns.get("wind_speed")
    precip_col = columns.get("precipitation")

    rows: list[dict[str, Any]] = []
    for _, row in frame.iterrows():
        rows.append({
            "timestamp": parse_timestamp(row[ts_col] if ts_col else None),
            "latitude": float(row[lat_col]) if lat_col and pd.notna(row[lat_col]) else 48.8566,
            "longitude": float(row[lon_col]) if lon_col and pd.notna(row[lon_col]) else 2.3522,
            "temperature": float(row[temp_col]) if temp_col and pd.notna(row.get(temp_col)) else None,
            "humidity": float(row[hum_col]) if hum_col and pd.notna(row.get(hum_col)) else None,
            "wind_speed": float(row[wind_col]) if wind_col and pd.notna(row.get(wind_col)) else None,
            "precipitation": float(row[precip_col]) if precip_col and pd.notna(row.get(precip_col)) else None,
        })
    return rows


def normalize_historical_payload(payload: dict[str, Any]) -> list[dict[str, Any]]:
    """Normalize a JSON payload (Open-Meteo format) from the historical zone."""
    return normalize_forecast_payload(payload)


# ─── Parquet snapshots ──────────────────────────────────────────────────────


def upload_parquet_snapshot(table_name: str, frame: pd.DataFrame) -> str:
    client = get_minio_client()
    settings = get_settings()
    ensure_bucket(client, settings.staging_bucket)
    now = datetime.now(timezone.utc)
    object_name = f"staging/{table_name}/{now.strftime('%Y-%m-%dT%H-%M-%S')}.parquet"
    body_buffer = io.BytesIO()
    frame.to_parquet(body_buffer, index=False)
    body = body_buffer.getvalue()
    client.put_object(settings.staging_bucket, object_name, io.BytesIO(body), len(body))
    return object_name


# ─── Database upsert ────────────────────────────────────────────────────────


def upsert_rows(table_name: str, rows: list[dict[str, Any]], conflict_columns: list[str]) -> int:
    if not rows:
        return 0
    ensure_schema()
    columns = list(rows[0].keys())
    placeholders = ", ".join(f":{column}" for column in columns)
    update_columns = [column for column in columns if column not in conflict_columns and column != "id"]
    update_clause = ", ".join(f"{column} = EXCLUDED.{column}" for column in update_columns)
    sql = (
        f"INSERT INTO {table_name} ({', '.join(columns)}) "
        f"VALUES ({placeholders}) "
        f"ON CONFLICT ({', '.join(conflict_columns)}) DO UPDATE SET {update_clause}"
    )
    with get_engine().begin() as connection:
        connection.execute(text(sql), rows)
    return len(rows)


# ─── Object processing ──────────────────────────────────────────────────────


def process_forecast_object(object_name: str) -> dict[str, Any]:
    raw_bytes = get_raw_object_bytes(object_name)
    payload = json.loads(raw_bytes)
    rows = normalize_forecast_payload(payload)
    if not rows:
        return {"object": object_name, "inserted": 0, "parquet": None}
    parquet_object = upload_parquet_snapshot("weather_realtime", pd.DataFrame(rows))
    inserted = upsert_rows("weather_realtime", rows, ["timestamp", "latitude", "longitude"])
    return {"object": object_name, "inserted": inserted, "parquet": parquet_object}


def process_historical_object(object_name: str) -> dict[str, Any]:
    raw_bytes = get_raw_object_bytes(object_name)
    if object_name.endswith(".csv"):
        rows = normalize_historical_csv(raw_bytes)
    else:
        payload = json.loads(raw_bytes)
        rows = normalize_historical_payload(payload)
    if not rows:
        return {"object": object_name, "inserted": 0, "parquet": None}
    parquet_object = upload_parquet_snapshot("weather_historical", pd.DataFrame(rows))
    inserted = upsert_rows("weather_historical", rows, ["timestamp", "latitude", "longitude"])
    return {"object": object_name, "inserted": inserted, "parquet": parquet_object}


# ─── Staging query ──────────────────────────────────────────────────────────


def get_staging_rows(table_name: str, limit: int = 100, date: str | None = None, latitude: float | None = None, longitude: float | None = None) -> list[dict[str, Any]]:
    if table_name not in {"weather_realtime", "weather_historical"}:
        raise ValueError("table must be weather_realtime or weather_historical")
    filters: list[str] = []
    params: dict[str, Any] = {"limit": limit}
    if date:
        filters.append("timestamp::date = :date")
        params["date"] = date
    if latitude is not None:
        filters.append("latitude = :latitude")
        params["latitude"] = latitude
    if longitude is not None:
        filters.append("longitude = :longitude")
        params["longitude"] = longitude
    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
    sql = f"SELECT * FROM {table_name} {where_clause} ORDER BY timestamp DESC LIMIT :limit"
    ensure_schema()
    with get_engine().begin() as connection:
        result = connection.execute(text(sql), params)
        return [dict(row._mapping) for row in result]


# ─── Curated: anomaly detection ────────────────────────────────────────────


def calculate_curated_rows() -> list[dict[str, Any]]:
    ensure_schema()
    with get_engine().begin() as connection:
        realtime_rows = [
            dict(row._mapping)
            for row in connection.execute(text(
                "SELECT timestamp, latitude, longitude, temperature, humidity, wind_speed, precipitation FROM weather_realtime ORDER BY timestamp ASC"
            ))
        ]
        historical_rows = [
            dict(row._mapping)
            for row in connection.execute(text(
                "SELECT timestamp, latitude, longitude, temperature, humidity, wind_speed, precipitation FROM weather_historical ORDER BY timestamp ASC"
            ))
        ]

    if not historical_rows or not realtime_rows:
        return []

    variables = ["temperature", "humidity", "wind_speed", "precipitation"]
    stats: dict[str, dict[str, dict[str, float]]] = {}

    for row in historical_rows:
        lat_lon = f"{row['latitude']},{row['longitude']}"
        ts = parse_timestamp(row["timestamp"])
        key = f"{lat_lon}_{ts.month}_{ts.hour}"
        if key not in stats:
            stats[key] = {v: {"sum": 0.0, "sum_sq": 0.0, "count": 0} for v in variables}
        for var in variables:
            val = row.get(var)
            if val is not None:
                stats[key][var]["sum"] += val
                stats[key][var]["sum_sq"] += val * val
                stats[key][var]["count"] += 1

    curated_rows: list[dict[str, Any]] = []
    for row in realtime_rows:
        lat_lon = f"{row['latitude']},{row['longitude']}"
        ts = parse_timestamp(row["timestamp"])
        key = f"{lat_lon}_{ts.month}_{ts.hour}"
        stat = stats.get(key)
        if stat is None:
            continue
        for var in variables:
            val = row.get(var)
            if val is None:
                continue
            s = stat[var]
            if s["count"] < 2:
                continue
            mean = s["sum"] / s["count"]
            variance = (s["sum_sq"] / s["count"]) - (mean * mean)
            stddev = math.sqrt(max(variance, 0.0))
            if stddev == 0:
                continue
            zscore = (val - mean) / stddev
            curated_rows.append({
                "timestamp": ts,
                "latitude": row["latitude"],
                "longitude": row["longitude"],
                "variable": var,
                "value": val,
                "mean": round(mean, 4),
                "stddev": round(stddev, 4),
                "zscore": round(zscore, 4),
                "is_anomaly": abs(zscore) > 2.0,
            })

    return curated_rows


def refresh_curated() -> int:
    rows = calculate_curated_rows()
    return upsert_rows("weather_curated", rows, ["timestamp", "latitude", "longitude", "variable"])


def get_curated_rows(limit: int = 100, anomalies_only: bool = False) -> list[dict[str, Any]]:
    filters: list[str] = []
    params: dict[str, Any] = {"limit": limit}
    if anomalies_only:
        filters.append("is_anomaly = TRUE")
    where_clause = f"WHERE {' AND '.join(filters)}" if filters else ""
    sql = f"SELECT * FROM weather_curated {where_clause} ORDER BY ABS(zscore) DESC LIMIT :limit"
    ensure_schema()
    with get_engine().begin() as connection:
        result = connection.execute(text(sql), params)
        return [dict(row._mapping) for row in result]


# ─── Stats ──────────────────────────────────────────────────────────────────


def get_stats() -> dict[str, Any]:
    """Return metrics on bucket and table fill levels."""
    settings = get_settings()
    client = get_minio_client()

    # MinIO: count objects and total size per prefix
    raw_stats: dict[str, Any] = {}
    for zone in ("forecast", "historical"):
        count, total_bytes = 0, 0
        for obj in client.list_objects(settings.raw_bucket, prefix=f"raw/{zone}/", recursive=True):
            count += 1
            total_bytes += obj.size or 0
        raw_stats[zone] = {"objects": count, "total_bytes": total_bytes}

    staging_parquet: dict[str, Any] = {}
    for table in ("weather_realtime", "weather_historical"):
        count, total_bytes = 0, 0
        for obj in client.list_objects(settings.staging_bucket, prefix=f"staging/{table}/", recursive=True):
            count += 1
            total_bytes += obj.size or 0
        staging_parquet[table] = {"parquet_files": count, "total_bytes": total_bytes}

    # PostgreSQL: row counts per table
    ensure_schema()
    db_stats: dict[str, int] = {}
    with get_engine().begin() as connection:
        for table in ("weather_realtime", "weather_historical", "weather_curated"):
            row = connection.execute(text(f"SELECT COUNT(*) FROM {table}")).fetchone()
            db_stats[table] = row[0] if row else 0

    anomaly_count = 0
    with get_engine().begin() as connection:
        row = connection.execute(text("SELECT COUNT(*) FROM weather_curated WHERE is_anomaly = TRUE")).fetchone()
        anomaly_count = row[0] if row else 0

    return {
        "raw_bucket": {
            "bucket": settings.raw_bucket,
            "zones": raw_stats,
        },
        "staging_bucket": {
            "bucket": settings.staging_bucket,
            "parquet": staging_parquet,
        },
        "database": {
            "tables": db_stats,
            "anomalies_detected": anomaly_count,
        },
    }


# ─── Zone ingestion ─────────────────────────────────────────────────────────


def ingest_zone(zone: str, parallel: bool = False) -> dict[str, Any]:
    if zone not in {"forecast", "historical"}:
        raise ValueError("zone must be forecast or historical")
    objects = get_raw_objects(zone)
    if not objects:
        return {"zone": zone, "objects": 0, "rows": 0, "parquet": []}

    handler = process_forecast_object if zone == "forecast" else process_historical_object
    results: list[dict[str, Any]] = []
    if parallel and len(objects) > 1:
        with ThreadPoolExecutor(max_workers=min(8, len(objects))) as executor:
            futures = {executor.submit(handler, item["name"]): item["name"] for item in objects}
            for future in as_completed(futures):
                results.append(future.result())
    else:
        for item in objects:
            results.append(handler(item["name"]))
    return {
        "zone": zone,
        "objects": len(objects),
        "rows": sum(item["inserted"] for item in results),
        "parquet": [item["parquet"] for item in results],
        "details": results,
    }


# ─── Full pipeline orchestration ────────────────────────────────────────────


def ingest_all(parallel: bool = False) -> dict[str, Any]:
    started_at = time.perf_counter()
    ensure_schema()
    forecast = ingest_zone("forecast", parallel=parallel)
    historical = ingest_zone("historical", parallel=parallel)
    curated_rows = refresh_curated()
    total_ms = round((time.perf_counter() - started_at) * 1000.0, 2)
    return {
        "mode": "parallel" if parallel else "sequential",
        "total_ms": total_ms,
        "zones": {"forecast": forecast, "historical": historical},
        "curated_rows": curated_rows,
    }
