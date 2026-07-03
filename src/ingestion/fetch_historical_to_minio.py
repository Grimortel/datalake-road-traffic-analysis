#!/usr/bin/env python3
"""Ingest historical weather data FROM FILE (CSV) into MinIO raw zone.

This is the FILE-BASED source (as opposed to the API-based forecast source).
The CSV can come from:
  - A local file path
  - A remote URL (e.g. Open-Meteo bulk CSV export, Zenodo dataset)
  - Mock generation for demo/testing

Usage:
  python src/ingestion/fetch_historical_to_minio.py --path data/historical_weather.csv
  python src/ingestion/fetch_historical_to_minio.py --url https://bulk.open-meteo.com/...
  python src/ingestion/fetch_historical_to_minio.py --mock
"""

from __future__ import annotations

import argparse
import datetime
import io
import sys
from pathlib import Path

import pandas as pd
import requests
from dotenv import load_dotenv

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from src.pipeline.service import ensure_bucket, get_minio_client, get_settings

load_dotenv()


def download_csv(url: str) -> bytes:
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.content


def build_mock_historical_csv(latitude: float = 48.8566, longitude: float = 2.3522) -> bytes:
    """Generate a mock CSV with hourly weather data for 30 days."""
    import math

    hours = pd.date_range("2024-01-01", periods=720, freq="h", tz="UTC")
    rows = []
    for i, ts in enumerate(hours):
        rows.append({
            "timestamp": ts.strftime("%Y-%m-%dT%H:%M:%SZ"),
            "latitude": latitude,
            "longitude": longitude,
            "temperature_2m": round(5.0 + 10.0 * math.sin(i * math.pi / 12), 2),
            "relative_humidity_2m": round(70.0 + 10.0 * math.cos(i * math.pi / 12), 2),
            "wind_speed_10m": round(12.0 + 5.0 * math.sin(i * math.pi / 6), 2),
            "precipitation": round(0.0 if i % 8 != 0 else 2.5, 2),
        })
    frame = pd.DataFrame(rows)
    return frame.to_csv(index=False).encode("utf-8")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", help="Local CSV file path")
    parser.add_argument("--url", help="Remote CSV URL to download")
    parser.add_argument("--mock", action="store_true", help="Generate mock CSV data")
    parser.add_argument("--latitude", type=float, default=48.8566)
    parser.add_argument("--longitude", type=float, default=2.3522)
    args = parser.parse_args()

    if args.path:
        print(f"Reading CSV from local file: {args.path}")
        with open(args.path, "rb") as f:
            raw_bytes = f.read()
    elif args.url:
        print(f"Downloading CSV from: {args.url}")
        raw_bytes = download_csv(args.url)
    elif args.mock:
        print("Generating mock historical CSV")
        raw_bytes = build_mock_historical_csv(args.latitude, args.longitude)
    else:
        print("No source provided, generating mock CSV")
        raw_bytes = build_mock_historical_csv(args.latitude, args.longitude)

    settings = get_settings()
    client = get_minio_client()
    ensure_bucket(client, settings.raw_bucket)

    now = datetime.datetime.now(datetime.timezone.utc)
    object_name = f"raw/historical/{now.strftime('%Y-%m-%d')}/{now.strftime('%H-%M-%S')}.csv"
    client.put_object(settings.raw_bucket, object_name, io.BytesIO(raw_bytes), len(raw_bytes))
    print(f"Uploaded to s3://{settings.raw_bucket}/{object_name}")


if __name__ == "__main__":
    main()
