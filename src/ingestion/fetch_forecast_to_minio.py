#!/usr/bin/env python3
"""Fetch Open-Meteo Forecast data and upload raw JSON to MinIO.

Usage:
  python src/ingestion/fetch_forecast_to_minio.py --latitude 48.8566 --longitude 2.3522
  python src/ingestion/fetch_forecast_to_minio.py --latitude 48.8566 --longitude 2.3522 --mock-on-error
"""

from __future__ import annotations

import argparse
import datetime
import io
import json
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from src.pipeline.service import build_mock_forecast_payload, ensure_bucket, get_minio_client, get_settings

load_dotenv()

OPENMETEO_FORECAST_URL = "https://api.open-meteo.com/v1/forecast"


def fetch_forecast(latitude: float, longitude: float) -> dict:
    params = {
        "latitude": latitude,
        "longitude": longitude,
        "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation",
        "timezone": "UTC",
    }
    resp = requests.get(OPENMETEO_FORECAST_URL, params=params, timeout=15)
    resp.raise_for_status()
    return resp.json()


def upload_to_minio(data: bytes, object_name: str) -> None:
    settings = get_settings()
    client = get_minio_client()
    ensure_bucket(client, settings.raw_bucket)
    client.put_object(settings.raw_bucket, object_name, io.BytesIO(data), length=len(data))


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--latitude", type=float, default=48.8566)
    parser.add_argument("--longitude", type=float, default=2.3522)
    parser.add_argument("--mock-on-error", action="store_true", help="Upload mock payload if API is unreachable")
    args = parser.parse_args()

    try:
        payload = fetch_forecast(args.latitude, args.longitude)
    except Exception as exc:
        if not args.mock_on_error:
            raise
        print(f"Open-Meteo unreachable ({exc}), generating mock data")
        payload = build_mock_forecast_payload(args.latitude, args.longitude)

    now = datetime.datetime.now(datetime.timezone.utc)
    object_name = f"raw/forecast/{now.strftime('%Y-%m-%d')}/{now.strftime('%H-%M-%S')}.json"
    raw_bytes = json.dumps(payload, ensure_ascii=False, indent=2).encode("utf-8")
    upload_to_minio(raw_bytes, object_name)
    print(f"Uploaded to s3://{get_settings().raw_bucket}/{object_name}")


if __name__ == "__main__":
    main()
