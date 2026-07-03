#!/usr/bin/env python3
"""Fetch a TomTom flowSegmentData JSON and upload it raw to MinIO.

Usage:
  python src/ingestion/fetch_tomtom_to_minio.py --point "lat,lon"

Environment (recommended): set values in .env
  TOMTOM_API_KEY, MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_SECURE
"""

import argparse
import datetime
import io
import json
import os
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from src.pipeline.service import build_mock_tomtom_payload, ensure_bucket, get_minio_client, get_settings

load_dotenv()

TOMTOM_API_KEY = os.getenv("TOMTOM_API_KEY")
TT_BASE = os.getenv("TOMTOM_BASE_URL", "https://api.tomtom.com/traffic/services/4")


def fetch_tomtom(point: str) -> dict:
    if not TOMTOM_API_KEY:
        raise RuntimeError("TOMTOM_API_KEY not set in environment")
    # Example endpoint, using flowSegmentData absolute JSON format
    url = f"{TT_BASE}/flowSegmentData/absolute/10/json"
    params = {"point": point, "key": TOMTOM_API_KEY}
    resp = requests.get(url, params=params, timeout=10)
    if resp.status_code >= 400:
        raise RuntimeError(f"TomTom API error {resp.status_code}: {resp.text}")
    return resp.json()


def upload_to_minio(data: bytes, object_name: str) -> None:
    settings = get_settings()
    client = get_minio_client()
    ensure_bucket(client, settings.raw_bucket)
    client.put_object(settings.raw_bucket, object_name, io.BytesIO(data), length=len(data))


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--point", required=True, help="latitude,longitude (e.g. 37.7749,-122.4194)")
    p.add_argument("--mock-on-error", action="store_true", help="Upload a mock payload if TomTom returns an error")
    args = p.parse_args()

    try:
        obj = fetch_tomtom(args.point)
    except Exception as exc:
        if not args.mock_on_error:
            raise
        obj = build_mock_tomtom_payload(args.point, reason=str(exc))
    now = datetime.datetime.utcnow()
    date = now.strftime("%Y-%m-%d")
    time = now.strftime("%H-%M-%S")
    object_name = f"raw/tomtom/{date}/{time}.json"

    raw_bytes = json.dumps(obj, ensure_ascii=False, indent=2).encode("utf-8")

    upload_to_minio(raw_bytes, object_name)
    print(f"Uploaded to s3://{get_settings().raw_bucket}/{object_name}")


if __name__ == "__main__":
    main()
