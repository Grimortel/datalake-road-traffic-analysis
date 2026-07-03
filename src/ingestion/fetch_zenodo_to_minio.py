#!/usr/bin/env python3
"""Upload the Zenodo hourly traffic dataset to MinIO raw storage.

Usage:
  python src/ingestion/fetch_zenodo_to_minio.py --path /path/to/traffic_hourly.csv
  python src/ingestion/fetch_zenodo_to_minio.py --url https://.../traffic_hourly.csv
  python src/ingestion/fetch_zenodo_to_minio.py --mock

If no source is provided, a small mock CSV is generated so the project stays runnable.
"""

from __future__ import annotations

import argparse
import io
import sys
from pathlib import Path

import requests
from dotenv import load_dotenv

if __package__ in {None, ""}:
    sys.path.append(str(Path(__file__).resolve().parents[2]))

from src.pipeline.service import build_mock_zenodo_dataframe, ensure_bucket, get_minio_client, get_settings

load_dotenv()


def download_csv(url: str) -> bytes:
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    return response.content


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--path", help="Local CSV file path")
    parser.add_argument("--url", help="Remote CSV URL")
    parser.add_argument("--mock", action="store_true", help="Generate and upload mock data")
    args = parser.parse_args()

    settings = get_settings()
    client = get_minio_client()
    ensure_bucket(client, settings.raw_bucket)

    if args.mock:
        frame = build_mock_zenodo_dataframe()
        raw_bytes = frame.to_csv(index=False).encode("utf-8")
    elif args.path:
        with open(args.path, "rb") as handle:
            raw_bytes = handle.read()
    elif args.url:
        raw_bytes = download_csv(args.url)
    else:
        frame = build_mock_zenodo_dataframe()
        raw_bytes = frame.to_csv(index=False).encode("utf-8")

    object_name = "raw/zenodo/traffic_hourly.csv"
    client.put_object(settings.raw_bucket, object_name, io.BytesIO(raw_bytes), len(raw_bytes))
    print(f"Uploaded s3://{settings.raw_bucket}/{object_name}")


if __name__ == "__main__":
    main()
