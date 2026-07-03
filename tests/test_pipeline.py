from __future__ import annotations

import math
from datetime import datetime, timezone
from unittest import TestCase
from unittest.mock import patch

from fastapi.testclient import TestClient

from src.api.main import app
from src.pipeline.service import normalize_forecast_payload, normalize_historical_csv, normalize_historical_payload, parse_timestamp


class PipelineTransformTests(TestCase):
    def test_normalize_forecast_payload(self) -> None:
        payload = {
            "latitude": 48.8566,
            "longitude": 2.3522,
            "hourly": {
                "time": ["2026-06-04T10:00", "2026-06-04T11:00"],
                "temperature_2m": [18.5, 19.2],
                "relative_humidity_2m": [65.0, 63.0],
                "wind_speed_10m": [12.0, 14.0],
                "precipitation": [0.0, 0.5],
            },
        }

        rows = normalize_forecast_payload(payload)

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["latitude"], 48.8566)
        self.assertEqual(rows[0]["longitude"], 2.3522)
        self.assertEqual(rows[0]["temperature"], 18.5)
        self.assertEqual(rows[0]["humidity"], 65.0)
        self.assertEqual(rows[0]["wind_speed"], 12.0)
        self.assertEqual(rows[0]["precipitation"], 0.0)
        self.assertEqual(rows[0]["timestamp"].tzinfo, timezone.utc)

    def test_normalize_historical_payload(self) -> None:
        payload = {
            "latitude": 48.8566,
            "longitude": 2.3522,
            "hourly": {
                "time": ["2024-01-01T00:00"],
                "temperature_2m": [3.5],
                "relative_humidity_2m": [80.0],
                "wind_speed_10m": [8.0],
                "precipitation": [1.2],
            },
        }

        rows = normalize_historical_payload(payload)

        self.assertEqual(len(rows), 1)
        self.assertEqual(rows[0]["temperature"], 3.5)
        self.assertEqual(rows[0]["humidity"], 80.0)

    def test_parse_timestamp_iso(self) -> None:
        ts = parse_timestamp("2026-06-04T10:00")
        self.assertEqual(ts.hour, 10)
        self.assertEqual(ts.tzinfo, timezone.utc)

    def test_parse_timestamp_z_suffix(self) -> None:
        ts = parse_timestamp("2026-06-04T10:00:00Z")
        self.assertEqual(ts.tzinfo, timezone.utc)

    def test_normalize_historical_csv(self) -> None:
        csv_content = (
            "timestamp,latitude,longitude,temperature_2m,relative_humidity_2m,wind_speed_10m,precipitation\n"
            "2024-01-01T00:00:00Z,48.8566,2.3522,3.5,80.0,8.0,1.2\n"
            "2024-01-01T01:00:00Z,48.8566,2.3522,3.2,81.0,7.5,0.0\n"
        )
        rows = normalize_historical_csv(csv_content.encode("utf-8"))

        self.assertEqual(len(rows), 2)
        self.assertEqual(rows[0]["temperature"], 3.5)
        self.assertEqual(rows[0]["latitude"], 48.8566)
        self.assertEqual(rows[1]["precipitation"], 0.0)

    def test_normalize_empty_hourly(self) -> None:
        payload = {"latitude": 48.8566, "longitude": 2.3522, "hourly": {"time": []}}
        rows = normalize_forecast_payload(payload)
        self.assertEqual(rows, [])


class ApiSmokeTests(TestCase):
    def setUp(self) -> None:
        self.client = TestClient(app)

    def test_health(self) -> None:
        response = self.client.get("/health")
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json(), {"status": "ok"})

    @patch("src.api.main.get_raw_objects")
    def test_raw_route(self, mock_get_raw_objects) -> None:
        mock_get_raw_objects.return_value = [{"name": "raw/forecast/2026-06-04/mock.json", "size": 12, "last_modified": None}]

        response = self.client.get("/raw/forecast")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["objects"][0]["name"], "raw/forecast/2026-06-04/mock.json")

    @patch("src.api.main.get_staging_rows")
    def test_staging_route(self, mock_get_staging_rows) -> None:
        mock_get_staging_rows.return_value = [{"timestamp": datetime.now(timezone.utc).isoformat(), "temperature": 18.5}]

        response = self.client.get("/staging/weather_realtime")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(len(response.json()["rows"]), 1)

    @patch("src.api.main.get_curated_rows")
    def test_curated_route(self, mock_get_curated_rows) -> None:
        mock_get_curated_rows.return_value = [{"variable": "temperature", "zscore": 2.5, "is_anomaly": True}]

        response = self.client.get("/curated/anomalies")

        self.assertEqual(response.status_code, 200)
        self.assertTrue(response.json()["rows"][0]["is_anomaly"])

    @patch("src.api.main.ingest_all")
    def test_ingest_route(self, mock_ingest_all) -> None:
        mock_ingest_all.return_value = {"mode": "sequential", "total_ms": 1, "zones": {}, "curated_rows": 0}

        response = self.client.post("/ingest")

        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.json()["mode"], "sequential")
