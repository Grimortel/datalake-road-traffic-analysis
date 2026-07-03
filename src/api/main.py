from __future__ import annotations

import json

from fastapi import FastAPI, HTTPException, Query, Response
from fastapi.responses import JSONResponse

from src.pipeline.service import (
    ensure_schema,
    get_curated_rows,
    get_raw_object_bytes,
    get_raw_objects,
    get_staging_rows,
    ingest_all,
)

app = FastAPI(title="Datalake Weather API", version="1.0.0")


@app.on_event("startup")
def startup() -> None:
    ensure_schema()


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}


@app.get("/raw/{zone}")
def get_raw_zone(zone: str) -> dict:
    try:
        objects = get_raw_objects(zone)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"zone": zone, "objects": objects}


@app.get("/raw/{zone}/object")
def get_raw_object(
    zone: str,
    name: str = Query(..., description="Full object path in bucket, e.g. raw/forecast/2026-06-04/..json"),
) -> Response:
    if not name.startswith(f"raw/{zone}/"):
        raise HTTPException(status_code=400, detail="object path does not match zone")
    try:
        payload = get_raw_object_bytes(name)
    except Exception as exc:
        raise HTTPException(status_code=404, detail=str(exc)) from exc

    try:
        return JSONResponse(content=json.loads(payload))
    except Exception:
        return Response(content=payload, media_type="application/octet-stream")


@app.get("/staging/{table}")
def get_staging_table(
    table: str,
    date: str | None = Query(default=None),
    latitude: float | None = Query(default=None),
    longitude: float | None = Query(default=None),
) -> dict:
    try:
        rows = get_staging_rows(table, limit=100, date=date, latitude=latitude, longitude=longitude)
    except ValueError as exc:
        raise HTTPException(status_code=400, detail=str(exc)) from exc
    return {"table": table, "filters": {"date": date, "latitude": latitude, "longitude": longitude}, "rows": rows}


@app.get("/curated/anomalies")
def get_curated_anomalies(
    anomalies_only: bool = Query(default=False),
) -> dict:
    rows = get_curated_rows(limit=100, anomalies_only=anomalies_only)
    return {"filters": {"anomalies_only": anomalies_only}, "rows": rows}


@app.api_route("/ingest", methods=["GET", "POST"])
def ingest_sequential() -> dict:
    result = ingest_all(parallel=False)
    return result


@app.api_route("/ingest-fast", methods=["GET", "POST"])
def ingest_parallel() -> dict:
    result = ingest_all(parallel=True)
    return result
