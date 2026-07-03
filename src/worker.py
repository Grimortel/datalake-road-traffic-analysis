from __future__ import annotations

from prefect import flow, task

from src.pipeline.service import ingest_all


@task(name="run-datalake-ingestion", retries=3, retry_delay_seconds=10)
def run_datalake_ingestion(parallel: bool = False) -> dict:
    return ingest_all(parallel=parallel)


@flow(name="weather-datalake-flow", log_prints=True)
def weather_datalake_flow(parallel: bool = False) -> dict:
    return run_datalake_ingestion(parallel=parallel)


def main() -> None:
    weather_datalake_flow.serve(name="weather-datalake", interval=360, pause_on_shutdown=True)


if __name__ == "__main__":
    main()
