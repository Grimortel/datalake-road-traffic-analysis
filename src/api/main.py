from fastapi import FastAPI

app = FastAPI(title="Datalake Traffic API")


@app.get("/health")
def health() -> dict:
    return {"status": "ok"}
