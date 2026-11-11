from fastapi import FastAPI, HTTPException, Response
from pydantic import BaseModel, Field
from .detector import detect

app = FastAPI(title="Realtime Anomaly Ops", version="1.0.0")
class Event(BaseModel):
    metric: str = Field(pattern=r"^[a-zA-Z0-9_]+$")
    value: float
    baseline: list[float] = Field(min_length=5)

@app.get("/health")
def health(): return {"status": "ok", "mode": "synchronous-demo"}

@app.get("/metrics", include_in_schema=False)
def metrics(): return Response("anomaly_service_up 1\n", media_type="text/plain")

@app.post("/v1/events")
def score(event: Event):
    try: decision = detect(event.value, event.baseline)
    except ValueError as exc: raise HTTPException(422, str(exc))
    return {"metric": event.metric, "value": event.value, **decision.__dict__}
