from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse
from dotenv import load_dotenv
from typing import Optional
import redis
import json
import asyncio
import os

load_dotenv()
app = FastAPI()

app.mount("/static", StaticFiles(directory="static"), name="static")

r = redis.Redis(
    host=os.getenv("REDIS_HOST", "localhost"),
    port=os.getenv("REDIS_PORT", 6379),
    decode_responses=True
)

# ---------------------------
# GPS Data Model
# ---------------------------
class GPSData(BaseModel):
    bus_id: str
    lat: float
    lng: float
    speed: float
    timestamp: str
    route_id: Optional[str] = None
    route_no: Optional[str] = None


# ---------------------------
# GPS Data Model
# ---------------------------
class CongestionDectionData(BaseModel):
    status: str
    avg_speed: float
    avg_tti: str
    severity: str
    point_count: str
    vehicles: str
    centroid_lat: float
    centroid_lon: float
    updated_at: str
    batch_id: str


# ---------------------------
# Map
# ---------------------------
@app.get("/")
async def root():
    return FileResponse("static/map.html")

# ---------------------------
# Bus snapshot
# ---------------------------
@app.get("/bus_current")
async def current_positions():
    buses = r.hgetall("bus_latest")
    return [json.loads(v) for v in buses.values()]


# ---------------------------
# SSE Bus Streaming Endpoint
# ---------------------------
@app.get("/bus_stream")
async def stream(request: Request):
    pubsub = r.pubsub()
    pubsub.subscribe("bus_updates")

    async def event_generator():
        while True:
            if await request.is_disconnected():
                break

            message = pubsub.get_message(ignore_subscribe_messages=True)

            if message:
                yield {
                    "event": "update",
                    "data": message["data"]
                }

            await asyncio.sleep(0.01)

    return EventSourceResponse(event_generator())


# ---------------------------
# Congestion snapshot
# ---------------------------
@app.get("/congestion_current")
async def current_congestion():
    """Return the latest congestion clusters, ordered by severity (highest first)."""
    pairs = r.zrevrange("congestion:severity", 0, -1, withscores=True)
    if not pairs:
        return []

    pipe = r.pipeline()
    for gid, _sev in pairs:
        pipe.hgetall(f"congestion:cluster:{gid}")
    rows = pipe.execute()

    result = []
    for (gid, _sev), row in zip(pairs, rows):
        if not row:
            continue
        result.append({
            "grid_id":      gid,
            "status":       row.get("status", "Congested"),
            "avg_speed":    float(row.get("avg_speed", 0) or 0),
            "avg_tti":      float(row.get("avg_tti", 0) or 0),
            "severity":     float(row.get("severity", 0) or 0),
            "point_count":  int(row.get("point_count", 0) or 0),
            "vehicles":     int(row.get("vehicles", 0) or 0),
            "centroid_lat": float(row.get("centroid_lat", 0) or 0),
            "centroid_lon": float(row.get("centroid_lon", 0) or 0),
            "updated_at":   row.get("updated_at", ""),
            "batch_id":     row.get("batch_id", ""),
        })
    return result


# ---------------------------
# SSE Congestion Dectection Streaming Endpoint
# ---------------------------
@app.get("/congestion_detection_stream")
async def stream(request: Request):
    pubsub = r.pubsub()
    pubsub.subscribe("congestion_detection_updates")

    async def event_generator():
        while True:
            if await request.is_disconnected():
                break

            message = pubsub.get_message(ignore_subscribe_messages=True)

            if message:
                yield {
                    "event": "update",
                    "data": message["data"]
                }

            await asyncio.sleep(0.01)

    return EventSourceResponse(event_generator())
