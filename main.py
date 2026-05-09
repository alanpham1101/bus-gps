from fastapi import FastAPI, Request
from fastapi.staticfiles import StaticFiles
from fastapi.responses import FileResponse
from pydantic import BaseModel
from sse_starlette.sse import EventSourceResponse
from dotenv import load_dotenv
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
