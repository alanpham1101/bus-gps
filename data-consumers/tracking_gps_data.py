from __future__ import annotations

import csv
import json
import logging
import os
import signal
from datetime import datetime, timezone
from typing import Any, Optional

import redis
from kafka import KafkaConsumer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("TrackingGPSData")

KAFKA_BOOTSTRAP_SERVERS: str = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
KAFKA_TOPIC: str             = os.environ.get("KAFKA_TOPIC", "hcmc_bus_gps")
KAFKA_GROUP_ID: str          = os.environ.get("KAFKA_GROUP_ID", "gps-data-tracking")
KAFKA_AUTO_OFFSET_RESET: str = os.environ.get("KAFKA_AUTO_OFFSET_RESET", "latest")

REDIS_HOST: str  = os.environ.get("REDIS_HOST", "localhost")
REDIS_PORT: int  = int(os.environ.get("REDIS_PORT", "6379"))

BUS_LATEST_KEY: str       = os.environ.get("BUS_LATEST_KEY", "bus_latest")
BUS_UPDATES_CHANNEL: str  = os.environ.get("BUS_UPDATES_CHANNEL", "bus_updates")

POLL_TIMEOUT_MS: int  = int(os.environ.get("POLL_TIMEOUT_MS", "500"))
POLL_MAX_RECORDS: int = int(os.environ.get("POLL_MAX_RECORDS", "500"))

VEHICLE_ROUTE_MAPPING_PATH: str = os.environ.get(
    "VEHICLE_ROUTE_MAPPING_PATH",
    "data-publisher/input_data/vehicle_route_mapping.csv",
)

_VEHICLE_ROUTES: dict[str, dict[str, Optional[str]]] = {}


def _load_vehicle_routes(path: str) -> dict[str, dict[str, Optional[str]]]:
    """Load the static vehicle → route mapping (vehicle, route_id, route_no) into memory."""
    routes: dict[str, dict[str, Optional[str]]] = {}
    if not os.path.exists(path):
        logger.warning(
            "Vehicle route mapping not found at %s — buses will have route_no=None",
            path,
        )
        return routes

    with open(path, newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            vid = (row.get("vehicle") or "").strip()
            if not vid:
                continue
            routes[vid] = {
                "route_id": (row.get("route_id") or "").strip() or None,
                "route_no": (row.get("route_no") or "").strip() or None,
            }
    logger.info("Loaded %d vehicle→route mappings from %s", len(routes), path)
    return routes


def _to_bus_payload(wp: dict[str, Any]) -> Optional[dict[str, Any]]:
    """Translate a Kafka `msgBusWayPoint` record into the GPSData shape.

    Enriches with route_id/route_no from the static mapping when available.
    Returns None when required GPS fields are missing, so the caller can skip it.
    """
    vehicle = wp.get("vehicle")
    lat = wp.get("y")
    lng = wp.get("x")
    if vehicle is None or lat is None or lng is None:
        return None

    ts_epoch = wp.get("datetime")
    if isinstance(ts_epoch, (int, float)) and ts_epoch > 0:
        ts_iso = datetime.fromtimestamp(ts_epoch, tz=timezone.utc).isoformat()
    else:
        ts_iso = datetime.now(tz=timezone.utc).isoformat()

    vehicle_id = str(vehicle)
    route_info = _VEHICLE_ROUTES.get(vehicle_id, {})

    return {
        "bus_id": vehicle_id,
        "lat": float(lat),
        "lng": float(lng),
        "speed": float(wp.get("speed") or 0.0),
        "timestamp": ts_iso,
        "route_id": route_info.get("route_id"),
        "route_no": route_info.get("route_no"),
    }


def _build_consumer() -> KafkaConsumer:
    return KafkaConsumer(
        KAFKA_TOPIC,
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        group_id=KAFKA_GROUP_ID,
        auto_offset_reset=KAFKA_AUTO_OFFSET_RESET,
        enable_auto_commit=True,
        auto_commit_interval_ms=5000,
        value_deserializer=lambda v: json.loads(v.decode("utf-8")),
        key_deserializer=lambda k: k.decode("utf-8") if k is not None else None,
        client_id="tracking-gps-data",
    )


def _build_redis() -> redis.Redis:
    return redis.Redis(host=REDIS_HOST, port=REDIS_PORT, decode_responses=True)


def main() -> None:
    global _VEHICLE_ROUTES
    _VEHICLE_ROUTES = _load_vehicle_routes(VEHICLE_ROUTE_MAPPING_PATH)

    consumer = _build_consumer()
    r = _build_redis()

    stop = False

    def _handle_signal(signum, _frame):
        nonlocal stop
        logger.info("Signal %s received — shutting down gracefully.", signum)
        stop = True

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    logger.info(
        "Tracking GPS data running | topic=%s group=%s bootstrap=%s redis=%s:%d",
        KAFKA_TOPIC,
        KAFKA_GROUP_ID,
        KAFKA_BOOTSTRAP_SERVERS,
        REDIS_HOST,
        REDIS_PORT,
    )

    total_tracked = 0
    total_skipped = 0

    try:
        while not stop:
            records = consumer.poll(
                timeout_ms=POLL_TIMEOUT_MS, max_records=POLL_MAX_RECORDS
            )
            if not records:
                continue

            pipe = r.pipeline(transaction=False)
            in_batch = 0

            for _tp, batch in records.items():
                for msg in batch:
                    try:
                        payload = _to_bus_payload(msg.value)
                    except Exception as exc:
                        logger.warning("Bad record at offset=%d: %s", msg.offset, exc)
                        total_skipped += 1
                        continue

                    if payload is None:
                        total_skipped += 1
                        continue

                    payload_json = json.dumps(payload, separators=(",", ":"))
                    pipe.hset(BUS_LATEST_KEY, payload["bus_id"], payload_json)
                    pipe.publish(BUS_UPDATES_CHANNEL, payload_json)
                    in_batch += 1

            if in_batch > 0:
                try:
                    pipe.execute()
                    total_tracked += in_batch
                    logger.info(
                        "Tracked %d msgs → Redis | total_tracked=%d total_skipped=%d",
                        in_batch,
                        total_tracked,
                        total_skipped,
                    )
                except Exception as exc:
                    logger.error("Redis pipeline execute failed: %s", exc)
    finally:
        logger.info(
            "Closing — total_tracked=%d total_skipped=%d", total_tracked, total_skipped
        )
        try:
            consumer.close()
        except Exception:
            pass
        try:
            r.close()
        except Exception:
            pass


if __name__ == "__main__":
    main()
