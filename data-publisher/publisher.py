from __future__ import annotations

import json
import logging
import os
import time
from typing import Iterator

import ijson
from kafka import KafkaProducer

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s — %(message)s",
)
logger = logging.getLogger("BusGPSPublisher")

KAFKA_BOOTSTRAP_SERVERS: str = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
KAFKA_TOPIC: str             = os.environ.get("KAFKA_TOPIC", "hcmc_bus_gps")
INPUT_FILE: str              = os.environ.get(
    "INPUT_FILE", "data-publisher/input_data/sample.json"
)
BATCH_SIZE: int              = int(os.environ.get("BATCH_SIZE", "500"))
BATCH_INTERVAL_SECONDS: float = float(os.environ.get("BATCH_INTERVAL_SECONDS", "30"))

def _iter_waypoints(path: str) -> Iterator[dict]:
    """Stream `msgBusWayPoint` objects one at a time from a JSON-array file.
    """
    with open(path, "rb") as f:
        for record in ijson.items(f, "item", use_float=True):
            wp = record.get("msgBusWayPoint")
            if wp is None or not wp.get("vehicle"):
                continue
            yield wp


def _build_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS.split(","),
        key_serializer=lambda k: k.encode("utf-8"),
        value_serializer=lambda v: json.dumps(
            v, separators=(",", ":"), default=float
        ).encode("utf-8"),
        acks=1,
        linger_ms=10,
        compression_type="lz4",
        retries=5,
    )


def _sleep_remaining(batch_started_at: float) -> None:
    """Sleep so the wall-clock spacing between batches is exactly BATCH_INTERVAL_SECONDS."""
    elapsed = time.monotonic() - batch_started_at
    remaining = BATCH_INTERVAL_SECONDS - elapsed
    if remaining > 0:
        logger.info("Batch done in %.2fs — sleeping %.2fs.", elapsed, remaining)
        time.sleep(remaining)
    else:
        logger.warning(
            "Batch took %.2fs, longer than interval %.2fs — skipping sleep.",
            elapsed,
            BATCH_INTERVAL_SECONDS,
        )


def main() -> None:
    if not os.path.exists(INPUT_FILE):
        raise FileNotFoundError(f"INPUT_FILE not found: {INPUT_FILE}")

    logger.info(
        "Publishing %s → topic=%s servers=%s | rate=%d msg / %.0fs%s",
        INPUT_FILE,
        KAFKA_TOPIC,
        KAFKA_BOOTSTRAP_SERVERS,
        BATCH_SIZE,
        BATCH_INTERVAL_SECONDS,
    )

    producer = _build_producer()
    total_sent = 0
    batch_count = 0

    while True:
        in_batch = 0
        batch_started_at = time.monotonic()

        for wp in _iter_waypoints(INPUT_FILE):
            try:
                producer.send(KAFKA_TOPIC, key=str(wp["vehicle"]), value=wp)
            except Exception as exc:
                logger.error("Failed to enqueue record (vehicle=%s): %s", wp.get("vehicle"), exc)
                continue

            in_batch += 1
            total_sent += 1

            if in_batch >= BATCH_SIZE:
                producer.flush()
                batch_count += 1
                logger.info(
                    "Batch %d sent (%d msgs) | total_sent=%d", batch_count, in_batch, total_sent
                )
                _sleep_remaining(batch_started_at)
                in_batch = 0
                batch_started_at = time.monotonic()

        if in_batch > 0:
            producer.flush()
            batch_count += 1
            logger.info(
                "Final partial batch %d sent (%d msgs) | total_sent=%d",
                batch_count,
                in_batch,
                total_sent,
            )

    producer.close()


if __name__ == "__main__":
    main()
