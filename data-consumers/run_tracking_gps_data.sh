set -e
cd "$(dirname "$0")"/..

echo "Starting GPS live-position bridge (Kafka → Redis) ..."
MSYS_NO_PATHCONV=1 docker compose --profile tracking-gps run --rm \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  -e KAFKA_TOPIC=hcmc_bus_gps \
  -e KAFKA_GROUP_ID=gps-data-tracking \
  -e KAFKA_AUTO_OFFSET_RESET=latest \
  -e REDIS_HOST=redis \
  -e REDIS_PORT=6379 \
  tracking-gps-data

echo "Done."
