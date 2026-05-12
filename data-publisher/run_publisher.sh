set -e
cd "$(dirname "$0")"/..

echo "Starting bus GPS publisher (1000 msg / 15s) to Kafka topic hcmc_bus_gps"
MSYS_NO_PATHCONV=1 docker compose --profile publish run --rm \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  -e KAFKA_TOPIC=hcmc_bus_gps \
  -e INPUT_FILE=/app/data-publisher/input_data/sample.json \
  -e BATCH_SIZE=1000 \
  -e BATCH_INTERVAL_SECONDS=15 \
  publisher

echo "Done."
