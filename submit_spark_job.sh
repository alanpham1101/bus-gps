set -e
cd "$(dirname "$0")"

echo "Submitting Spark job to spark-master:7077 ..."
docker compose --profile submit run --rm spark-submit

echo "Done."
