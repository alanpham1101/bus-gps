set -e
cd "$(dirname "$0")"

echo "Submitting Spark Streaming job to spark-master:7077 ..."
MSYS_NO_PATHCONV=1 docker compose --profile submit run --rm \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  spark-submit \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  /app/data-publisher/spark_stream_job.py

echo "Done."
