set -e
cd "$(dirname "$0")"/..

echo "Submitting Bus Probe Clustering (DBSCAN) job to spark-master:7077 ..."
MSYS_NO_PATHCONV=1 docker compose --profile submit run --rm \
  -e KAFKA_BOOTSTRAP_SERVERS=kafka:9092 \
  -e REDIS_HOST=redis \
  -e REDIS_PORT=6379 \
  -e HDFS_OUTPUT_PATH=hdfs://namenode:9000/data/analytics/congestion_behavior \
  -e BUS_STOPS_PATH=/app/data/bus_stops.csv \
  -e HADOOP_USER_NAME=root \
  spark-submit \
  /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  --total-executor-cores 4 \
  --conf spark.jars.ivy=/tmp/.ivy2 \
  --conf spark.executorEnv.HADOOP_USER_NAME=root \
  --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1 \
  --conf spark.sql.execution.arrow.pyspark.enabled=true \
  /app/data-consumers/streaming_probe_clustering.py

echo "Done."
