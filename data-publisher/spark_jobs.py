import os
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Input path: env SPARK_INPUT_PATH, or first CLI arg, or default (works when run from project root or Docker mount at /app)
DEFAULT_INPUT = "data-publisher/data-extracted/part1/part1/sub_raw_300.json"
INPUT_PATH = os.environ.get("SPARK_INPUT_PATH") or (sys.argv[1] if len(sys.argv) > 1 else DEFAULT_INPUT)

# When running in Docker with project mounted at /app, use absolute path so driver can read the file
if os.path.exists("/app") and not os.path.isabs(INPUT_PATH):
    app_input = os.path.join("/app", INPUT_PATH)
    if os.path.exists(app_input):
        INPUT_PATH = app_input

spark = SparkSession.builder.appName("GPSProcessing").getOrCreate()

df = spark.read.option("multiLine", "true").json(INPUT_PATH)

processed_df = df.select(
    col("msgType"),
    col("msgBusWayPoint.x").cast("double").alias("latitude"),
    col("msgBusWayPoint.y").cast("double").alias("longitude")
)

processed_df.show()
spark.stop()
