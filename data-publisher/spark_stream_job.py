import os
from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, LongType, BooleanType
from pyspark.sql.functions import col, to_json, struct

# CONFIGURATION CONSTANTS
KAFKA_BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_TOPIC_NAME = "hcmc_bus_gps"
CHECKPOINT_LOCATION = "/tmp/checkpoints/hcmc_bus_gps"
INPUT_PATH = "/app/data-publisher/input_data/"

def main():
    # Initialize SparkSession
    spark = SparkSession.builder \
        .appName("BusGPSDataPublisher") \
        .getOrCreate()
    #Define Strict Schema
    #Root key is msgBusWayPoint which contains the data
    msg_bus_way_point_schema = StructType([
        StructField("vehicle", StringType(), True),
        StructField("speed", DoubleType(), True),
        StructField("datetime", LongType(), True),
        StructField("x", DoubleType(), True),
        StructField("y", DoubleType(), True),
        StructField("ignition", BooleanType(), True),
        StructField("working", BooleanType(), True)
    ])
    
    root_schema = StructType([
        StructField("msgBusWayPoint", msg_bus_way_point_schema, True)
    ])
    
    #Read Stream from JSON directory
    df_stream = spark.readStream \
        .option("multiLine", "true") \
        .schema(root_schema) \
        .json(INPUT_PATH)
    
    # Transform Data
    # Set the Kafka 'key' to vehicle (for Strict Ordering & Consistent Partitioning)
    # Set the Kafka 'value' to a JSON string of the flattened vehicle data
    kafka_df = df_stream.select(
        col("msgBusWayPoint.vehicle").cast("string").alias("key"),
        to_json(col("msgBusWayPoint")).alias("value")
    )
    
    # Write to Kafka Sink
    query = kafka_df.writeStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS) \
        .option("topic", KAFKA_TOPIC_NAME) \
        .option("checkpointLocation", CHECKPOINT_LOCATION) \
        .outputMode("append") \
        .start()
        
    query.awaitTermination()

if __name__ == "__main__":
    main()
