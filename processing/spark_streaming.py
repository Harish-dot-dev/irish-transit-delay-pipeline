import os
from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, when
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, TimestampType
)
import os
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = os.environ["PATH"] + ";C:\\hadoop\\bin"
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"  # Force Spark to use localhost
os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"  # Stop it using 'Achu'
load_dotenv()

# ---------- Why SparkSession? ----------
# SparkSession is the entry point to everything in PySpark.
# Think of it as the "engine start" for Spark.
# .config() lines add extra capabilities:
#   - delta extension lets Spark read/write Delta Lake format
#   - kafka jar lets Spark talk to Kafka as a data source
#   - delta.autoOptimize keeps Delta files tidy automatically

spark = (
    SparkSession.builder
    .appName("TransitBronzeLayer")
    .master("local[*]")   # local[*] = use all CPU cores on your machine
    .config("spark.driver.host", "localhost")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.jars.packages",
            "io.delta:delta-spark_2.12:3.0.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .config("spark.databricks.delta.properties.defaults.autoOptimize.optimizeWrite", "true")
    .getOrCreate()
)

# Reduce noisy logs — only show warnings and above
spark.sparkContext.setLogLevel("WARN")

# ---------- Schema ----------
# We tell Spark exactly what shape our JSON messages are.
# Without a schema Spark would read everything as a string.
# With a schema it enforces types — bad records get caught early.
schema = StructType([
    StructField("trip_id",       StringType(),  True),
    StructField("route_id",      StringType(),  True),
    StructField("stop_id",       StringType(),  True),
    StructField("stop_sequence", IntegerType(), True),
    StructField("delay_seconds", IntegerType(), True),
    StructField("ingested_at",   StringType(),  True),
])

# ---------- Read from Kafka ----------
# Spark treats Kafka as a streaming source — it continuously
# reads new messages as they arrive in the topic.
#
# startingOffsets: "earliest" means read ALL messages in the topic
# including ones published before this job started.
# Use "latest" in production to only read new messages.
#
# Kafka gives us each message as a row with columns:
#   key, value, topic, partition, offset, timestamp
# The actual JSON payload is in the 'value' column as bytes.

raw_stream = (
    spark.readStream
    .format("kafka")
    .option("kafka.bootstrap.servers", "localhost:9092")
    .option("subscribe", "transit-raw")
    .option("startingOffsets", "earliest")
    .load()
)

# ---------- Parse the JSON payload ----------
# Step 1: cast value from bytes → string
# Step 2: from_json() parses the string using our schema
#         This gives us a nested column called 'parsed'
# Step 3: select() flattens parsed.trip_id, parsed.route_id etc
#         into top-level columns
# Step 4: add a delay_category column — useful for dashboards later

parsed_stream = (
    raw_stream
    .withColumn("value_str", col("value").cast("string"))
    .withColumn("parsed", from_json(col("value_str"), schema))
    .select(
        col("parsed.trip_id"),
        col("parsed.route_id"),
        col("parsed.stop_id"),
        col("parsed.stop_sequence"),
        col("parsed.delay_seconds"),
        col("parsed.ingested_at"),
        col("timestamp").alias("kafka_timestamp")  # when Kafka received the message
    )
    .withColumn("delay_category",
        when(col("delay_seconds") <= 0,    "on_time")
        .when(col("delay_seconds") <= 300,  "minor")    # under 5 mins
        .when(col("delay_seconds") <= 900,  "moderate") # 5–15 mins
        .otherwise("severe")                            # over 15 mins
    )
)

# ---------- Write to Bronze Delta Lake ----------
# Delta Lake is like a database table stored as files on disk.
# It gives us:
#   - ACID transactions (no corrupt data if job crashes mid-write)
#   - Schema enforcement (rejects records that don't match)
#   - Time travel (query data as it was at any point in the past)
#
# outputMode "append" — only write new records, never overwrite
# format "delta"      — write as Delta Lake format (not plain parquet)
# checkpointLocation  — Spark saves its progress here so if the job
#                       crashes it resumes from where it left off,
#                       not from the beginning

bronze_path     = "./delta_lake/bronze/transit_raw"
checkpoint_path = "./delta_lake/bronze/checkpoints/transit_raw"

query = (
    parsed_stream.writeStream
    .outputMode("append")
    .format("delta")
    .option("checkpointLocation", checkpoint_path)
    .option("path", bronze_path)
    .trigger(processingTime="30 seconds")  # process a batch every 30 seconds
    .start()
)

print("Bronze streaming job started — writing to Delta Lake...")
print(f"Output path: {bronze_path}")

# awaitTermination() keeps the job running until you stop it with Ctrl+C
query.awaitTermination()