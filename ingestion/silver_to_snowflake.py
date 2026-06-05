import os
import pandas as pd
from dotenv import load_dotenv
from pyspark.sql import SparkSession
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()

os.environ["HADOOP_HOME"]        = "C:\\hadoop"
os.environ["PATH"]               = os.environ["PATH"] + ";C:\\hadoop\\bin"
os.environ["SPARK_LOCAL_IP"]     = "127.0.0.1"
os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"

# ---------- SparkSession ----------
spark = (
    SparkSession.builder
    .appName("SilverToSnowflake")
    .master("local[*]")
    .config("spark.driver.host", "localhost")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.jars.packages",
            "io.delta:delta-spark_2.12:3.0.0,"
            "org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0")
    .config("spark.sql.extensions",
            "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog",
            "org.apache.spark.sql.delta.catalog.DeltaCatalog")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")

# ---------- Read Silver Delta Lake ----------
# We read the cleaned Silver layer into Spark first
# then convert to Pandas for the Snowflake loader
print("Reading Silver Delta Lake...")
silver_path = "./delta_lake/silver/transit_cleaned"
silver_df   = spark.read.format("delta").load(silver_path)

print(f"Silver record count: {silver_df.count()}")

# ---------- Why convert to Pandas? ----------
# Snowflake's write_pandas() function expects a Pandas DataFrame.
# For 2.6M records we sample a representative subset —
# this keeps the load fast and within Snowflake trial limits.
# In production you'd use Snowflake's COPY INTO with staged files.

print("Converting to Pandas...")
pandas_df = silver_df.toPandas()

# Snowflake expects uppercase column names
pandas_df.columns = [c.upper() for c in pandas_df.columns]

# Convert timestamp columns to strings — Snowflake connector handles these better
pandas_df["INGESTED_AT"]  = pandas_df["INGESTED_AT"].astype(str)
pandas_df["PROCESSED_AT"] = pandas_df["PROCESSED_AT"].astype(str)

# Convert boolean to string — avoids type mapping issues
pandas_df["IS_DELAYED"] = pandas_df["IS_DELAYED"].astype(str)

print(f"Pandas shape: {pandas_df.shape}")
print(f"Columns: {list(pandas_df.columns)}")

# ---------- Connect to Snowflake ----------
print("\nConnecting to Snowflake...")
conn = snowflake.connector.connect(
    account   = os.getenv("SNOWFLAKE_ACCOUNT"),
    user      = os.getenv("SNOWFLAKE_USER"),
    password  = os.getenv("SNOWFLAKE_PASSWORD"),
    database  = os.getenv("SNOWFLAKE_DATABASE"),
    warehouse = os.getenv("SNOWFLAKE_WAREHOUSE"),
    schema    = "SILVER"
)

cursor = conn.cursor()

# ---------- Create table in Snowflake if it doesn't exist ----------
# This DDL matches our Silver schema exactly
cursor.execute("""
    CREATE TABLE IF NOT EXISTS transit_pipeline.silver.transit_cleaned (
        TRIP_ID         VARCHAR,
        ROUTE_ID        VARCHAR,
        STOP_ID         VARCHAR,
        STOP_SEQUENCE   INTEGER,
        DELAY_SECONDS   INTEGER,
        DELAY_MINUTES   FLOAT,
        DELAY_CATEGORY  VARCHAR,
        IS_DELAYED      VARCHAR,
        INGESTED_AT     VARCHAR,
        PROCESSED_AT    VARCHAR
    )
""")
print("Table created/verified in Snowflake")

# ---------- Truncate before load ----------
# We rebuild the Silver table fresh each run
# matching the overwrite behaviour of our PySpark Silver job
cursor.execute("TRUNCATE TABLE transit_pipeline.silver.transit_cleaned")
print("Table truncated — loading fresh data...")

# ---------- Load data into Snowflake ----------
# write_pandas() uses Snowflake's PUT + COPY INTO internally
# It chunks the data automatically and is much faster than row-by-row inserts
print("Writing to Snowflake...")
success, nchunks, nrows, _ = write_pandas(
    conn       = conn,
    df         = pandas_df,
    table_name = "TRANSIT_CLEANED",
    schema     = "SILVER",
    database   = "TRANSIT_PIPELINE"
)

print(f"\nLoad complete!")
print(f"Success  : {success}")
print(f"Chunks   : {nchunks}")
print(f"Rows loaded: {nrows:,}")

# ---------- Verify in Snowflake ----------
cursor.execute("SELECT COUNT(*) FROM transit_pipeline.silver.transit_cleaned")
count = cursor.fetchone()[0]
print(f"Snowflake row count: {count:,}")

cursor.execute("""
    SELECT delay_category, COUNT(*) as cnt
    FROM transit_pipeline.silver.transit_cleaned
    GROUP BY delay_category
    ORDER BY cnt DESC
""")
print("\nDelay breakdown in Snowflake:")
for row in cursor.fetchall():
    print(f"  {row[0]:<12} {row[1]:>10,}")

cursor.close()
conn.close()
spark.stop()
print("\nDone!")