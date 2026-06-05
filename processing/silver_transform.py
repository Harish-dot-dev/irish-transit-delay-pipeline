import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, when, round, upper, trim, regexp_replace,
    to_timestamp, current_timestamp, lit
)

# Windows fixes
os.environ["HADOOP_HOME"] = "C:\\hadoop"
os.environ["PATH"] = os.environ["PATH"] + ";C:\\hadoop\\bin"
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
os.environ["SPARK_LOCAL_HOSTNAME"] = "localhost"

# ---------- SparkSession ----------
spark = (
    SparkSession.builder
    .appName("TransitSilverLayer")
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

# ---------- Paths ----------
bronze_path = "./delta_lake/bronze/transit_raw"
silver_path = "./delta_lake/silver/transit_cleaned"

# ---------- Read Bronze ----------
# This is a batch read — not streaming.
# We read everything currently in Bronze, transform it, write to Silver.
# In production Airflow would trigger this job on a schedule.
print("Reading from Bronze layer...")
bronze_df = spark.read.format("delta").load(bronze_path)

print(f"Bronze record count: {bronze_df.count()}")
bronze_df.printSchema()

# ---------- Understand the data first ----------
# Before transforming, always look at what you have.
# This is good data engineering practice.
print("\nSample Bronze records:")
bronze_df.show(5, truncate=False)

print("\nDelay category distribution:")
bronze_df.groupBy("delay_category").count().orderBy("count", ascending=False).show()

# ---------- Silver Transformations ----------
# Each transformation is explained below.
# The goal: clean, standardised, analysis-ready data.

silver_df = (
    bronze_df

    # 1. DROP NULLS on critical columns
    # Records with no trip_id or stop_id are useless for analysis
    .dropna(subset=["trip_id", "stop_id", "route_id"])

    # 2. CLEAN route_id
    # Raw data has route_ids like "2 64 d a" with spaces and random chars
    # We strip whitespace and uppercase everything for consistency
    .withColumn("route_id",
        upper(trim(regexp_replace(col("route_id"), "\\s+", " ")))
    )

    # 3. CLEAN stop_id — trim any whitespace
    .withColumn("stop_id", trim(col("stop_id")))

    # 4. CONVERT delay to minutes — easier to read than seconds
    # round() to 2 decimal places
    .withColumn("delay_minutes",
        round(col("delay_seconds") / 60, 2)
    )

    # 5. ADD is_delayed flag — boolean, useful for aggregations
    # True if delay is more than 1 minute
    .withColumn("is_delayed",
        when(col("delay_seconds") > 60, True).otherwise(False)
    )

    # 6. FILTER out impossible values
    # Delays over 2 hours (7200s) or under -1 hour are likely bad data
    .filter(
        (col("delay_seconds") >= -3600) &
        (col("delay_seconds") <= 7200)
    )

    # 7. PARSE ingested_at string → proper timestamp
    .withColumn("ingested_at",
        to_timestamp(col("ingested_at"))
    )

    # 8. ADD processed_at — when Silver transformation ran
    .withColumn("processed_at", current_timestamp())

    # 9. SELECT only the columns we want in Silver
    # Drop kafka_timestamp — internal plumbing, not needed downstream
    .select(
        "trip_id",
        "route_id",
        "stop_id",
        "stop_sequence",
        "delay_seconds",
        "delay_minutes",
        "delay_category",
        "is_delayed",
        "ingested_at",
        "processed_at"
    )
)

# ---------- How many records passed the filters? ----------
silver_count = silver_df.count()
print(f"\nSilver record count after cleaning: {silver_count}")

print("\nSample Silver records:")
silver_df.show(5, truncate=False)

print("\nDelay distribution in minutes:")
silver_df.select("delay_minutes").summary("min", "25%", "50%", "75%", "max").show()

print("\nIs_delayed breakdown:")
silver_df.groupBy("is_delayed").count().show()

# ---------- Write to Silver Delta Lake ----------
# mode("overwrite") replaces Silver completely each run
# This is fine for batch — we always rebuild Silver from Bronze
# partitionBy("delay_category") splits files by category —
# makes downstream queries faster when filtering by category
print(f"\nWriting to Silver layer: {silver_path}")

(
    silver_df.write
    .format("delta")
    .mode("overwrite")
    .partitionBy("delay_category")
    .save(silver_path)
)

print("Silver layer written successfully!")
print(f"Partitioned by delay_category: on_time / minor / moderate / severe")

# ---------- Verify ----------
verify_df = spark.read.format("delta").load(silver_path)
print(f"\nVerification — Silver record count: {verify_df.count()}")
verify_df.groupBy("delay_category").count().orderBy("count", ascending=False).show()

spark.stop()