# ==============================================================================
# Project Argus: Call Detail Records (CDR) Spark Declarative Pipeline (SDP)
# Medallion Architecture: Bronze -> Silver
# ==============================================================================

import json
import urllib.request
from pyspark import pipelines as dp
from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col, expr, trim, to_timestamp
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Initialize Spark session for declarative reference
spark = SparkSession.builder.getOrCreate()

# Define schemas for declarative streams
cdr_spark_schema = StructType([
    StructField("RECORD_ID", StringType(), True),
    StructField("TIMESTAMP", StringType(), True),
    StructField("CALLING_NUM", StringType(), True),
    StructField("CALLED_NUM", StringType(), True),
    StructField("CALL_TYPE", StringType(), True),
    StructField("DURATION_SEC", IntegerType(), True),
    StructField("CALLER_IMSI", StringType(), True),
    StructField("CALLER_IMEI", StringType(), True),
    StructField("CALLER_MNO", StringType(), True),
    StructField("BTS_ID", StringType(), True),
    StructField("CELL_ID", IntegerType(), True),
    StructField("LAC", IntegerType(), True),
    StructField("STATUS", StringType(), True),
    StructField("kafka_ingest_time", TimestampType(), True)
])

def fetch_schema(schema_registry_url, subject):
    """
    Fetches the latest Avro schema from Confluent Schema Registry.
    """
    try:
        url = f"{schema_registry_url}/subjects/{subject}/versions/latest"
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req) as response:
            res = json.loads(response.read().decode())
            return res['schema']
    except Exception as e:
        raise e

# ==============================================================================
# 1. BRONZE LAYER: Consuming raw Kafka topic as a streaming table
# ==============================================================================
@dp.table(name="cdr_bronze")
def cdr_bronze() -> DataFrame:
    kafka_bootstrap_servers = "kafka:29092"
    schema_registry_url = "http://schema-registry:8081"
    kafka_topic = "cdr-records"

    # Define streaming source
    df_kafka = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers)
        .option("subscribe", kafka_topic)
        .option("startingOffsets", "earliest")
        .load()
    )

    avro_schema_json = fetch_schema(schema_registry_url, f"{kafka_topic}-value")

    df_avro_extracted = df_kafka.select(
        expr("substring(value, 6, length(value) - 5)").alias("avro_payload"),
        col("timestamp").alias("kafka_ingest_time")
    )

    return df_avro_extracted.select(
        from_avro(col("avro_payload"), avro_schema_json).alias("data"),
        col("kafka_ingest_time")
    ).select("data.*", "kafka_ingest_time")

# ==============================================================================
# 2. SILVER LAYER: Data Normalization and Sanitization
# ==============================================================================
@dp.table(name="cdr_silver")
def cdr_silver() -> DataFrame:
    # Query Bronze table defined in the same pipeline
    df_bronze = spark.readStream.table("cdr_bronze")

    return df_bronze.filter(
        col("RECORD_ID").isNotNull() &
        col("CALLING_NUM").isNotNull() & (trim(col("CALLING_NUM")) != "") &
        col("CALLED_NUM").isNotNull() & (trim(col("CALLED_NUM")) != "") &
        col("CALLER_IMSI").isNotNull() & (trim(col("CALLER_IMSI")) != "")
    ).select(
        col("RECORD_ID").alias("record_id"),
        to_timestamp(col("TIMESTAMP")).alias("event_timestamp"),
        trim(col("CALLING_NUM")).alias("calling_num"),
        trim(col("CALLED_NUM")).alias("called_num"),
        col("CALL_TYPE").alias("call_type"),
        col("DURATION_SEC").cast("int").alias("duration_sec"),
        trim(col("CALLER_IMSI")).alias("caller_imsi"),
        trim(col("CALLER_IMEI")).alias("caller_imei"),
        col("CALLER_MNO").alias("caller_mno"),
        col("BTS_ID").alias("bts_id"),
        col("CELL_ID").cast("int").alias("cell_id"),
        col("LAC").cast("int").alias("lac"),
        col("STATUS").alias("status"),
        col("kafka_ingest_time")
    )
