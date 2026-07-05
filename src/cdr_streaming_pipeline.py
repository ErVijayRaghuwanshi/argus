#!/usr/bin/env python3
# ==============================================================================
# Project Argus: Call Detail Records (CDR) Streaming Pipeline
# Medallion Architecture: Bronze (Raw) -> Silver (Cleaned) -> Gold (Neo4j / Redis)
# ==============================================================================

import sys
import json
import time
import urllib.request
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, expr, trim, to_timestamp
from pyspark.sql.avro.functions import from_avro
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Define schemas for Parquet structured streaming
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

silver_spark_schema = StructType([
    StructField("record_id", StringType(), True),
    StructField("event_timestamp", TimestampType(), True),
    StructField("calling_num", StringType(), True),
    StructField("called_num", StringType(), True),
    StructField("call_type", StringType(), True),
    StructField("duration_sec", IntegerType(), True),
    StructField("caller_imsi", StringType(), True),
    StructField("caller_imei", StringType(), True),
    StructField("caller_mno", StringType(), True),
    StructField("bts_id", StringType(), True),
    StructField("cell_id", IntegerType(), True),
    StructField("lac", IntegerType(), True),
    StructField("status", StringType(), True),
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
            print(f"Successfully retrieved schema for subject '{subject}' (Version: {res['version']})")
            return res['schema']
    except Exception as e:
        print(f"Error fetching schema from registry ({url}): {e}")
        raise e

def process_partition_to_sinks(rows):
    """
    Writes a Spark partition of Silver CDR records to Redis and Neo4j.
    Runs in parallel on executors.
    """
    import redis
    from neo4j import GraphDatabase

    # 1. Establish Redis connection
    r_client = redis.Redis(host='redis', port=6379, db=0, decode_responses=True)
    redis_pipe = r_client.pipeline(transaction=False)
    redis_batch_size = 50
    redis_count = 0

    # 2. Establish Neo4j connection
    neo4j_uri = "bolt://neo4j:7687"
    neo4j_driver = GraphDatabase.driver(neo4j_uri, auth=("neo4j", "argus-local-dev-password"))

    # Track processed counts
    records_processed = 0

    with neo4j_driver.session() as session:
        for row in rows:
            record_id = row.record_id
            timestamp_str = row.event_timestamp.isoformat() if row.event_timestamp else None
            calling_num = row.calling_num
            called_num = row.called_num
            call_type = row.call_type
            duration_sec = row.duration_sec
            caller_imsi = row.caller_imsi
            caller_imei = row.caller_imei
            caller_mno = row.caller_mno
            bts_id = row.bts_id
            lac = row.lac
            status = row.status

            # --- SINK 1: Redis Cache Updates ---
            # Set bi-directional mapping for fast query routing
            redis_pipe.set(f"imsi:{caller_imsi}", calling_num)
            redis_pipe.set(f"msisdn:{calling_num}", caller_imsi)

            # Store / update subscriber latest session status
            state_key = f"subscriber:state:{calling_num}"
            redis_pipe.hset(state_key, mapping={
                "last_seen_timestamp": timestamp_str or "",
                "last_seen_bts_id": bts_id or "",
                "last_seen_imsi": caller_imsi or "",
                "last_seen_imei": caller_imei or "",
                "last_seen_mno": caller_mno or "",
                "status": status or ""
            })
            redis_count += 1

            if redis_count >= redis_batch_size:
                redis_pipe.execute()
                redis_count = 0

            # --- SINK 2: Neo4j Graph Database Updates ---
            # Merge entity nodes (Phone, IMSI, IMEI, CellTower) and build relationship links
            cypher = """
            MERGE (caller:Phone {msisdn: $calling_num})
            MERGE (called:Phone {msisdn: $called_num})
            MERGE (imsi:IMSI {imsi: $caller_imsi})
            MERGE (imei:IMEI {imei: $caller_imei})
            
            MERGE (caller)-[:HAS_IMSI]->(imsi)
            MERGE (caller)-[:HAS_IMEI]->(imei)
            """

            params = {
                "calling_num": calling_num,
                "called_num": called_num,
                "caller_imsi": caller_imsi,
                "caller_imei": caller_imei,
                "record_id": record_id,
                "timestamp": timestamp_str
            }

            if bts_id:
                cypher += """
                MERGE (tower:CellTower {tower_id: $bts_id})
                CREATE (caller)-[:CONNECTED_TO {timestamp: $timestamp, lac: $lac}]->(tower)
                """
                params["bts_id"] = bts_id
                params["lac"] = lac

            if call_type == "VOICE":
                cypher += """
                CREATE (caller)-[:CALLS {timestamp: $timestamp, duration_sec: $duration_sec, record_id: $record_id}]->(called)
                """
                params["duration_sec"] = duration_sec
            else:
                cypher += """
                CREATE (caller)-[:SENDS_SMS {timestamp: $timestamp, record_id: $record_id}]->(called)
                """

            session.run(cypher, params)
            records_processed += 1

        # Push any remaining redis cached commands
        if redis_count > 0:
            redis_pipe.execute()

    neo4j_driver.close()
    print(f"Executor Partition processed {records_processed} records.")

def write_to_redis_and_neo4j(batch_df, batch_id):
    """
    ForeachBatch writer executing on driver, triggering parallel partition processing.
    """
    print(f"Processing Batch ID: {batch_id} | Rows: {batch_df.count()}")
    batch_df.foreachPartition(process_partition_to_sinks)

def main():
    print("Initializing Project Argus CDR Streaming Pipeline...")

    # Initialize Spark Session configured for standalone cluster
    spark = SparkSession.builder \
        .appName("Argus-CDR-Streaming-Pipeline") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Kafka & Schema Registry Endpoints (Internal container network topology)
    kafka_bootstrap_servers = "kafka:29092"
    schema_registry_url = "http://schema-registry:8081"
    kafka_topic = "cdr-records"

    # shared filesystem paths (mounted under /tmp/src/ to sync across containers)
    bronze_path = "/tmp/src/parquet/cdr_bronze"
    silver_path = "/tmp/src/parquet/cdr_silver"
    checkpoint_bronze = "/tmp/src/parquet/checkpoints/cdr_bronze"
    checkpoint_silver = "/tmp/src/parquet/checkpoints/cdr_silver"
    checkpoint_gold = "/tmp/src/parquet/checkpoints/cdr_gold"

    # Option to clear historical sandbox records if run with '--clear'
    if len(sys.argv) > 1 and sys.argv[1] == "--clear":
        print("Clearing historical local Parquet tables and checkpoints...")
        import shutil
        import os
        for path in [bronze_path, silver_path, checkpoint_bronze, checkpoint_silver, checkpoint_gold]:
            if os.path.exists(path):
                shutil.rmtree(path)
                print(f"Removed: {path}")

    # ==========================================================================
    # 1. BRONZE LAYER: Consuming raw Kafka and storing JSON/Avro parsed format
    # ==========================================================================
    print(f"Subscribing to Kafka topic '{kafka_topic}' from {kafka_bootstrap_servers}...")
    
    df_kafka = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", kafka_bootstrap_servers) \
        .option("subscribe", kafka_topic) \
        .option("startingOffsets", "earliest") \
        .load()

    # Retrieve schema registry definition dynamically
    avro_schema_json = fetch_schema(schema_registry_url, f"{kafka_topic}-value")

    # Slice out 5-byte Confluent schema wire-format header before passing to Spark from_avro
    df_avro_extracted = df_kafka.select(
        expr("substring(value, 6, length(value) - 5)").alias("avro_payload"),
        col("timestamp").alias("kafka_ingest_time")
    )

    # Deserialize Avro payload
    df_bronze = df_avro_extracted.select(
        from_avro(col("avro_payload"), avro_schema_json).alias("data"),
        col("kafka_ingest_time")
    )

    # Write streams continuously into Bronze Parquet directory
    print(f"Starting Bronze stream writer targeting: {bronze_path}")
    bronze_query = df_bronze.select("data.*", "kafka_ingest_time") \
        .writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_bronze) \
        .start(bronze_path)

    # Wait for Bronze to complete at least one batch natively
    print("Waiting for Bronze Parquet table to process first batch...")
    while bronze_query.lastProgress is None:
        if bronze_query.exception() is not None:
            raise bronze_query.exception()
        time.sleep(1)
    
    # Settle down to let files sync cleanly
    time.sleep(2)
    print("Bronze Parquet table initialized. Starting Silver layer...")

    # ==========================================================================
    # 2. SILVER LAYER: Data Normalization, Standardizations & Sanitization
    # ==========================================================================
    print(f"Reading from Bronze Parquet table for cleaning & validation...")
    
    df_bronze_stream = spark.readStream \
        .format("parquet") \
        .schema(cdr_spark_schema) \
        .load(bronze_path)

    # Enforce PII structures, trim whitespaces, cast data types, filter out bad states
    df_silver = df_bronze_stream.filter(
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

    # Write cleaned streams continuously to Silver Parquet directory
    print(f"Starting Silver stream writer targeting: {silver_path}")
    silver_query = df_silver.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("checkpointLocation", checkpoint_silver) \
        .start(silver_path)

    # Wait for Silver to complete at least one batch natively
    print("Waiting for Silver Parquet table to process first batch...")
    while silver_query.lastProgress is None:
        if silver_query.exception() is not None:
            raise silver_query.exception()
        time.sleep(1)
        
    time.sleep(2)
    print("Silver Parquet table initialized. Starting Gold sinks (Redis & Neo4j)...")

    # ==========================================================================
    # 3. GOLD LAYER / SINKS: Populating Redis State & Neo4j Multi-Model Graph
    # ==========================================================================
    print("Reading from Silver Parquet table and establishing Gold sinks (Redis & Neo4j)...")
    
    df_silver_stream = spark.readStream \
        .format("parquet") \
        .schema(silver_spark_schema) \
        .load(silver_path)

    # Trigger custom micro-batch execution feeding Redis pipeline and Neo4j bolt driver
    gold_query = df_silver_stream.writeStream \
        .foreachBatch(write_to_redis_and_neo4j) \
        .option("checkpointLocation", checkpoint_gold) \
        .start()

    # Block driver execution thread until streaming terminations
    print("SDP Streaming Pipeline is running. Waiting for stream terminations...")
    spark.streams.awaitAnyTermination()

if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("Streaming pipeline interrupted by user. Exiting gracefully...")
