# Project Argus: CDR Streaming Pipeline

This directory contains the core PySpark Streaming Pipeline script for Project Argus. It is designed to read Call Detail Records (CDRs) from Apache Kafka, process them through a Medallion Architecture (Bronze -> Silver -> Gold), and stream them into Redis and Neo4j.

---

## 🏛️ Pipeline Architecture

1. **Bronze Stage (Raw)**:
   - Continuously reads from the `cdr-records` Kafka topic.
   - Slices off Confluent's 5-byte schema wire-format header from the binary payload.
   - Deserializes the Avro payload dynamically using the Schema Registry schema.
   - Appends raw records and Kafka metadata directly to the Bronze Parquet table under `src/parquet/cdr_bronze`.

2. **Silver Stage (Cleaned)**:
   - Reads the raw stream from the Bronze Parquet table.
   - Validates that mandatory keys are present (filters out missing IDs, MSISDNs, and IMSIs).
   - Normalizes data: trims whitespaces from caller strings, casts session durations to integers, and parses timestamps.
   - Writes cleaned records to the Silver Parquet table under `src/parquet/cdr_silver`.

3. **Gold Stage (Sinks)**:
   - Reads the cleaned stream from the Silver Parquet table.
   - Executes parallelized executor writes via `foreachPartition` batch sinks to:
     - **Redis Cache**: Creates bidirectional mapping lookups (`imsi:<imsi> -> msisdn` and `msisdn:<msisdn> -> imsi`) and records hash states (`subscriber:state:<msisdn>`) containing the latest cell tower, network operator, device details, and status.
     - **Neo4j Graph**: Merges transaction nodes (`Phone`, `IMSI`, `IMEI`, `CellTower`) and builds connection edges (`CALLS`, `SENDS_SMS`, `CONNECTED_TO`) to model communications and locations in real-time.

---

## 🚀 How to Run the Pipeline

### Step 1: Start the Infrastructure Stack
Make sure the sandbox containers are built and running. From the project root, execute:
```bash
docker compose up -d
```

### Step 2: Running the Imperative Pipeline
Submit the standard PySpark Structured Streaming script to the standalone Spark master cluster. The shared `./src` folder is mapped to `/tmp/src` inside the container:

```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /tmp/src/cdr_streaming_pipeline.py
```

#### Clearing Historical Data (Optional)
If you want to clear historical checkpoint logs and Parquet tables to start with a clean slate, append the `--clear` argument:
```bash
docker exec -it spark-master /opt/spark/bin/spark-submit \
  --master spark://spark-master:7077 \
  /tmp/src/cdr_streaming_pipeline.py --clear
```

### Step 3: Running the Spark Declarative Pipeline (SDP)
Alternatively, a declarative implementation of the Bronze and Silver stages is available at [cdr_declarative_pipeline.py](file:///Users/ervijay/Documents/Programs/Repo/argus/src/cdr_declarative_pipeline.py) using the Spark 4.1+ Declarative Pipelines (SDP) API.

It utilizes `@dp.table` decorators to model the ingestion graph. Run it via the `spark-pipelines` CLI inside the `spark-master` container, using the [spark-pipeline.yml](file:///Users/ervijay/Documents/Programs/Repo/argus/src/spark-pipeline.yml) specification:

```bash
# Execute the SDP pipeline using the Spark pipelines runner
docker exec -it spark-master /opt/spark/bin/spark-pipelines run --spec /tmp/src/spark-pipeline.yml
```

SDP will execute the streaming flows to completion and write the materialized output tables under the container's Spark warehouse directory: `/opt/spark/work-dir/spark-warehouse/cdr_bronze` and `/opt/spark/work-dir/spark-warehouse/cdr_silver`.


---

## 🔬 Verifying Outputs

### 1. Verify Redis Key-Value Cache
Connect to the Redis CLI inside the container and check the database state:
```bash
# Check the total number of keys in Redis
docker exec -it redis redis-cli dbsize

# Fetch the bidirectional mapping for an IMSI
docker exec -it redis redis-cli get "imsi:405840352502016"

# Fetch the latest session status for a subscriber number
docker exec -it redis redis-cli hgetall "subscriber:state:+918853949905"
```

### 2. Verify Neo4j Graph Database
Run Cypher queries in Neo4j to check merged entities and communication relationships:
```bash
# Count active nodes by label
docker exec -it neo4j cypher-shell -u neo4j -p argus-local-dev-password \
  "MATCH (n) RETURN labels(n), count(n);"

# Count active relationships by type
docker exec -it neo4j cypher-shell -u neo4j -p argus-local-dev-password \
  "MATCH ()-[r]->() RETURN type(r), count(r);"
```
You can also open the Neo4j Browser in your host browser at **[http://localhost:7474](http://localhost:7474)** (using credentials `neo4j / argus-local-dev-password`) to visually traverse calls, SMS events, IMSI mappings, and cell tower connections!
