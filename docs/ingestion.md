# Ingestion Layer Architecture: Project Argus

The Ingestion Layer of **Project Argus** serves as the critical, secure gateway between external telecommunications operators and Internet Service Providers (ISPs) and the air-gapped internal big data compute clusters. It is designed to handle **5 to 10 billion events per day** with **99.999% availability**, ensuring zero data loss during high-volume spikes or downstream processing delays.

---

## 1. Architectural Topology Overview

The Ingestion Layer leverages **Apache NiFi** for edge ingress logistics, validation, and serialization, and **Apache Kafka** as a shock-absorbing, highly partitioned, immutable messaging queue.

```
       [External Telecom SFTP Nodes]      [ISP Secure S3 Buckets]
                     │                              │
                     ▼                              ▼
  ┌────────────────────────────────────────────────────────────────┐
  │                   APACHE NIFI INGESTION EDGE                   │
  │  - Encrypted Protocol Handshakes & Polling                     │
  │  - File Integrity & Checksum Validations                       │
  │  - Normalization: CSV/ASN.1 Binary Parsing to JSON             │
  │  - Event Ingestion Tracking & Metadata Auditing                │
  └──────────────┬──────────────────────────────┬──────────────────┘
                 │ (Normal Stream)              │ (Malformed / Corrupt)
                 ▼                              ▼
  ┌─────────────────────────────┐        ┌─────────────────────────┐
  │ APACHE KAFKA PERSISTENT CORE│        │   DEAD LETTER QUEUE     │
  │  - Shock-Absorbing Buffers  │        │   - HDFS DLQ Directory  │
  │  - IMSI-Hash Partitioning   │        │   - PostgreSQL Audit Log│
  └──────────────┬──────────────┘        └─────────────────────────┘
                 │
                 ▼ (Parallel Partition Pulls)
  [Apache Spark Compute Brain]
```

---

## 2. Ingress Protocols & Apache NiFi Gateway

Apache NiFi acts as the perimeter guard of the platform, running within an air-gapped, highly isolated network demilitarized zone (DMZ).

### 2.1 File Retrieval & Extraction
NiFi runs continuous, asynchronous scheduling loops to pull raw telecommunication logs:
* **SFTP Landing Zones:** Utilizing NiFi's `ListSFTP` and `FetchSFTP` processors to establish high-throughput SSH handshakes with telco servers, executing transactional state tracking to avoid reprocessing files.
* **ISP Object Stores:** Utilizing `ListS3` and `FetchS3Object` with IAM role-based authentication to download historical raw batch archives from Amazon S3 landing buckets.

### 2.2 Validation & Sanitization Engine
Before processing, raw streams are scrutinized to prevent pipeline contamination:
1. **Integrity Checks:** Compares MD5/SHA256 file hashes against provider manifests to confirm transmission completeness.
2. **File Format Parsing:** Handles multiple source file formats:
   * Standard CSV/TSV cellular logs.
   * Binary **ASN.1 (Abstract Syntax Notation One)** structures native to telecommunication switching centres, parsed using custom NiFi schema controllers.
3. **JSON Normalization:** Normalizes parsed schemas into uniform JSON objects matching our [CDR and IPRD data schemas](HLD.md#6-core-raw-data-schemas).
4. **Ingress Event Tracing:** Generates an immutable UUID (`event_id`) and appends ingestion metadata (ingest timestamp, operator ID, file source name) to the record payload.

---

## 3. Buffer Layer: Apache Kafka Partitioning Strategy

Normalized streams from NiFi are written directly into separate, heavily partitioned Apache Kafka topics. Kafka acts as an immutable log queue that guarantees message durability and orders streams correctly.

### 3.1 Topic Topography
The system operates two primary high-throughput topics:
* **`telecom.cdr.raw`**: Hosts all Call Detail Records (voice, SMS, tower metadata).
* **`isp.ipdr.raw`**: Hosts all IP Detail Records (session logs, port leases).

### 3.2 Partitioning & Key Selection
To guarantee that downstream **Apache Spark** engines analyze geographic movements and time-series sequences accurately, events must be routed consistently.
* **The partition key is strictly defined as the target's IMSI (International Mobile Subscriber Identity)**.
* By hashing the IMSI, Kafka's default murmur3 partitioning algorithm ensures:
  1. **Consistent Routing:** All events for a specific SIM card/device land on the exact same physical Kafka partition.
  2. **Temporal Integrity:** Preserves the precise chronological order of a target's events within that partition, preventing geo-fencing race conditions.
  3. **Even Distribution:** Hashing prevents hot-spotting across partitions since IMSI ranges are uniformly distributed.

```
                  ┌─────────► [Partition 0] ──► Target IMSI Group A
                  ├─────────► [Partition 1] ──► Target IMSI Group B
  [Raw Stream] ───┼─ (IMSI Hash Key)
                  ├─────────► [Partition 2] ──► Target IMSI Group C
                  └─────────► [Partition N] ──► Target IMSI Group Z
```

### 3.3 Topic Settings & Durability Mandates
To prevent data loss, the Kafka cluster is configured with strict storage policies:
```properties
# Durability configuration for enterprise ingestion
min.insync.replicas=2
acks=all
compression.type=lz4
cleanup.policy=delete
retention.ms=259200000   # 72-Hour retention window (Shock protection)
segment.bytes=1073741824  # 1GB active log segment rollover
```
* **acks=all:** Prevents the producer (NiFi) from receiving a success write token until all active in-sync brokers have replicated the message.
* **LZ4 Compression:** Minimizes network packet payload sizes between NiFi, Kafka, and Spark, reducing serialization CPU bottlenecks.

### 3.4 Schema Registry & Serialization Formats
To ingest billions of events daily without excessive resource consumption, the ingestion pipeline transitions from raw JSON to compact binary serialization:
* **Apache Avro Serialization**: Apache NiFi serializes parsed telecommunication logs into Avro format. Avro strips verbose key names from the payload, sending only a 5-byte header containing the Schema ID registered in the **Kafka Schema Registry** followed by compact binary data.
* **Schema Evolution & Validation**: The central Schema Registry enforces compatibility rules (such as **BACKWARD** compatibility). This prevents upstream producer modifications (e.g. adding nullable columns or renaming metadata fields) from crashing downstream processing engines like **Apache Spark** or corrupting tables in Delta Lake.

---

## 4. Backpressure Management & Resilience

At peak rates of over 100,000 events per second, the pipeline is designed to automatically adapt to database load or cluster outages.

### 4.1 System Backpressure Loop
When downstream databases (e.g., [Solr](HLD.md#3-specialized-multi-model-querying) or [Neo4j](HLD.md#3-specialized-multi-model-querying)) experience slow queries, a chain reaction of backpressure safely controls incoming loads:

```
  [Downstream DB Query Lag]
            │
            ▼
  [Spark Compute Throttle] ──► Spark pauses consuming from Kafka
            │
            ▼
  [Kafka Log Accumulation] ──► Partitions reach retention buffer boundaries
            │
            ▼
  [NiFi Connection Pause]  ──► NiFi pauses SFTP/S3 downloads based on Kafka queue size
```

1. **NiFi Object Thresholds:** NiFi connections between internal processors utilize **threshold limits** (e.g., max 50,000 flowfiles or 10GB data queue limits). When a queue threshold is crossed, upstream file pulls pause automatically.
2. **SFTP Landing Buffers:** If NiFi pulls pause, telco files queue safely on the provider’s landing server directories (which are provisioned with 48-hour storage buffers).

### 4.2 Error Handling & Dead Letter Queue (DLQ)
Records that fail parsing, validation, or schema validation are never dropped:
* **Identification:** The NiFi parse engine catches formatting exceptions.
* **Routing:** Failed records route to a **Dead Letter Queue (DLQ)** on a separate, monitored HDFS directory: `/data/ingress/dlq/`.
* **Metadata Attachment:** The error flowfile includes appended context attributes:
  * `error.reason`: The exact parsing stacktrace.
  * `error.timestamp`: Time of ingestion failure.
  * `error.source`: Path of the origin file.
* **Auditing:** Ingress alerts are registered in the operational PostgreSQL DB, prompting operational teams to review malformed formats without interrupting the main pipeline.
