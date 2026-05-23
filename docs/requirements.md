# User & Performance Requirements: Project Argus

## 1. Scope & Objective

The system must ingest, process, store, and analyze massive volumes of Call Detail Records (CDR) and IP Detail Records (IPRD) to generate actionable intelligence. It must empower non-technical intelligence analysts to identify patterns of life, track target movements, map criminal or terrorist network relationships, and receive sub-second alerts on high-value targets (HVT).

```
   ┌─────────────────────────────────────────────────────────────┐
   │                       PROJECT ARGUS                         │
   │           High-Velocity Intel Processing Pipeline           │
   └───────────────┬─────────────────────────────┬───────────────┘
                   ▼                             ▼
        [CDR Ingest & Map]             [IPRD Ingest & Map]
      - Voice, SMS, Cell Towers       - IP Allocations, Sessions
                   │                             │
                   └──────────────┬──────────────┘
                                  ▼
                   [Dual Projection Storage Hub]
                   - HDFS/Delta (Historical Core)
                   - Solr (Instant Field Index)
                   - Neo4j (Relationship Graph)
                                  │
                                  ▼
                   [Analyst Interface Dashboard]
                   - Real-Time Alerts & Watchlists
                   - Pattern-of-Life Visualizations
```

---

## 2. Operational User Personas

To enforce strict security and optimize interface design, Project Argus supports three distinct operational roles. All system features and workspace configurations are mapped to these authorization profiles, detailed in [User Features & Capabilities](features.md).

### 2.1 Senior Case Analyst (Clearance Level 3)
* **Operational Focus:** Lead investigator and caseworker managing complex target networks and operational clearances.
* **Key Privileges:**
  * View unmasked, raw telecommunication MSISDNs, IMSIs, and leased IP histories.
  * Define and manage critical target watchlists and coordinate geo-fence coordinate boundaries.
  * Export target movement summaries and graph relationship structures for court-admissible evidence.
  * Authorize manual override permissions for junior analysts working on specific active cases.

### 2.2 Operations Officer (Clearance Level 2)
* **Operational Focus:** Real-time tactical tracking, pattern evaluation, and threat mitigation in the field.
* **Key Privileges:**
  * Access masked-by-default records (`+1-XXX-XXX-4321`) to perform spatial-temporal tracing.
  * Run multi-degree graph relationship traversals to trace communication chains.
  * Receive and resolve real-time geo-fencing and watchlist notification triggers.
  * Build Pattern of Life (PoL) reports and identify spatial anomalies.

### 2.3 Compliance & Auditing Inspector (Clearance Level 1)
* **Operational Focus:** Unbiased system governance, legal compliance, and internal threat assessment.
* **Key Privileges:**
  * Read-only access to the **Immutable System Audit Trail**.
  * Trace and inspect every single analyst query, data export, and session log.
  * Verify PII masking compliance across all indexing systems.
  * Audit active watchlists against judicial warrants.

---

## 3. Functional Requirements (What the System Must Do)

### 3.1 Multi-Source Data Ingestion

* **FR-1: Heterogeneous Ingestion** `[Priority: P0 - Critical]`
  * **Description:** Ingest raw CDR (voice call records, SMS events, cell tower IDs) and IPRD (IP allocations, source/destination IPs, timestamps, port numbers, transfer data volumes) from multiple national telecommunications operators and Internet Service Providers (ISPs).
* **FR-2: Ingestion Modes** `[Priority: P0 - Critical]`
  * **Description:** Support both **batch loading** (historical logs via secure FTP or Amazon S3) and **real-time streaming** (live network feeds from landing zones).

### 3.2 Advanced Analytical Capabilities

* **FR-3: Link & Graph Analysis** `[Priority: P0 - Critical]`
  * **Description:** Enable analysts to discover hidden networks and relationships.
  > [!NOTE]
  > *Example Query:* "Find all phone numbers that have been in contact with Target A, and any dynamic IP addresses those contact entities have leased."
* **FR-4: Pattern of Life (PoL) Mapping** `[Priority: P1 - High]`
  * **Description:** Analyze temporal (time-based) and spatial (geographic/cell tower location) records to build routine activity profiles for targets, flagging sudden anomalies (e.g., a target switching to a new burner device or initiating IP sessions at 3:00 AM from an unusual coordinate).
* **FR-5: Co-Location & Chaining Detection** `[Priority: P1 - High]`
  * **Description:** Detect "burner phone" swaps or secret meetings by identifying when two or more distinct SIM cards/IMEIs move sequentially through the same cell tower coordinates at identical times.
* **FR-6: IP-to-Entity Resolution** `[Priority: P0 - Critical]`
  * **Description:** Correlate dynamic IP addresses from IPRD logs with specific timestamps and subscriber registration logs to identify exactly which device or physical user was leased an IP during a specific cyber security event.

### 3.3 Real-Time Alerting & Triggering

* **FR-7: Geo-Fencing** `[Priority: P1 - High]`
  * **Description:** Trigger an immediate notification when a monitored IMEI/IMSI registers at a specific cell tower or enters a pre-defined geographic buffer zone.
* **FR-8: Watchlist Triggers** `[Priority: P0 - Critical]`
  * **Description:** Provide sub-second alerting if a target on a critical watch list makes a voice call, sends an SMS, or initiates an active IP session.

---

## 4. Analytical Scenarios & Workflows

To ensure development alignment, the system must accommodate two end-to-end analytical investigative workflows:

### Scenario A: Burner Phone Co-Location & Chaining
```
  [Target A Discards SIM] ─► Towers trace Target A's route
                                     │ (Spatial-Temporal Join)
                                     ▼
                             [Co-Location Engine] ─► Identifies SIM B moving along identical path
                                                             │
                                                             ▼
                                                     [Chaining Alert] ─► Triggers watchlist auto-update
```
1. **The Event:** Target A (monitored IMEI) enters a dark zone and discards their active SIM card.
2. **Automatic Detection:** The *Co-Location Engine* queries cell tower records to locate any other SIM card that initialized a registration sequence at the same coordinates within a $\pm$ 2-minute window and followed an identical trajectory for the subsequent 3 cell towers.
3. **Graph Analysis:** The system identifies SIM B (burner SIM) and runs a *Chaining Analysis* to map SIM B's incoming/outgoing calls, exposing their updated contact network. Graph operations are processed by the relationship graph engine detailed in [System Architecture (HLD)](architecture.md#1-detailed-backend-architecture).
4. **Action:** The Senior Case Analyst reviews the auto-generated correlation report and updates the active target watchlist with SIM B's identifiers.

### Scenario B: Cyber Incident Entity Resolution
1. **The Event:** A cyber threat event occurs from dynamic IP `198.51.100.42` at exactly `2026-05-24T01:30:00Z`.
2. **Correlation:** The Operations Officer inputs the IP and timestamp into the *IP-to-Entity Resolution* panel.
3. **Join Processing:** Spark queries IPRD records to locate the leased IP session active during that microsecond, resolving it to a specific IMSI and MSISDN. Dynamic joins utilize the key partitioning and logs detailed in [ingestion.md](ingestion.md#32-partitioning--key-selection).
4. **Spatial Mapping:** Once resolved, the system pulls the target's CDR cellular logs and renders their geographic movement history on the GIS Map, identifying where they were physically located when the cyber incident occurred.

---

## 5. Non-Functional Requirements (Performance & Compliance Constraints)

### 5.1 Scale & Performance

* **NFR-1: Horizontal Data Volume Scale** `[Priority: P0 - Critical]`
  * **Description:** Horizontal scale architecture to process and store an estimated **5 to 10 billion events per day**.
* **NFR-2: Long-Term Storage Retention** `[Priority: P0 - Critical]`
  * **Description:** Maintain active historical records spanning at least **5 years** of historical data for long-term intelligence lookups.
* **NFR-3: Service Level Agreements (SLAs) & Latency** `[Priority: P1 - High]`
  * The platform must adhere to the following strict latency boundaries under peak processing loads:

| SLA ID | Query/Action | Target Latency | Maximum Allowable | Measurement Method |
| :--- | :--- | :--- | :--- | :--- |
| **SLA-1** | Simple Target Lookup | $< 1.0$ s | $< 2.0$ s (95th %ile) | End-to-end API roundtrip |
| **SLA-2** | 3-Degree Graph Traversal | $< 15.0$ s | $< 30.0$ s | Neo4j query execution |
| **SLA-3** | Watchlist Alert Trigger | $< 500$ ms | $< 1000$ ms | Ingestion to WebSocket |
| **SLA-4** | Ingestion-to-Storage Latency | $< 5.0$ s | $< 10.0$ s | NiFi ingress to Delta Lake write |

### 5.2 Security, Auditing, and Compliance

> [!IMPORTANT]
> **Strict Operational Security Mandates:**
> * **NFR-4: Role-Based Access Control (RBAC)** `[Priority: P0 - Critical]`: Field-level masking must be strictly enforced. Junior analysts must see masked phone numbers (`+1-XXX-XXX-1234`) unless granted explicit administrative clearance for a specific target/case.
> * **NFR-5: Immutable Auditing** `[Priority: P0 - Critical]`: Every single search, query, record view, and data export conducted by an analyst must be logged in an unalterable, tamper-proof audit trail for internal security reviews.
> * **NFR-6: Data Compartmentalization** `[Priority: P0 - Critical]`: Absolute isolation of case data. Operation B must never see Operation A's target lists or analytical workspace files, even when operating on the same physical big data cluster.

### 5.3 Availability & Fault Tolerance

> [!WARNING]
> **Availability Requirements:**
> * **NFR-7: Pipeline Ingestion Resilience** `[Priority: P0 - Critical]`: The platform must guarantee **99.999% availability** for raw data ingestion (allowing a maximum of **5.26 minutes of unplanned ingestion downtime per calendar year**) to ensure critical intelligence feeds are never dropped.
> * **NFR-8: Disaster Recovery (DR)** `[Priority: P1 - High]`: Establish a multi-region active-passive or active-active topology to guarantee zero data loss (**RPO = 0**) and sub-minute recovery time (**RTO < 60s**) in the event of hardware or data center failure.

---

## 6. Data Lifecycle & Retention Policy

To manage storage efficiency and legal compliance, data transitions through a tiered lifecycle:

```
  [Raw Ingest] ──► Hot Tier (HDFS Delta) ──► Warm Tier (Delta Cold) ──► Purge
                    - 90 Days                 - 5 Years                  - Absolute Wipe
                    - Active indices          - Compressed Parquet       - NIST Standard
```

1. **Hot Ingestion Tier (0 - 90 Days):** High-availability Delta Lake storage on HDFS. Fully indexed in Solr and mapped in Neo4j. Optimized for instant queries.
2. **Warm Ingestion Tier (91 Days - 5 Years):** Columns compressed, indices pruned. Historical raw logs are aggregated into yearly Parquet blocks on cold storage. Solr indexes are cleared; lookups use Spark cluster sweeps.
3. **Purge Boundary (5+ Years):** In accordance with national archive rules, records are securely deleted using NIST SP 800-88 compliant physical storage wiping protocols.

---

## 7. Regulatory & Security Compliance Framework

To comply with federal and international communications privacy frameworks, Project Argus integrates:

| Compliance Standard | Target Mandate | Platform Realization |
| :--- | :--- | :--- |
| **ISO/IEC 27001** | Security Management controls for sensitive records. | Fully audited through PostgreSQL secure schemas and GSSAPI authentication. |
| **GDPR/PII Data Privacy** | Minimization and masking of PII logs. | Handled via localized OpenTelemetry regex filters that scrub fields prior to indexing. |
| **Tamper Evidence** | Unalterable log entries for administrative investigations. | Realized through hash-chained log segments and append-only database configurations. |

---

## 8. Requirements Mapping Matrix

To ensure development alignment, the table below maps each core high-level user requirement to its technical realization layer in the design:

| ID | User Requirement | Priority | Technical Component | Strategic Role |
| :--- | :--- | :--- | :--- | :--- |
| **FR-1** | Real-Time Stream Ingestion | `P0` | Apache NiFi & Apache Kafka | Ingress gateway captures incoming streams and buffers them safely. |
| **FR-2** | Batch Loading (Historical) | `P0` | Apache NiFi & Spark Batch | Pulls historical logs from S3/SFTP and processes them into HDFS. |
| **FR-3** | Link & Graph Analysis | `P0` | Neo4j Graph Database | Maps entities (SIM, IMEI, IP) as nodes and interactions as edges. |
| **FR-4** | Pattern of Life Mapping | `P1` | Apache Spark & Solr | Runs temporal-spatial analysis and stores indexed location histories. |
| **FR-5** | Co-Location Detection | `P1` | Apache Spark Core | Executes parallel trajectory-matching algorithms on HDFS data. |
| **FR-6** | IP-to-Entity Resolution | `P0` | Apache Spark & Solr | Joins dynamic IP logs with subscriber tables using timestamp windows. |
| **FR-7** | Geo-Fencing Alerts | `P1` | Spark Streaming & PostgreSQL | Evaluates incoming cell tower logs against active geo-fences. |
| **FR-8** | Watchlist Triggers | `P0` | Spark Streaming & PostgreSQL | Sub-second check of incoming event streams against active watchlist hashes. |
| **NFR-1** | 10 Billion Daily Events | `P0` | HDFS & Delta Lake | Columnar, compressed storage with partition pruning and data-skipping. |
| **NFR-2** | 5-Year Archive Retention | `P0` | HDFS Storage Tier | Low-cost distributed replication across commodity clusters. |
| **NFR-3** | Latency SLAs | `P1` | Multi-Model Architecture | Bypasses HDFS reads using Solr indices and Neo4j graph RAM pointers. |
| **NFR-4** | Field Masking (RBAC) | `P0` | Secure API Gateway | Masking algorithms filter JSON outputs based on analyst tokens. |
| **NFR-5** | Immutable Auditing | `P0` | PostgreSQL & Grafana Loki | Tamper-proof table structures track every query signature and user ID. |
| **NFR-7** | 99.999% Ingestion Uptime | `P0` | Kafka Partitioning | Distributed message replication prevents loss during broker failures. |


