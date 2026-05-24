# Project Argus: High-Scale Telecommunication & IP Intelligence Platform

[![License: Proprietary](https://img.shields.io/badge/License-Proprietary-red.svg)](#)
[![Status: Architecture & Design Phase](https://img.shields.io/badge/Status-Architecture%20%26%20Design%20Phase-blue.svg)](#)
[![Stack: Enterprise Big Data](https://img.shields.io/badge/Stack-Enterprise%20Big%20Data-orange.svg)](#)
[![Compliance: ISO 27001 & GDPR](https://img.shields.io/badge/Compliance-ISO%2027001%20%7C%20GDPR-success.svg)](#)

**Project Argus** is a state-of-the-art, high-scale telecommunication and IP network intelligence platform. Named after the hundred-eyed giant of Greek mythology, Argus is engineered to ingest, process, correlate, and analyze massive volumes of Call Detail Records (CDR) and IP Detail Records (IPRD) in real time. 

The system enables national security and intelligence analysts to perform rapid spatial-temporal tracking, trace multi-degree communication networks, build target "patterns of life," and trigger sub-second geo-fencing alerts—all while maintaining military-grade privacy controls and immutable auditing.

---

## 🚀 Platform Capabilities

* **High-Velocity Ingestion:** Horizontally scales to ingest and parse **5 to 10 billion events per day** from diverse national telco and ISP landing zones.
* **Dual-Projection Analytical Storage:** Spark updates specialized read-optimized indices in **Apache Solr** and relationship networks in **Neo4j** dynamically, bypassing slow HDFS disk scans.
* **Spatial-Temporal Pattern Matching:** Uncovers target meeting points, co-location trajectories, and "burner phone" swaps automatically.
* **IP-to-Entity Resolution:** Translates transient, dynamic IP leases back to physical subscriber identities within milliseconds using historic window joins.
* **Military-Grade Security & Anonymization:** Localized regex-based scrubbers run on **OpenTelemetry Collectors** to strip subscriber PII from debugging and application logs before storage.

---

## 🏛️ System Architecture Overview

Project Argus is structurally divided into a high-scale backend distributed computing pipeline and a responsive, state-synchronized mission-control frontend console.

```
              [Telco & ISP Landing Zones]
                          │ (Secure FTP / S3 feeds)
                          ▼
  ┌─────────────────────────────────────────────────────────┐
  │         INGESTION LAYER (Edge Gateway & Buffer)         │ ◄── [docs/ingestion.md]
  │ - Apache NiFi: Decryption, parsing, and normalization   │
  │ - Apache Kafka: Partitioned, shock-absorbing buffers    │
  └───────────────────────┬─────────────────────────────────┘
                          │
                          ▼ (IMSI-Hash Partitioned Streams)
  ┌─────────────────────────────────────────────────────────┐
  │          COMPUTE & DATA LAKE TIER (ACID Core)           │ ◄── [docs/architecture.md (Backend)]
  │ - Apache Spark: Structured streaming and window joins   │
  │ - Delta Lake on HDFS: Transactional columnar storage    │
  └───────────────────────┬─────────────────────────────────┘
                          │
                          ▼ (Incremental Aggregation Projections)
  ┌─────────────────────────────────────────────────────────┐
  │         SPECIALIZED ANALYTICAL QUERY PLATFORMS          │ ◄── [docs/architecture.md (Backend)]
  │ - Apache Solr: Sub-second target text index lookups     │
  │ - Neo4j Graph Database: Real-time relationship maps     │
  └───────────────────────┬─────────────────────────────────┘
                          │
                          ▼ (Secure Spring Boot API Gateway)
  ┌─────────────────────────────────────────────────────────┐
  │        ANALYST MISSION-CONTROL CONSOLE (React)          │ ◄── [docs/architecture.md (Frontend)]
  │ - Redux RTK Query: Automated caching & WebSocket sync   │ 
  │ - Cytoscape.js Network Graphs & Leaflet Spatial Maps    │
  └─────────────────────────────────────────────────────────┘
```

> [!TIP]
> * For details on Ingress Parsing, Kafka Hashing keys, and Backpressure: See the [Ingestion Layer Architecture](docs/ingestion.md).
> * For detailed component-by-component structures and data schemas: See the [System Architecture](docs/architecture.md).
> * For legal frameworks, user personas, and target SLAs: See the [User & Performance Requirements](docs/requirements.md).

---

## 🛠️ Complete Technology Stack

Project Argus utilizes a highly structured, enterprise-grade technology stack divided into distinct operational boundaries:

| Layer | Technology | Primary Role in Project Argus | Strategic Importance |
| :--- | :--- | :--- | :--- |
| **Ingestion Edge** | **Apache NiFi** | Border Gateway | Safely decrypts, validates, and parses incoming telco log files. |
| **Ingestion Edge** | **Apache Kafka** | Persistent Core Buffer | Absorb spikes up to billions of daily logs without dropping data packets. |
| **Compute & Store**| **Apache Spark** | Distributed Compute Brain | Processes micro-batch streaming runs and heavy analytical joins. |
| **Compute & Store**| **Delta Lake** | ACID Transaction Layer | Guarantees write reliability, scheme integrity, and fast file reads on HDFS. |
| **Compute & Store**| **HDFS** | Immutable Data Lake | Scalable, low-cost commodity disk cluster storing 5+ years of historical data. |
| **Analytical Query**| **Apache Solr** | Search Text Index | Powers sub-second target lookup queries without table scans. |
| **Analytical Query**| **Neo4j** | Graph Relationship Engine | Runs Cypher logic to trace multi-degree target connections in memory. |
| **Metadata DB**    | **PostgreSQL** | Operational Database | Manages watchlists, analyst settings, and user authorization tokens. |
| **User Interface** | **React v19** | Modern Frontend Framework | Renders heavy network graphs, geographic traces, and search views. |
| **User Interface** | **Tailwind CSS v4**| Design System Compiler | Rapid styling implementation utilizing Rust-based Lightning CSS. |
| **User Interface** | **Vite** | Frontend Build Engine | High-performance bundling and ultra-fast developer hot-reloading. |
| **Observability**  | **Prometheus & Grafana** | Cluster Metrics Dashboard | Tracks ingestion rates, CPU core usage, and memory bottlenecks. |
| **Observability**  | **OpenTelemetry & Loki** | Scrubbed System Logs | Aggregates application log traces, stripping phone numbers/IPs. |

---

## 📂 Repository Navigation Hub

The repository is structured to separate system requirements and HLD models from the implementation source:

| Directory / File | Description | Deep Dive Links |
| :--- | :--- | :--- |
| 🗂️ **`/docs`** | Contains complete engineering planning and architectural diagrams. | [Browse Docs Directory](docs/) |
| 📄 `vision.md` | Background details on the project origin and selection of the "Argus" codename. | [Project Vision & Pillars](docs/vision.md) |
| 📄 `requirements.md` | User personas, operational scenarios, and prioritized SLA limits. | [User & Performance Requirements](docs/requirements.md) |
| 📄 `architecture.md` | Detailed separate frontend & backend topology maps and data schemas. | [System Architecture (HLD)](docs/architecture.md) |
| 📄 `ingestion.md` | Deep-dive on Edge ingestion, NiFi processing, and Kafka buffers. | [Ingestion Layer Architecture](docs/ingestion.md) |
| 📄 `features.md` | End-user workspace capabilities, GIS tracking, and network graphs. | [User Features & Capabilities](docs/features.md) |
| 📄 `sandbox_setup.md` | Guide for running local Kafka (KRaft), Kafka UI, Solr, and Neo4j. | [Local Sandbox Setup](docs/sandbox_setup.md) |
| 🗂️ **`/src`** | Root development directory for source code components. *(Design Phase)* | [Browse Src Directory](src/) |

---



## 🤝 Development & Contribution Guidelines

Project Argus is an enterprise intelligence application requiring rigorous quality controls. All developers must adhere to the engineering standards detailed below:

### 1. Branching Strategy (GitFlow)
We employ a strict GitFlow branching model. Direct commits to `main` or `develop` are blocked.
* **Feature Branches:** named `feature/issue-[id]-short-description` (branched off `develop`).
* **Bug Fixes:** named `bugfix/issue-[id]-short-description` (branched off `develop`).
* **Hotfixes:** named `hotfix/issue-[id]-short-description` (branched off `main`, merged to both `main` and `develop`).

### 2. Coding Standards & Lints
All source code submissions are subject to pre-commit styling and code quality validations:
* **Backend (JVM):** ktlint and Spotless rules are checked during builds.
  ```bash
  # Check backend code compliance
  ./gradlew spotlessCheck
  ```
* **Frontend (React/Vite):** Prettier and ESLint formatting gates.
  ```bash
  # Validate frontend lint compliance
  npm run lint
  ```

### 3. Continuous Integration (CI) Quality Gates
Pull requests must pass automated pipeline sweeps prior to peer review approval:
* **Test Coverage:** All business logic components must maintain a minimum of **85% unit test coverage** (JaCoCo reports are scanned automatically).
* **Static Analysis:** Zero blocker or critical vulnerabilities flagged by SonarQube.

---

## 🗺️ Project Development Roadmap

Project Argus is currently in the **Architecture & Design Phase**. Implementation is organized across four distinct milestones:

```
┌────────────────────────────────────────────────────────────────────────────┐
│                              DEVELOPMENT ROADMAP                           │
├──────────────────────┬─────────────────────────────────────────────────────┤
│  PHASE 1 (Weeks 1-4) │ Ingestion & Storage Integration                     │
│                      │ - Establish NiFi edge flows                         │
│                      │ - Spin up Kafka partitioned clusters                │
│                      │ - Implement Spark streaming jobs to HDFS Delta Lake │
├──────────────────────┼─────────────────────────────────────────────────────┤
│  PHASE 2 (Weeks 5-8) │ Specialized Projection & Graph Services             │
│                      │ - Integrate Spark to Neo4j graph nodes              │
│                      │ - Map Spark logs to Apache Solr text collections    │
│                      │ - Develop Spark IP-to-Entity dynamic window joins   │
├──────────────────────┼─────────────────────────────────────────────────────┤
│  PHASE 3 (Weeks 9-11)│ Secure API Gateway & Compliance Systems             │
│                      │ - Build secure Kotlin-based API Gateway             │
│                      │ - Standardize Postgres RBAC and field-level masking │
│                      │ - Configure OpenTelemetry regex PII log scrubbers   │
├──────────────────────┼─────────────────────────────────────────────────────┤
│  PHASE 4 (Weeks 12+) │ Mission-Control UI Dashboard                        │
│                      │ - Compile React 19 visual frame                     │
│                      │ - Build Redux RTK Query server integrations         │
│                      │ - Render live geo-fencing maps and graph nodes      │
└──────────────────────┴─────────────────────────────────────────────────────┘
```
