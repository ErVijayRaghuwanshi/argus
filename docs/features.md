# User Features & Analytical Capabilities: Project Argus

**Project Argus** delivers a high-fidelity, visual mission-control portal that transforms billions of raw, disparate telecommunications (CDR) and internet (IPRD) records into actionable intelligence. 

Designed specifically for intelligence analysts, case investigators, and security auditors, the platform provides an intuitive, web-based analytical workspace. Below is an exhaustive breakdown of what end users get and what they can do with Project Argus.

---

## 1. Core Feature Portal Overview

```
                        ┌─────────────────────────────────────┐
                        │    ARGUSMISSION-CONTROL PORTAL      │
                        └──────────────────┬──────────────────┘
                                           │
         ┌──────────────────┬──────────────┼──────────────┬──────────────────┐
         ▼                  ▼              ▼              ▼                  ▼
  [Unified Search]   [Network Graph]   [GIS Maps]   [Burner Wizard]   [Watchlist Hub]
  - Entity Profiles  - Cypher Links    - Routes     - Co-Location     - Sub-Second Alerts
  - Masked Defaults  - Active Filters  - Playback   - Trajectories    - Geo-Fencing
```

---

## 2. Exhaustive Feature Breakdown

### 2.1 Unified Search & Subscriber Entity Profiles
End users get a centralized search interface that serves as the entry point for all investigations.
* **What Analysts Can Do:**
  * Search the historical repository instantly using any target identifier: a phone number (MSISDN), a SIM card ID (IMSI), a device hardware serial (**IMEI**), or a dynamic IP address.
  * Retrieve a comprehensive, unified **Subscriber Entity Profile** compiling subscriber identity records, active SIM/device logs, primary tower associations, and dynamic IP lease history.
  * Junior analysts see masked identifiers (`+1-XXX-XXX-7654`, `198.51.XX.XX`) by default in compliance with privacy mandates, while Senior Case Analysts can toggle unmasked views under active judicial warrants.

### 2.2 Interactive Relationship Graph Visualizer
Analysts get an interactive network visualization canvas powered by **Neo4j** and rendered in the browser via **Cytoscape.js**.
* **What Analysts Can Do:**
  * Render target phone numbers, SIM cards, and devices as nodes, with communication events (voice calls, SMS messages, IP connections) represented as directional linking edges.
  * Double-click any node to run immediate multi-degree contact tracing, expanding their local network recursively in real time.
  * Apply active timeline filters to view relationships active only during specific time windows (e.g., "communication chains active between 2:00 AM and 5:00 AM").
  * Filter relationship links by call durations, data volumes, or message frequency to isolate critical co-conspirators.

### 2.3 Spatial-Temporal GIS Tracking & Playback
Analysts get an interactive, GIS-enabled mapping interface utilizing Leaflet.js to map spatial tower movements chronologically.
* **What Analysts Can Do:**
  * Render a target's historical movements geographically by mapping their chronological cell tower registrations.
  * Use the **Time-Slider Playback Control** to reconstruct target movement vectors over days, hours, or minutes, displaying travel direction and velocity vectors.
  * View overlapping coverage circles of cellular towers to estimate a target's physical location boundaries during specific events.
  * Plot multiple targets simultaneously to visually identify meeting points or geographic overlaps.

### 2.4 Burner Phone Swap & Co-Location Wizard
Investigators get a specialized co-location tracking wizard designed to automatically expose target burner device swaps.
* **What Analysts Can Do:**
  * Input two or more suspicious SIM cards or device IMEIs into the analytical wizard.
  * The *Co-Location Engine* calculates overlapping trajectories, identifying SIM cards that registered at identical cell tower coordinates sequentially within tight $\pm 2$-minute windows.
  * Expose sequential "burner phone" swaps (e.g., when Target A discards SIM A, and immediately powers on SIM B at the same coordinate location, following the same route).
  * Export probabilistic co-location match scores to build court-admissible forensic evidence files.

### 2.5 Dynamic IP-to-Entity Resolution Console
Field Officers get a simple, high-speed resolution console to correlate cyber incidents back to physical cellular subscribers.
* **What Analysts Can Do:**
  * Input any dynamic, short-lived IP address and a precise microsecond-level UTC timestamp captured during a cyber or network event.
  * The system queries dynamic ISP allocation tables to instantly resolve which subscriber IMSI/MSISDN was leased that IP during the specified event window.
  * Instantly jump from a resolved IP profile directly to their physical spatial-temporal cell tower movements on the GIS Map.

### 2.6 Real-Time Watchlist & Geo-Fencing Hub
Analysts get a low-latency, real-time alert dashboard powered by Spark Streaming WebSockets.
* **What Analysts Can Do:**
  * Maintain active case watchlists by hashes or encrypted target parameters.
  * Configure custom **Geo-Fences** by drawing custom circular or polygonal boundaries directly onto the GIS Map.
  * Receive sub-second visual notifications and audio alerts in the dashboard when a watched device registers at a tower within a geo-fenced zone, or initiates a voice call/SMS.
  * Automatically route critical alert logs to secure PGAdmin audit tables for compliance verification.

---

## 3. Operational Workspaces by User Persona

All features are strictly partitioned to ensure compliance with international legal frameworks:

| Feature Module | Senior Case Analyst (Clearance Level 3) | Operations Officer (Clearance Level 2) | Compliance Inspector (Clearance Level 1) |
| :--- | :--- | :--- | :--- |
| **Unified Search** | Full unmasked profile search, case administration. | Masked-by-default profile lookup, spatial-temporal tracing. | Read-only access to search parameters and query logs. |
| **Relationship Graph**| Modify graph nodes, export full network maps. | Trace multi-degree communication links (masked defaults). | Audit query signatures and link extraction counts. |
| **GIS Spatial Map** | Configure geo-fence boundaries, export trajectory playbacks. | View target routes, analyze cell tower coverage circles. | Inspect target tracking requests against case warrants. |
| **Watchlist Alerts** | Create/delete HVTs, update active alerting hashes. | Receive and resolve real-time geo-fencing triggers. | Audit alert logs and PostgreSQL notification queues. |
| **Audit Trails** | View own workspace audit trail. | View own workspace audit trail. | Search, inspect, and export all analyst query logs. |
