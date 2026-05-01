# Industrial Edge Data Lake — Flat Glass Inspection Lines

**The data foundation layer of an Industrial Digital Twin**  
From raw machine logs to a structured, audited, always-on data layer —
ready to feed asset models, predictive analytics, and real-time state synchronization.

---

## What this is

This project is the **data infrastructure backbone** of a Digital Twin system
for flat glass and mirror inspection lines running in a real manufacturing facility in Brazil.

It ingests, deduplicates, and stores inspection events from Eagle Vision machines
(lines SL2, LRA1, and FSP) into a partitioned PostgreSQL database,
orchestrated by Apache Airflow and containerized via Docker.

The physical assets being digitized: automated optical inspection machines
that scan glass sheets for defects at production speed.
Their raw output — timestamped event logs — is the ground truth
that makes a digital representation of those assets possible.

This is not a tutorial or a sandbox.
It runs in production, 24 hours a day, handling real manufacturing data.

---

## Why this architecture exists

Industrial machines don't behave like web APIs.
They crash, lose power, produce duplicate records, overflow counters,
and sometimes stamp three events with the exact same millisecond timestamp.

Every engineering decision in this project exists to handle those realities:

**Deterministic deduplication** — Log files are re-read continuously.
Rather than relying on timestamps (which are non-deterministic at machine speed),
the pipeline generates a deterministic sequential key (`event_seq`)
based on physical read position via `cumcount()`.
PostgreSQL `ON CONFLICT DO NOTHING` makes every insert idempotent —
the pipeline can run infinitely without creating duplicate records.

**Edge case resilience** — The pipeline explicitly handles:
- Simultaneous events (3–4 glass sheets scanned in the same millisecond)
- Sensor read failures and spurious zero-value IDs
- 12-bit PLC counter rollovers at the hardware limit

**Continuous integrity auditing** — A dedicated Airflow DAG runs daily
to mathematically reconcile physical files (`.csv` / `.txt`) against the database.
Any discrepancy triggers an alert. Data loss is caught before it compounds.

**Zero-downtime cold archival** — Every quarter, a Rust binary extracts
historical data from PostgreSQL, compresses it to Parquet (Snappy),
and executes an instantaneous `DROP TABLE` on the closed partitions —
freeing disk space without locking production tables.

---

## Architecture

```
Physical Layer (Shop Floor)
│
├── Eagle Vision SL2       ┐
├── Eagle Vision LRA1      ├── Inspection logs (.txt / .csv)
└── Eagle Vision FSP       ┘
        │
        │  SMBClient (Windows XP legacy) / rsync (Linux modern)
        ▼
Staging Area (local server)
        │
        │  Airflow DAG triggers Rust binary
        ▼
Rust ETL Binary
  ├── Type coercion and business rule validation
  ├── Deterministic event_seq key generation
  └── Upsert into PostgreSQL (ON CONFLICT DO NOTHING)
        │
        ▼
PostgreSQL (partitioned by date)
  ├── Raw layer: one partition per day per line
  ├── Audit DAG: daily reconciliation at 23:30
  └── Quarterly cold archive → Parquet on network share
        │
        ▼  [next layer — in development]
Asset Administration Shell (AAS)
  └── Digital Twin state synchronization via FastAPI + Eclipse Ditto
```

The PostgreSQL layer is designed as the **bronze (raw) layer** of a medallion
data lake architecture. The next layers — silver (cleaned) and gold (aggregated)
— are under active development, feeding directly into the AAS submodel for telemetry.

---

## Tech Stack

| Layer | Tool | Role |
|---|---|---|
| OS & containers | Linux + Docker Compose | Hosting and service isolation |
| Database | PostgreSQL | Partitioned analytical storage |
| Orchestration | Apache Airflow | DAG scheduling and pipeline management |
| ETL processing | Rust + Python | Rust for high-throughput ingestion; Python for DAG logic |
| Monitoring | Prometheus + Grafana | Infrastructure and pipeline observability |
| File transport | SMBClient + rsync | Legacy and modern machine log collection |
| Backup | Crontab + SMBClient | Automated disaster recovery to network share |
| Cold storage | Parquet (Snappy) | Quarterly archival with instant partition drop |

---

## Network and Security

The server operates on a restricted industrial control network (Layer P2),
isolated from the corporate IT network.
Access is controlled through a pre-existing OT/IT gateway.

Firewall (ufw) allows only the minimum required ports:

```
8081/tcp  — Airflow Web UI
5050/tcp  — pgAdmin / PostgreSQL Web
5432/tcp  — Direct database connection (internal network only)
3000/tcp  — Grafana dashboards
22/tcp    — Remote SSH administration
```

---

## Project Structure

```
datalake_local/
├── dags/                    # Airflow DAG definitions (Python)
│   ├── ingestion_sl2.py
│   ├── ingestion_lra1.py
│   ├── ingestion_fsp.py
│   ├── audit_daily.py
│   └── archive_quarterly.py
├── rust_etl/                # Rust ETL binaries
│   ├── src/
│   └── Cargo.toml
├── sql/                     # Schema, partition management, audit queries
├── monitoring/              # Prometheus config + Grafana dashboard JSONs
├── docker-compose.yml
└── README.md
```

---

## How to Run

The system is designed to run 100% autonomously once deployed.

```bash
# Clone and configure environment
git clone https://github.com/VictorJenckel/local_cluster_industrial_data
cd local_cluster_industrial_data
cp .env.example .env  # configure paths, DB credentials, SMB targets

# Build and start all services
docker compose up -d

# Build Rust ETL binaries
cd rust_etl && cargo build --release
```

Management interfaces (accessible from any machine on the industrial network
using the server's static IP):

| Interface | URL |
|---|---|
| Airflow | `http://<server-ip>:8081` |
| PostgreSQL Web | `http://<server-ip>:5050` |
| Grafana | `http://<server-ip>:3000` |

**Requirements:** Linux (Ubuntu 22.04+), Docker + Compose plugin, Rust toolchain, SMBClient

---

## Connection to Digital Twin

This project is Layer 0 of a broader Digital Twin architecture for the inspection lines.

The PostgreSQL database feeds directly into:
- **AAS submodel for Telemetry** — live asset state exposed via REST API (FastAPI, in development)
- **Predictive maintenance models** — time-series anomaly detection on inspection event sequences
- **Glass ribbon drift monitoring** — OpenCV optical flow analysis using frames correlated with inspection events

The long-term goal: a fully synchronized digital representation of each inspection line —
where the physical state of every machine is reflected in real time in a structured AAS,
queryable by any system in the OT/IT stack.

---

## Background

Built and maintained by [Victor Jenckel](https://github.com/VictorJenckel) —
industrial automation engineer with 15+ years on the shop floor,
specializing in OT/IT integration and Industrial Digital Twins.

🇧🇷 Taubaté, SP, Brazil · 🇩🇪 German citizen  
📧 victorjenckel@gmail.com  
💼 [linkedin.com/in/victorjenckel](https://www.linkedin.com/in/victorjenckel)
