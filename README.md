# 🏦 BankCo Churn Intelligence Platform

![Status](https://img.shields.io/badge/Status-Production%20Ready-success)
![Snowflake](https://img.shields.io/badge/Snowflake-Dynamic%20Tables%20%2B%20Cortex-29b6f6)
![Kafka](https://img.shields.io/badge/Kafka-Real--Time%20Streaming-red)
![Python](https://img.shields.io/badge/Python-3.9-yellow)

> A production-grade **Real-Time Customer Churn Prediction & Retention Platform**.
> Ingests live banking events via Kafka, computes churn risk automatically via Snowflake Dynamic Tables,
> and triggers personalized AI retention emails via Snowflake Cortex (llama3-8b).
> **Zero idle compute cost** — event-driven from end to end.

---

## 🏗️ Architecture

![Architecture Diagram](diagrams/architecture.svg)

---

## Diagram A — Entity Relationship (UML)

```mermaid
erDiagram
    DIM_CUSTOMERS {
        VARCHAR CUSTOMER_ID PK
        VARCHAR FULL_NAME
        VARCHAR EMAIL
        VARCHAR SEGMENT
        DATE    JOIN_DATE
        FLOAT   RISK_PROFILE_SCORE
    }
    DIM_ACCOUNTS {
        VARCHAR ACCOUNT_ID PK
        VARCHAR CUSTOMER_ID FK
        VARCHAR PRODUCT_CODE
        FLOAT   AVAILABLE_BALANCE
        VARCHAR ACCOUNT_STATUS
    }
    FACT_TRANSACTION_LEDGER {
        VARCHAR   TRANSACTION_REF PK
        VARCHAR   ACCOUNT_ID FK
        TIMESTAMP POSTING_DATE
        VARCHAR   TRANSACTION_CODE
        FLOAT     AMOUNT
        VARCHAR   CHANNEL_ID
    }
    APP_ACTIVITY_LOGS {
        VARCHAR   LOG_ID PK
        VARCHAR   CUSTOMER_ID FK
        VARCHAR   EVENT_TYPE
        TIMESTAMP EVENT_TIMESTAMP
        VARCHAR   ERROR_CODE
    }
    SUPPORT_CASES {
        VARCHAR   CASE_ID PK
        VARCHAR   CUSTOMER_ID FK
        TIMESTAMP OPEN_TIMESTAMP
        VARCHAR   CATEGORY
        FLOAT     SENTIMENT_SCORE
    }
    DYN_CUSTOMER_FEATURES {
        VARCHAR CUSTOMER_ID PK
        FLOAT   TOTAL_SPEND_30D
        FLOAT   WIRE_OUT_30D
        INT     ERROR_COUNT_30D
        INT     SUPPORT_CASES_30D
        FLOAT   AVG_SENTIMENT
        INT     ACTIVE_DAYS_30D
    }
    DYN_CHURN_PREDICTIONS {
        VARCHAR CUSTOMER_ID PK
        FLOAT   CHURN_SCORE
        VARCHAR RISK_CLASS
        TIMESTAMP COMPUTED_AT
    }
    AGENT_INTERVENTION_LOG {
        VARCHAR   INTERVENTION_ID PK
        VARCHAR   CUSTOMER_ID FK
        FLOAT     CHURN_SCORE
        TEXT      GENERATED_EMAIL
        TIMESTAMP CREATED_AT
    }

    DIM_CUSTOMERS  ||--o{ DIM_ACCOUNTS            : "owns"
    DIM_ACCOUNTS   ||--o{ FACT_TRANSACTION_LEDGER  : "has"
    DIM_CUSTOMERS  ||--o{ APP_ACTIVITY_LOGS        : "generates"
    DIM_CUSTOMERS  ||--o{ SUPPORT_CASES            : "submits"
    DIM_CUSTOMERS  ||--|| DYN_CUSTOMER_FEATURES    : "aggregated into"
    DYN_CUSTOMER_FEATURES ||--|| DYN_CHURN_PREDICTIONS : "scored into"
    DIM_CUSTOMERS  ||--o{ AGENT_INTERVENTION_LOG   : "receives"
```

---

## Diagram B — Sequence (Full Customer Journey)

```mermaid
sequenceDiagram
    autonumber
    participant U  as 👤 Customer
    participant P  as 🟢 producer.py
    participant K  as 📨 Kafka
    participant C  as 🔵 consumer.py
    participant SF as ❄️ Snowflake Raw
    participant DT as 🔄 Dynamic Tables
    participant LM as 🧠 Cortex LLM
    participant UI as 📊 Streamlit (SiS)

    Note over U,P: Event Generation
    U->>P: Makes $5,000 wire transfer
    P->>K: Publish {event_type:TXN, payload:{...}}

    Note over K,SF: Real-Time Ingestion (micro-batch)
    K->>C: Consumer polls message
    C->>C: Buffer 500 msgs OR 5s elapsed
    C->>SF: executemany() → FACT_TRANSACTION_LEDGER

    Note over SF,DT: Automatic Refresh (no polling, no idle cost)
    SF-->>DT: DYN_CUSTOMER_FEATURES detects upstream change
    DT->>DT: Recompute features for affected customers only
    DT-->>DT: DYN_CHURN_PREDICTIONS → score: 0.87 HIGH

    Note over DT,LM: Event-Driven AI (fires only when stream has data)
    DT-->>SF: STREAM_HIGH_RISK_CUSTOMERS gets new row
    SF->>SF: TASK_GENERATE_EMAILS fires
    SF->>LM: CORTEX.COMPLETE(prompt, score=0.87)
    LM-->>SF: Retention email text
    SF->>SF: INSERT → AGENT_INTERVENTION_LOG

    Note over UI: Serving (Streamlit in Snowflake)
    U->>UI: Opens dashboard
    UI->>SF: Query DYN_CHURN_PREDICTIONS
    SF-->>UI: Sarah Chen | 0.87 | HIGH ✉️
```

---

## Diagram C — Event-Driven Dataflow

```mermaid
flowchart TD
    subgraph DOCKER["🐳 Docker Compose"]
        P["🟢 producer.py\nTXN / LOG / USER events\n200 events/min"]
        K["📨 Kafka\nbank_transactions topic"]
        C["🔵 consumer.py\nmicro-batch · flush 500 msgs / 5s"]
        P -->|"JSON events"| K
        K -->|"poll + buffer"| C
    end

    subgraph RAW["❄️ Snowflake — Raw Layer"]
        TXN[("FACT_TRANSACTION_LEDGER")]
        LOG[("APP_ACTIVITY_LOGS")]
        CUS[("DIM_CUSTOMERS")]
        ACC[("DIM_ACCOUNTS")]
        SUP[("SUPPORT_CASES")]
    end

    subgraph DYNAMIC["🔄 Dynamic Tables — Auto-Refresh (no polling)"]
        DCF["DYN_CUSTOMER_FEATURES\nlag: 5 min · 30-day aggregates"]
        DCP["DYN_CHURN_PREDICTIONS\nlag: 5 min · heuristic score 0–1"]
        DCF --> DCP
    end

    subgraph AGENT["🧠 Intelligence Layer"]
        STR["STREAM_HIGH_RISK_CUSTOMERS\nCDC on DYN_CHURN_PREDICTIONS"]
        TSK["TASK_GENERATE_EMAILS\nWHEN stream has data → fires SP"]
        LLM["CORTEX.COMPLETE llama3-8b\n7-day dedup · max 50/run"]
        AIL[("AGENT_INTERVENTION_LOG")]
        STR -->|"has data?"| TSK
        TSK --> LLM
        LLM --> AIL
    end

    subgraph SERVE["📊 Serving Layer"]
        CS["Cortex Search\nCHURN_LOGS_SEARCH\nNL search over error logs"]
        CA["Cortex Analyst\nsemantic_model.yaml\nNL → SQL → results"]
        SIS["Streamlit in Snowflake\ndashboard.py\nZero local infra"]
        CS --> SIS
        CA --> SIS
    end

    C -->|"TXN rows"| TXN
    C -->|"LOG rows"| LOG
    C -->|"USER rows"| CUS

    TXN --> DCF
    LOG --> DCF
    SUP --> DCF
    CUS --> DCF
    ACC --> DCF

    DCP --> STR
    DCP --> CA
    LOG --> CS
    AIL --> CA

    style DOCKER  fill:#1a1a2e,color:#fff
    style RAW     fill:#0d2137,color:#fff
    style DYNAMIC fill:#1a0a2e,color:#fff
    style AGENT   fill:#2a0a0a,color:#fff
    style SERVE   fill:#0a2a0a,color:#fff
```

---

## 🛠️ Tech Stack

| Component | Technology | Role |
|---|---|---|
| **Streaming** | Apache Kafka | Ingests TXN / LOG / USER events in real-time |
| **Ingestion** | Python consumer (micro-batch) | Buffers 500 msgs / 5s → Snowflake |
| **Warehouse** | Snowflake | Central compute + storage |
| **Feature Eng.** | Dynamic Table `DYN_CUSTOMER_FEATURES` | Auto-refreshes 30-day aggregates (lag: 5 min) |
| **Scoring** | Dynamic Table `DYN_CHURN_PREDICTIONS` | Heuristic churn score, zero idle cost |
| **AI Trigger** | Snowflake Stream + Task | CDC — fires only when new HIGH-risk rows appear |
| **GenAI** | Cortex `COMPLETE` (llama3-8b) | Generates personalized retention emails |
| **Search** | Cortex Search Service | NL search over app error logs |
| **Analyst** | Cortex Analyst + semantic model | NL → SQL over churn predictions |
| **Dashboard** | Streamlit in Snowflake | 4-tab UI, zero local infra |
| **Orchestration** | Docker Compose | Kafka + producer + consumer |

---

## ⚡ Quick Start

### 1. Prerequisites
- Docker Desktop running
- Snowflake account with `BANK_WAREHOUSE` warehouse

### 2. Configure credentials
```bash
cp .env.example .env
# Edit .env with your Snowflake credentials
```

### 3. Run the pipeline
```bash
docker-compose up -d --build
```

This single command:
1. Starts Kafka + Zookeeper
2. Runs `setup.py` — drops/creates `CHURN_DEMO`, all tables, dynamic tables, stream, task, stored proc, seeds 1M+ rows
3. Runs `deploy_cortex.py` — creates Cortex Search Service + uploads semantic model
4. Starts `producer.py` — streams 200 events/min
5. Starts `consumer.py` — micro-batches into Snowflake

### 4. Deploy Streamlit Dashboard
In Snowflake UI: **Streamlit → New App → paste `src/app/dashboard.py`**

### 5. Verify
```bash
docker logs churn_setup     # Should end with ✅ SETUP COMPLETE
docker logs kafka_consumer  # Should show ✅ Flushed N rows
```

---

## 📂 Project Structure

```
proejct_proto/
├── .env.example              ← Credentials template
├── docker-compose.yml        ← Full pipeline orchestration
├── Dockerfile                ← Single image for all Python services
├── requirements.txt
├── semantic_model.yaml       ← Cortex Analyst definition
├── diagrams/
│   └── architecture.svg      ← Animated event-driven dataflow
├── scripts/
│   ├── setup.py              ← DB + tables + dynamic tables + proc + task + seed
│   └── deploy_cortex.py      ← Stage + semantic model + Cortex Search
├── streaming/
│   ├── producer.py           ← Kafka event generator (retry loop)
│   └── consumer.py           ← Kafka → Snowflake (micro-batch, retry loop)
└── src/
    ├── core/config.py        ← Snowflake credentials from env vars
    └── app/dashboard.py      ← Streamlit in Snowflake (4 tabs)
```

---

## 🧠 Key Engineering Decisions

| Decision | Rationale |
|---|---|
| **Dynamic Tables over Proc+Task** | Snowflake manages refresh DAG automatically. No idle polling. Pay only for actual compute. |
| **`WHEN STREAM_HAS_DATA`** | LLM task fires only when new HIGH-risk customers appear. Zero credits on idle. |
| **Micro-batching in consumer** | Single `executemany()` per 500 msgs vs. 500 round trips. 100x fewer Snowflake API calls. |
| **7-day LLM dedup** | Same customer never receives two emails within 7 days. Controls Cortex cost. |
| **Streamlit in Snowflake** | Zero local infrastructure. Native Snowpark session. No credentials in app code. |
| **Kafka retry loop** | Producer/consumer wait for broker readiness. Eliminates Docker startup race condition. |

---

*Built for BankCo Technical Interview — Spring 2026*
