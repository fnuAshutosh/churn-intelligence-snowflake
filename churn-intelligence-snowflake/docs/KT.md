# 🧠 Knowledge Transfer (KT): Project Walkthrough

This document is your **Guide to the Codebase**. Start here if you want to understand how every piece fits together.

---

## 🗺️ 1. Project Roadmap: How to Read the Code
Follow this order to understand the flow from Data -> Intelligence -> Action.

1.  **Start with Infrastructure** (`src/snowflake_ops/pipeline.py`)
2.  **Understand the Brain** (`src/ai/agent.py` & `src/ai/ml_model.py`)
3.  **Explore the App** (`src/app/dashboard.py`)
4.  **See Execution** (`scripts/deploy_all.py` & `scripts/simulate_event.py`)

---

## 📂 2. File-by-File Explanation

### A. Infrastructure (`src/snowflake_ops/`)
This layer defines the "skeleton" of the data platform using Snowflake objects.

*   **`pipeline.py`**:
    *   **What it does:** Creates the Tables, Dynamic Tables, Streams, and Tasks.
    *   **Key Concept:** Instead of Airflow, we use `DYNAMIC TABLE`s for declarative ETL. The `PROCESS_ALERTS_TASK` is the "trigger" that wakes up every minute to check for new data.

### B. Intelligence (`src/ai/`)
This is where the Data Science and GenAI magic happens.

*   **`agent.py`**:
    *   **What it does:** Defines the `GENERATE_RETENTION_EMAIL` function.
    *   **Key Concept:** It wraps `SNOWFLAKE.CORTEX.COMPLETE` with a specific prompt template (Llama 3). It runs *inside* the database engine.
*   **`ml_model.py`**:
    *   **What it does:** Defines the Machine Learning training and inference logic.
    *   **Key Concept:** It creates a Python Stored Procedure (`TRAIN_CHURN_MODEL`) that trains a Scikit-Learn Random Forest model on Snowflake compute, saves it to a Stage, and exposes a UDF (`PREDICT_CHURN`) for scoring.

### C. Application (`src/app/`)
The user interface for business stakeholders.

*   **`dashboard.py`**:
    *   **What it does:** A Streamlit application running natively in Snowflake (SiS).
    *   **Key Concept:** It connects securely to the Snowflake session (`get_active_session()`), visualizes live metrics, and lets managers review/approve the AI-generated emails.

### D. Core & Configs (`src/core/` & `configs/`)
Standard engineering practices.

*   **`settings.toml`**: The single source of truth for configuration (DB names, model params).
*   **`config.py`**: A loader utility that merges the TOML file with Environment Variables (for security).

### E. Scripts (`scripts/`)
Tools to run and test the system.

*   **`deploy_all.py`**: The "One-Click Deploy" script. It calls the setup functions from `src` in the correct order.
*   **`simulate_event.py`**: A testing utility. It injects a fake "crisis" event (High Declines) for a user to trigger the entire pipeline for demos.

---

## 🎓 3. Key Concepts to Master

| Concept | File Location | Why it matters |
| :--- | :--- | :--- |
| **Declarative ETL** | `src/snowflake_ops/pipeline.py` | Replaces complex orchestration tools; Snowflake manages dependencies automatically. |
| **Serverless AI** | `src/ai/agent.py` | Access LLMs (Llama 3) via SQL without managing GPU infrastructure. |
| **In-Database ML** | `src/ai/ml_model.py` | Training models where the data lives improves security and data governance. |
| **Native Apps** | `src/app/dashboard.py` | Build internal tools that run securely inside your data warehouse boundary. |

---

## 🚀 4. How to Experiment
1.  **Read `src/ai/agent.py`:** Try changing the Prompt Template to be more aggressive or empathetic.
2.  **Read `src/snowflake_ops/pipeline.py`:** Try changing the `TARGET_LAG` of the Dynamic Table to 5 minutes.
3.  **Run `scripts/simulate_event.py`:** See how your changes affect the output in real-time.

Happy Coding!
