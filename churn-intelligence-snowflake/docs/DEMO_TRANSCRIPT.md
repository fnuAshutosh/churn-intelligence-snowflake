# 🎥 Demo Video Transcript: Real-Time Churn Intelligence on Snowflake

**Title:** Building an End-to-End AI Agent for Churn Prevention using Snowflake Cortex & Snowpark.
**Duration:** ~3-5 Minutes.

---

## 🎬 Scene 1: Introduction (The Hook)
**Visual:** Slide with Project Title or Face Camera.

**Speaker:**
"Hi everyone! In this video, I’m going to show you how I built a **Real-Time Churn Intelligence Platform** completely native to **Snowflake**. 

The problem I’m solving is simple but critical: Detecting high-risk customers in real-time and taking **immediate action** before they churn, using Generative AI."

---

## 🏗️ Scene 2: Architecture (The "How")
**Visual:** Show the Architecture Diagram (ASCII or Mermaid from README).

**Speaker:**
"Most companies build complex stacks with Kafka, Spark, and external API calls for this.
I simplified everything into a **Single Data Platform**:
1.  **Ingestion:** Raw transaction data lands in Snowflake via Kafka (simulated).
2.  **Engineering:** I verify risk using **Dynamic Tables** for declarative ETL—no Airflow needed.
3.  **Intelligence:** 
    *   I trained a **Random Forest Model** using **Snowpark** to predict churn probability.
    *   And here's the cool part: I built an **AI Agent** using **Snowflake Cortex (Llama 3)** to automatically write personalized retention emails."

---

## 🚀 Scene 3: The Live Demo (The "Wow")

### Step 1: The Setup (Snowsight)
**Visual:** Open Snowflake UI (Snowsight). Show the `CHURN_METRICS_LIVE` Dynamic Table.

**Speaker:**
"Let's look at the pipeline. Here is my Dynamic Table `CHURN_METRICS_LIVE`. It automatically aggregates user transactions with a 1-minute lag. It’s the heartbeat of the system."

### Step 2: Injecting Fault (Terminal)
**Visual:** Split screen with Terminal. Run `python scripts/simulate_event.py --user u_DEMO_VIDEO`.

**Speaker:**
"Now, let's simulate a crisis. I'm injecting a user `u_DEMO_VIDEO` who just had 12 transaction declines in 10 minutes. This is a massive churn signal."
*(Run the command)*
"Data is ingested. The Dynamic Table is refreshing..."

### Step 3: The AI Agent in Action (Snowsight Task History)
**Visual:** Go to `Activity` -> `Task History` in Snowflake. Filter by `PROCESS_ALERTS_TASK`.

**Speaker:**
"My automated Task `PROCESS_ALERTS_TASK` detects this change on the Stream. It sees the 'Critical' status and immediately calls the **Cortex AI Agent**."

### Step 4: The Manager Dashboard (Streamlit)
**Visual:** Open the Streamlit App (Tabs). Refresh the page.
**Action:** Select `u_DEMO_VIDEO` from the dropdown.

**Speaker:**
"And here is the **Manager Dashboard**, built entirely in Snowflake Streamlit.
We see `u_DEMO_VIDEO` just popped up with a Risk Score of **0.95**.
And look at this..."
*(Highlight the Email Section)*
"The **AI Agent** has already drafted a personalized email using Llama 3:
*'Dear Customer, we noticed you had recent declines... here is a $50 credit.'*
All I have to do is click **Approve**."

---

## 🏁 Scene 4: Conclusion
**Visual:** Face Camera or GitHub Repo.

**Speaker:**
"So, in just a few minutes, we went from Raw Data -> ML Prediction -> GenAI Action, all within Snowflake's secure boundary.
This architecture reduces latency, cuts infrastructure costs, and improves data governance.

Thanks for watching! Code is in the description."
