"""
src/app/dashboard.py — Streamlit in Snowflake dashboard.

Deploy via: Snowflake UI → Streamlit → New App → paste this file.
"""

import streamlit as st
import json
import pandas as pd
from snowflake.snowpark.context import get_active_session

# NOTE: Removed 'snowflake.cortex' import to avoid ModuleNotFoundError.
# We call the SQL function SNOWFLAKE.CORTEX.COMPLETE directly via session.sql().

st.set_page_config(
    page_title="BankCo Churn Intelligence",
    page_icon="🏦",
    layout="wide",
)

session = get_active_session()

# Helper for Cortex calls via SQL
def run_cortex_complete(model, prompt):
    try:
        # Escape single quotes for SQL string literal
        safe_prompt = prompt.replace("'", "''")
        query = f"SELECT SNOWFLAKE.CORTEX.COMPLETE('{model}', '{safe_prompt}')"
        result = session.sql(query).collect()[0][0]
        return result
    except Exception as e:
        return f"Error calling Cortex: {e}"

# ── Styling ───────────────────────────────────────────────────────────────────
st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&display=swap');
  html, body, [class*="css"] { font-family: 'Inter', sans-serif; }
  .stApp { background: #0d1117; color: white; }
  .metric-card {
    background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
    border: 1px solid #0f3460;
    border-radius: 12px;
    padding: 20px;
    text-align: center;
  }
  .chat-box { background: #16213e; border-radius: 8px; padding: 20px; margin-bottom: 20px; height: 500px; overflow-y: scroll; }
  .chat-user { 
      background: #1a237e; 
      color: white;
      border-radius: 12px 12px 2px 12px; 
      padding: 10px 14px; 
      margin: 8px 0 8px auto; 
      width: fit-content;
      max-width: 80%;
      text-align: right;
  }
  .chat-bot  { 
      background: #1b5e20; 
      color: white;
      border-radius: 12px 12px 12px 2px; 
      padding: 10px 14px; 
      margin: 8px auto 8px 0; 
      width: fit-content;
      max-width: 80%;
      text-align: left;
  }
</style>
""", unsafe_allow_html=True)

st.markdown("## 🏦 BankCo Churn Intelligence Platform")
st.caption("Real-time churn risk · Snowflake Dynamic Tables · Cortex AI · Kafka")

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📊 Overview", "🔴 High Risk", "📡 Live Feed", "✉️ AI Emails", "🔍 Analyst (SQL)", "🤖 Chat Agent"
])

# ── 1. Overview ───────────────────────────────────────────────────────────────
with tab1:
    st.subheader("Risk Summary")
    summary = session.sql("""
        SELECT
            COUNT(*) AS TOTAL,
            SUM(CASE WHEN RISK_CLASS = 'HIGH' THEN 1 ELSE 0 END) AS HIGH,
            SUM(CASE WHEN RISK_CLASS = 'MEDIUM' THEN 1 ELSE 0 END) AS MEDIUM,
            SUM(CASE WHEN RISK_CLASS = 'LOW' THEN 1 ELSE 0 END) AS LOW,
            ROUND(AVG(CHURN_SCORE), 3) AS AVG_SCORE
        FROM DYN_CHURN_PREDICTIONS
    """).to_pandas()

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Customers", f"{summary['TOTAL'][0]:,}")
    c2.metric("🔴 High Risk",     f"{summary['HIGH'][0]:,}")
    c3.metric("🟡 Medium Risk",   f"{summary['MEDIUM'][0]:,}")
    c4.metric("🟢 Low Risk",      f"{summary['LOW'][0]:,}")
    c5.metric("Avg Score",        f"{summary['AVG_SCORE'][0]}")

    st.divider()
    c_a, c_b = st.columns(2)
    with c_a:
        st.subheader("By Segment")
        df_seg = session.sql("SELECT SEGMENT, AVG(CHURN_SCORE) as SCORE FROM DYN_CHURN_PREDICTIONS GROUP BY 1").to_pandas()
        st.bar_chart(df_seg.set_index("SEGMENT"))
    with c_b:
        st.subheader("Risk Distribution")
        df_risk = session.sql("SELECT RISK_CLASS, COUNT(*) as N FROM DYN_CHURN_PREDICTIONS GROUP BY 1").to_pandas()
        st.bar_chart(df_risk.set_index("RISK_CLASS"))

# ── 2. High Risk ──────────────────────────────────────────────────────────────
with tab2:
    st.subheader("🔴 High Risk Customers")
    df_risk = session.sql("""
        SELECT FULL_NAME, SEGMENT, CHURN_SCORE, RISK_CLASS, COMPUTED_AT
        FROM DYN_CHURN_PREDICTIONS WHERE RISK_CLASS = 'HIGH' ORDER BY CHURN_SCORE DESC LIMIT 100
    """).to_pandas()
    st.dataframe(df_risk, use_container_width=True)

# ── 3. Live Feed ──────────────────────────────────────────────────────────────
with tab3:
    col1, col2 = st.columns(2)
    with col1:
        st.subheader("💳 Transactions")
        st.dataframe(session.sql("SELECT * FROM FACT_TRANSACTION_LEDGER ORDER BY POSTING_DATE DESC LIMIT 20").to_pandas())
    with col2:
        st.subheader("⚠️ App Errors")
        st.dataframe(session.sql("SELECT * FROM APP_ACTIVITY_LOGS WHERE ERROR_CODE IS NOT NULL ORDER BY EVENT_TIMESTAMP DESC LIMIT 20").to_pandas())

# ── 4. AI Emails ──────────────────────────────────────────────────────────────
with tab4:
    st.subheader("✉️ Retention Emails")
    emails = session.sql("SELECT * FROM AGENT_INTERVENTION_LOG ORDER BY CREATED_AT DESC LIMIT 50").to_pandas()
    for _, row in emails.iterrows():
        with st.expander(f"Email for {row['CUSTOMER_ID']} (Score: {row['CHURN_SCORE']})"):
            st.write(row["GENERATED_EMAIL"])

# ── 5. Cortex Analyst (Simulated) ─────────────────────────────────────────────
with tab5:
    st.subheader("🔍 Cortex Analyst (Text-to-SQL)")
    st.caption("Using `llama3-70b` via SQL directly to generate queries from natural language.")

    question = st.text_input("Ask a question about churn data:", "Show me top 5 high risk customers by churn score")
    
    if st.button("Run Analysis", key="btn_analyst"):
        schema_context = """
        Table: ANALYST_CHURN_VIEW
        Columns:
        - FULL_NAME (text)
        - SEGMENT (text: Young Professional, Student, Established, High Net Worth)
        - CHURN_SCORE (number 0-1)
        - RISK_CLASS (text: HIGH, MEDIUM, LOW)
        - WIRE_OUT_30D (number)
        - AVG_SENTIMENT (number 0-1)
        - TOTAL_SPEND_30D (number)
        """
        
        prompt = f"""
        You are a Snowflake SQL Expert. 
        Given tables:
        {schema_context}
        
        Generate a valid Snowflake SQL query for: "{question}"
        Return ONLY the SQL. No markdown, no explanations.
        """
        
        with st.spinner("Generating SQL..."):
            sql_resp = run_cortex_complete("llama3-70b", prompt).replace("```sql", "").replace("```", "").strip()
            st.code(sql_resp, language="sql")
            
            try:
                df_res = session.sql(sql_resp).to_pandas()
                st.dataframe(df_res)
            except Exception as e:
                st.error(f"SQL Error: {e}")

# ── 6. Chat Agent (Simulated) ─────────────────────────────────────────────────
with tab6:
    st.subheader("🤖 Cortex Chat Agent")
    st.caption("Combines Search + SQL using `llama3-70b` routing.")

    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Display Chat History using Custom CSS (no st.chat_message)
    for msg in st.session_state.messages:
        role_class = "chat-user" if msg["role"] == "user" else "chat-bot"
        icon = "👤" if msg["role"] == "user" else "🤖"
        st.markdown(f'<div class="{role_class}">{icon} {msg["content"]}</div>', unsafe_allow_html=True)

    st.divider()

    # Input Form (Safe for all Streamlit versions)
    with st.form("chat_form", clear_on_submit=True):
        col1, col2 = st.columns([6, 1])
        with col1:
            user_q = st.text_input("Message:", key="user_q_input")
        with col2:
            submitted = st.form_submit_button("Send")
    
    if submitted and user_q:
        # Add User Message
        st.session_state.messages.append({"role": "user", "content": user_q})
        st.markdown(f'<div class="chat-user">👤 {user_q}</div>', unsafe_allow_html=True)

        with st.spinner("Thinking..."):
            # 1. Decide intent
            intent_prompt = f"""
            Classify intent: SQL (data analysis) or SEARCH (logs/errors).
            Question: {user_q}
            Return only 'SQL' or 'SEARCH'.
            """
            intent = run_cortex_complete("llama3-70b", intent_prompt).replace("'", "").strip()
            
            reply = ""
            if "SEARCH" in intent:
                # search logs
                search_q = f"SELECT SNOWFLAKE.CORTEX.SEARCH_PREVIEW('CHURN_LOGS_SEARCH', '{user_q}', 5)"
                try:
                    res = session.sql(search_q).collect()
                    reply = f"**Found related logs:**\n\n```json\n{res}\n```"
                except:
                    reply = "Search service unavailble or error."
            else:
                # generate SQL (same as Tab 5)
                schema_context = "Table: ANALYST_CHURN_VIEW (cols: FULL_NAME, SEGMENT, CHURN_SCORE, RISK_CLASS, WIRE_OUT_30D)"
                sql_prompt = f"Generate SQL query for table ANALYST_CHURN_VIEW to answer: {user_q}. Return ONLY SQL. LIMIT 10."
                sql = run_cortex_complete("llama3-70b", sql_prompt).replace("```sql","").replace("```","").strip()
                try:
                    df = session.sql(sql).to_pandas()
                    reply = f"**Here is the data:**\n\n" + df.to_markdown()
                except Exception as e:
                    reply = f"I tried to run SQL but failed: {e}\n\nQuery was: `{sql}`"

            # Add Assistant Message
            st.session_state.messages.append({"role": "assistant", "content": reply})
            st.markdown(f'<div class="chat-bot">🤖 {reply}</div>', unsafe_allow_html=True)
            
            # Force refresh to show history properly
            st.rerun()

    if st.button("Start New Chat"):
        st.session_state.messages = []
        st.rerun()
