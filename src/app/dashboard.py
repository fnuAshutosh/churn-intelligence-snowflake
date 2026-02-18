"""
src/app/dashboard.py — Streamlit in Snowflake dashboard.

Deploy via: Snowflake UI → Streamlit → New App → paste this file.
Or via: CREATE STREAMLIT in deploy_cortex.py

Tabs:
  1. Overview        — KPIs + segment chart
  2. High Risk       — sortable customer table
  3. Live Feed       — real-time transactions + errors
  4. AI Emails       — retention email viewer
  5. Cortex Analyst  — natural language → SQL via semantic model
  6. Cortex Agent    — conversational AI (Search + Analyst)
"""

import streamlit as st
import json
from snowflake.snowpark.context import get_active_session

st.set_page_config(
    page_title="BankCo Churn Intelligence",
    page_icon="🏦",
    layout="wide",
)

session = get_active_session()

# ── Styling ───────────────────────────────────────────────────────────────────
st.markdown("""
<style>
  @import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;600;700&display=swap');
  html, body, [class*="css"] { font-family: 'Inter', sans-serif; }

  .stApp { background: #0d1117; }

  .metric-card {
    background: linear-gradient(135deg, #1a1a2e 0%, #16213e 100%);
    border: 1px solid #0f3460;
    border-radius: 12px;
    padding: 20px;
    text-align: center;
  }
  .risk-high   { color: #ef5350; font-weight: 700; }
  .risk-medium { color: #ffa726; font-weight: 700; }
  .risk-low    { color: #66bb6a; font-weight: 700; }

  .chat-user { background: #1a237e; border-radius: 12px 12px 2px 12px; padding: 10px 14px; margin: 6px 0; }
  .chat-bot  { background: #1b5e20; border-radius: 12px 12px 12px 2px; padding: 10px 14px; margin: 6px 0; }

  div[data-testid="stDataFrame"] { border-radius: 8px; overflow: hidden; }
</style>
""", unsafe_allow_html=True)

# ── Header ────────────────────────────────────────────────────────────────────
st.markdown("## 🏦 BankCo Churn Intelligence Platform")
st.caption("Real-time churn risk · Snowflake Dynamic Tables · Cortex AI · Kafka Streaming")

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "📊 Overview",
    "🔴 High Risk",
    "📡 Live Feed",
    "✉️ AI Emails",
    "🔍 Cortex Analyst",
    "🤖 Cortex Agent",
])


# ════════════════════════════════════════════════════════════════════════════
# TAB 1 — Overview
# ════════════════════════════════════════════════════════════════════════════
with tab1:
    st.subheader("Risk Summary")

    summary = session.sql("""
        SELECT
            COUNT(*)                                                AS TOTAL_CUSTOMERS,
            SUM(CASE WHEN RISK_CLASS = 'HIGH'   THEN 1 ELSE 0 END) AS HIGH_RISK,
            SUM(CASE WHEN RISK_CLASS = 'MEDIUM' THEN 1 ELSE 0 END) AS MEDIUM_RISK,
            SUM(CASE WHEN RISK_CLASS = 'LOW'    THEN 1 ELSE 0 END) AS LOW_RISK,
            ROUND(AVG(CHURN_SCORE), 3)                              AS AVG_SCORE
        FROM DYN_CHURN_PREDICTIONS
    """).to_pandas()

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("👥 Total Customers",  f"{summary['TOTAL_CUSTOMERS'][0]:,}")
    c2.metric("🔴 High Risk",        f"{summary['HIGH_RISK'][0]:,}")
    c3.metric("🟡 Medium Risk",      f"{summary['MEDIUM_RISK'][0]:,}")
    c4.metric("🟢 Low Risk",         f"{summary['LOW_RISK'][0]:,}")
    c5.metric("📈 Avg Churn Score",  f"{summary['AVG_SCORE'][0]:.3f}")

    st.divider()

    col_a, col_b = st.columns(2)

    with col_a:
        st.subheader("Avg Churn Score by Segment")
        by_seg = session.sql("""
            SELECT SEGMENT,
                   ROUND(AVG(CHURN_SCORE), 3) AS AVG_SCORE,
                   COUNT(*) AS CUSTOMERS
            FROM DYN_CHURN_PREDICTIONS
            GROUP BY SEGMENT ORDER BY AVG_SCORE DESC
        """).to_pandas()
        st.bar_chart(by_seg.set_index("SEGMENT")["AVG_SCORE"])

    with col_b:
        st.subheader("Risk Class Distribution")
        dist = session.sql("""
            SELECT RISK_CLASS, COUNT(*) AS N
            FROM DYN_CHURN_PREDICTIONS
            GROUP BY RISK_CLASS
        """).to_pandas()
        st.bar_chart(dist.set_index("RISK_CLASS")["N"])

    st.divider()
    col_c, col_d, col_e = st.columns(3)

    emails_24h = session.sql("""
        SELECT COUNT(*) AS N FROM AGENT_INTERVENTION_LOG
        WHERE CREATED_AT >= DATEADD('day', -1, CURRENT_TIMESTAMP())
    """).to_pandas()["N"][0]
    col_c.metric("✉️ Emails Sent (24h)", emails_24h)

    txn_24h = session.sql("""
        SELECT COUNT(*) AS N FROM FACT_TRANSACTION_LEDGER
        WHERE POSTING_DATE >= DATEADD('day', -1, CURRENT_TIMESTAMP())
    """).to_pandas()["N"][0]
    col_d.metric("💳 Transactions (24h)", f"{txn_24h:,}")

    errors_24h = session.sql("""
        SELECT COUNT(*) AS N FROM APP_ACTIVITY_LOGS
        WHERE ERROR_CODE IS NOT NULL
          AND EVENT_TIMESTAMP >= DATEADD('day', -1, CURRENT_TIMESTAMP())
    """).to_pandas()["N"][0]
    col_e.metric("⚠️ App Errors (24h)", f"{errors_24h:,}")


# ════════════════════════════════════════════════════════════════════════════
# TAB 2 — High Risk Customers
# ════════════════════════════════════════════════════════════════════════════
with tab2:
    st.subheader("🔴 High Risk Customers")

    col1, col2 = st.columns([3, 1])
    with col1:
        search_name = st.text_input("🔎 Filter by name", placeholder="e.g. Sarah")
    with col2:
        limit = st.slider("Show top N", 10, 500, 100)

    name_filter = f"AND p.FULL_NAME ILIKE '%{search_name}%'" if search_name else ""

    df = session.sql(f"""
        SELECT
            p.CUSTOMER_ID,
            p.FULL_NAME,
            p.SEGMENT,
            p.EMAIL,
            ROUND(p.CHURN_SCORE, 3)  AS CHURN_SCORE,
            p.RISK_CLASS,
            f.WIRE_OUT_30D,
            f.SUPPORT_CASES_30D,
            ROUND(f.AVG_SENTIMENT, 2) AS AVG_SENTIMENT,
            f.ACTIVE_DAYS_30D,
            CASE WHEN a.CUSTOMER_ID IS NOT NULL THEN '✉️ Yes' ELSE '—' END AS EMAIL_SENT
        FROM DYN_CHURN_PREDICTIONS p
        JOIN DYN_CUSTOMER_FEATURES f ON p.CUSTOMER_ID = f.CUSTOMER_ID
        LEFT JOIN (
            SELECT DISTINCT CUSTOMER_ID FROM AGENT_INTERVENTION_LOG
            WHERE CREATED_AT > DATEADD('day', -7, CURRENT_TIMESTAMP())
        ) a ON p.CUSTOMER_ID = a.CUSTOMER_ID
        WHERE p.RISK_CLASS = 'HIGH'
        {name_filter}
        ORDER BY p.CHURN_SCORE DESC
        LIMIT {limit}
    """).to_pandas()

    st.dataframe(df, use_container_width=True, height=500)
    st.caption(f"Showing {len(df)} high-risk customers")


# ════════════════════════════════════════════════════════════════════════════
# TAB 3 — Live Feed
# ════════════════════════════════════════════════════════════════════════════
with tab3:
    if st.button("🔄 Refresh Feed"):
        st.rerun()

    col1, col2 = st.columns(2)

    with col1:
        st.subheader("💳 Latest Transactions")
        live = session.sql("""
            SELECT TRANSACTION_REF, ACCOUNT_ID, TRANSACTION_CODE,
                   AMOUNT, CHANNEL_ID, POSTING_DATE
            FROM FACT_TRANSACTION_LEDGER
            ORDER BY POSTING_DATE DESC LIMIT 50
        """).to_pandas()
        st.dataframe(live, use_container_width=True, height=400)

    with col2:
        st.subheader("⚠️ Latest App Errors")
        errors = session.sql("""
            SELECT CUSTOMER_ID, EVENT_TYPE, ERROR_CODE, DEVICE_OS, EVENT_TIMESTAMP
            FROM APP_ACTIVITY_LOGS
            WHERE ERROR_CODE IS NOT NULL
            ORDER BY EVENT_TIMESTAMP DESC LIMIT 50
        """).to_pandas()
        st.dataframe(errors, use_container_width=True, height=400)


# ════════════════════════════════════════════════════════════════════════════
# TAB 4 — AI Retention Emails
# ════════════════════════════════════════════════════════════════════════════
with tab4:
    st.subheader("✉️ AI-Generated Retention Emails")
    st.caption("Generated by Cortex COMPLETE (llama3-8b) · 7-day dedup · max 50/run")

    interventions = session.sql("""
        SELECT
            a.INTERVENTION_ID,
            a.CUSTOMER_ID,
            c.FULL_NAME,
            c.SEGMENT,
            ROUND(a.CHURN_SCORE, 3) AS CHURN_SCORE,
            a.CREATED_AT,
            a.GENERATED_EMAIL
        FROM AGENT_INTERVENTION_LOG a
        JOIN DIM_CUSTOMERS c ON a.CUSTOMER_ID = c.CUSTOMER_ID
        ORDER BY a.CREATED_AT DESC
        LIMIT 100
    """).to_pandas()

    if interventions.empty:
        st.info("No emails generated yet. The pipeline will generate emails once high-risk customers are detected (check back in ~5 minutes).")
    else:
        st.metric("Total emails generated", len(interventions))
        for _, row in interventions.iterrows():
            with st.expander(
                f"✉️ **{row['FULL_NAME']}** ({row['SEGMENT']}) — "
                f"Score: **{row['CHURN_SCORE']}** — {str(row['CREATED_AT'])[:16]}"
            ):
                st.write(row["GENERATED_EMAIL"])


# ════════════════════════════════════════════════════════════════════════════
# TAB 5 — Cortex Analyst (NL → SQL)
# ════════════════════════════════════════════════════════════════════════════
with tab5:
    st.subheader("🔍 Cortex Analyst — Natural Language Queries")
    st.caption("Ask questions in plain English. Cortex Analyst converts them to SQL using the semantic model.")

    # Suggested questions
    suggestions = [
        "Show me all high risk customers with their churn scores",
        "What is the average churn score by customer segment?",
        "Which customers have the highest wire transfer outflows?",
        "How many retention emails were sent today?",
        "Show customers with low sentiment and high churn risk",
    ]

    selected = st.selectbox("💡 Try a suggested question:", [""] + suggestions)
    analyst_q = st.text_input("Or type your own question:", value=selected)

    if analyst_q and st.button("🔍 Run Analysis", key="analyst_btn"):
        with st.spinner("Cortex Analyst is thinking..."):
            try:
                resp = session.sql(f"""
                    SELECT SNOWFLAKE.CORTEX.ANALYST(
                        '{analyst_q.replace("'", "''")}',
                        '@AGENT_ASSETS/semantic_model.yaml'
                    ) AS RESULT
                """).collect()

                result = json.loads(resp[0]["RESULT"])

                if "sql" in result:
                    st.code(result["sql"], language="sql")
                    df_result = session.sql(result["sql"]).to_pandas()
                    st.dataframe(df_result, use_container_width=True)
                    st.caption(f"Returned {len(df_result)} rows")
                elif "message" in result:
                    st.info(result["message"])
                else:
                    st.json(result)

            except Exception as e:
                st.error(f"Analyst error: {e}")
                st.info("Tip: Cortex Analyst requires SNOWFLAKE.CORTEX_USER role privilege.")


# ════════════════════════════════════════════════════════════════════════════
# TAB 6 — Cortex Agent (Conversational AI)
# ════════════════════════════════════════════════════════════════════════════
with tab6:
    st.subheader("🤖 Cortex Agent — Conversational AI")
    st.caption("Combines Cortex Analyst (SQL) + Cortex Search (log search) in one chat interface.")

    # Init chat history
    if "messages" not in st.session_state:
        st.session_state.messages = []

    # Display history
    for msg in st.session_state.messages:
        role_class = "chat-user" if msg["role"] == "user" else "chat-bot"
        icon = "👤" if msg["role"] == "user" else "🤖"
        st.markdown(
            f'<div class="{role_class}">{icon} {msg["content"]}</div>',
            unsafe_allow_html=True,
        )

    # Input
    user_input = st.chat_input("Ask about churn risk, customers, transactions, or errors...")

    if user_input:
        st.session_state.messages.append({"role": "user", "content": user_input})

        with st.spinner("Agent thinking..."):
            try:
                # Build conversation history for the agent
                history = [
                    {"role": m["role"], "content": m["content"]}
                    for m in st.session_state.messages[:-1]
                ]

                resp = session.sql(f"""
                    SELECT SNOWFLAKE.CORTEX.AGENT(
                        'CHURN_INTELLIGENCE_AGENT',
                        PARSE_JSON('{json.dumps(history).replace("'", "''")}'),
                        '{user_input.replace("'", "''")}'
                    ) AS RESPONSE
                """).collect()

                agent_reply = resp[0]["RESPONSE"]

                # Try to parse structured response
                try:
                    parsed = json.loads(agent_reply)
                    reply_text = parsed.get("message", agent_reply)

                    # If agent returned SQL results, show them
                    if "sql" in parsed:
                        st.code(parsed["sql"], language="sql")
                        df_r = session.sql(parsed["sql"]).to_pandas()
                        st.dataframe(df_r, use_container_width=True)

                    # If agent returned search results
                    if "results" in parsed:
                        for r in parsed["results"][:5]:
                            st.json(r)

                except (json.JSONDecodeError, TypeError):
                    reply_text = agent_reply

                st.session_state.messages.append({"role": "assistant", "content": reply_text})
                st.rerun()

            except Exception as e:
                err_msg = str(e)
                if "CHURN_INTELLIGENCE_AGENT" in err_msg or "does not exist" in err_msg:
                    # Fallback: use Analyst directly if Agent not available
                    try:
                        resp = session.sql(f"""
                            SELECT SNOWFLAKE.CORTEX.ANALYST(
                                '{user_input.replace("'", "''")}',
                                '@AGENT_ASSETS/semantic_model.yaml'
                            ) AS RESULT
                        """).collect()
                        result = json.loads(resp[0]["RESULT"])
                        if "sql" in result:
                            st.code(result["sql"], language="sql")
                            df_r = session.sql(result["sql"]).to_pandas()
                            st.dataframe(df_r, use_container_width=True)
                            reply_text = f"_(Cortex Agent unavailable — using Analyst directly)_\n\nFound {len(df_r)} results."
                        else:
                            reply_text = result.get("message", str(result))
                    except Exception as e2:
                        reply_text = f"Error: {e2}"
                else:
                    reply_text = f"Agent error: {err_msg}"

                st.session_state.messages.append({"role": "assistant", "content": reply_text})
                st.rerun()

    if st.button("🗑️ Clear Chat"):
        st.session_state.messages = []
        st.rerun()
