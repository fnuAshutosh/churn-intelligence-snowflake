"""
src/app/dashboard.py — Streamlit in Snowflake dashboard.

Deploy via: Snowflake UI → Streamlit → New App → paste this file.
Accesses Snowflake data natively via snowflake.snowpark.context.
"""

import streamlit as st
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
    .metric-card {
        background: linear-gradient(135deg, #1a1a2e, #16213e);
        border: 1px solid #0f3460;
        border-radius: 12px;
        padding: 20px;
        text-align: center;
    }
    .high-risk  { color: #e94560; font-weight: bold; }
    .medium-risk{ color: #f5a623; font-weight: bold; }
    .low-risk   { color: #4caf50; font-weight: bold; }
    .stDataFrame { font-size: 13px; }
</style>
""", unsafe_allow_html=True)

st.title("🏦 BankCo Churn Intelligence Platform")
st.caption("Real-time customer churn risk powered by Snowflake Dynamic Tables + Cortex AI")

# ── Tabs ──────────────────────────────────────────────────────────────────────
tab1, tab2, tab3, tab4 = st.tabs(["📊 Overview", "🔴 High Risk", "📡 Live Feed", "✉️ AI Interventions"])

# ── Tab 1: Overview ───────────────────────────────────────────────────────────
with tab1:
    st.subheader("Risk Summary")

    summary = session.sql("""
        SELECT
            COUNT(*)                                                AS total_customers,
            SUM(CASE WHEN risk_class = 'HIGH'   THEN 1 ELSE 0 END) AS high_risk,
            SUM(CASE WHEN risk_class = 'MEDIUM' THEN 1 ELSE 0 END) AS medium_risk,
            SUM(CASE WHEN risk_class = 'LOW'    THEN 1 ELSE 0 END) AS low_risk,
            ROUND(AVG(churn_score), 3)                              AS avg_score
        FROM DYN_CHURN_PREDICTIONS
    """).to_pandas()

    c1, c2, c3, c4, c5 = st.columns(5)
    c1.metric("Total Customers",  f"{summary['TOTAL_CUSTOMERS'][0]:,}")
    c2.metric("🔴 High Risk",     f"{summary['HIGH_RISK'][0]:,}")
    c3.metric("🟡 Medium Risk",   f"{summary['MEDIUM_RISK'][0]:,}")
    c4.metric("🟢 Low Risk",      f"{summary['LOW_RISK'][0]:,}")
    c5.metric("Avg Churn Score",  f"{summary['AVG_SCORE'][0]:.3f}")

    st.divider()
    st.subheader("Churn Score by Segment")
    by_seg = session.sql("""
        SELECT segment, ROUND(AVG(churn_score),3) AS avg_score, COUNT(*) AS customers
        FROM DYN_CHURN_PREDICTIONS
        GROUP BY segment ORDER BY avg_score DESC
    """).to_pandas()
    st.bar_chart(by_seg.set_index("SEGMENT")["AVG_SCORE"])

    emails_today = session.sql("""
        SELECT COUNT(*) AS n FROM AGENT_INTERVENTION_LOG
        WHERE CREATED_AT >= DATEADD('day', -1, CURRENT_TIMESTAMP())
    """).to_pandas()["N"][0]
    st.metric("✉️ Retention Emails Sent (24h)", emails_today)

# ── Tab 2: High Risk Customers ────────────────────────────────────────────────
with tab2:
    st.subheader("🔴 High Risk Customers")
    limit = st.slider("Show top N customers", 10, 500, 100)

    df = session.sql(f"""
        SELECT
            p.CUSTOMER_ID,
            p.FULL_NAME,
            p.SEGMENT,
            p.EMAIL,
            ROUND(p.CHURN_SCORE, 3)  AS CHURN_SCORE,
            p.RISK_CLASS,
            p.COMPUTED_AT,
            CASE WHEN a.CUSTOMER_ID IS NOT NULL THEN '✉️ Yes' ELSE '—' END AS EMAIL_SENT
        FROM DYN_CHURN_PREDICTIONS p
        LEFT JOIN (
            SELECT DISTINCT CUSTOMER_ID FROM AGENT_INTERVENTION_LOG
            WHERE CREATED_AT > DATEADD('day', -7, CURRENT_TIMESTAMP())
        ) a ON p.CUSTOMER_ID = a.CUSTOMER_ID
        WHERE p.RISK_CLASS = 'HIGH'
        ORDER BY p.CHURN_SCORE DESC
        LIMIT {limit}
    """).to_pandas()

    st.dataframe(df, use_container_width=True)

# ── Tab 3: Live Transaction Feed ──────────────────────────────────────────────
with tab3:
    st.subheader("📡 Live Transaction Feed (last 50)")
    if st.button("🔄 Refresh"):
        st.rerun()

    live = session.sql("""
        SELECT
            t.TRANSACTION_REF,
            t.ACCOUNT_ID,
            t.TRANSACTION_CODE,
            t.AMOUNT,
            t.CHANNEL_ID,
            t.POSTING_DATE
        FROM FACT_TRANSACTION_LEDGER t
        ORDER BY t.POSTING_DATE DESC
        LIMIT 50
    """).to_pandas()
    st.dataframe(live, use_container_width=True)

    st.divider()
    st.subheader("App Error Log (last 50 errors)")
    errors = session.sql("""
        SELECT CUSTOMER_ID, EVENT_TYPE, ERROR_CODE, DEVICE_OS, EVENT_TIMESTAMP
        FROM APP_ACTIVITY_LOGS
        WHERE ERROR_CODE IS NOT NULL
        ORDER BY EVENT_TIMESTAMP DESC
        LIMIT 50
    """).to_pandas()
    st.dataframe(errors, use_container_width=True)

# ── Tab 4: AI Interventions ───────────────────────────────────────────────────
with tab4:
    st.subheader("✉️ AI-Generated Retention Emails")

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

    for _, row in interventions.iterrows():
        with st.expander(f"✉️ {row['FULL_NAME']} ({row['SEGMENT']}) — Score: {row['CHURN_SCORE']} — {row['CREATED_AT']}"):
            st.write(row["GENERATED_EMAIL"])
