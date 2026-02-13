import streamlit as st
import snowflake.snowpark.context
import pandas as pd
import os

# -----------------------------------------------------------------------------
# Configuration & Layout
# -----------------------------------------------------------------------------
st.set_page_config(
    page_title="Churn Intelligence Platform",
    layout="wide",
    initial_sidebar_state="expanded"
)

st.title("Churn Intelligence Dashboard")
st.markdown("Real-time monitoring of High-Risk Users and GenAI Interventions.")

# -----------------------------------------------------------------------------
# Snowflake Connection (SiS / Local Hybrid)
# -----------------------------------------------------------------------------
if "conn" not in st.session_state:
    try:
        # 1. Try Native Snowpark Session (SiS)
        session = snowflake.snowpark.context.get_active_session()
        st.session_state.conn = session
        st.success("Connected via Snowpark Session (SiS)")
    except Exception:
        # 2. Fallback to Local Connector (for dev)
        try:
            import snowflake.connector
            params = {
                "user": os.environ.get("SNOWFLAKE_USER"),
                "password": os.environ.get("SNOWFLAKE_PASSWORD"),
                "account": os.environ.get("SNOWFLAKE_ACCOUNT"),
                "role": os.environ.get("SNOWFLAKE_ROLE", "ACCOUNTADMIN"),
                "warehouse": os.environ.get("SNOWFLAKE_WAREHOUSE", "BANKING"),
                "database": os.environ.get("SNOWFLAKE_DATABASE", "CHURN_DEMO"),
                "schema": os.environ.get("SNOWFLAKE_SCHEMA", "PUBLIC")
            }
            st.session_state.conn = snowflake.connector.connect(**params)
            st.info("Connected via Local Connector")
        except Exception as e:
            st.error(f"Failed to connect: {e}")
            st.stop()

def run_query(query):
    try:
        conn = st.session_state.conn
        if hasattr(conn, 'sql'): # Snowpark Session
            return conn.sql(query).to_pandas()
        else: # Local Connector
            return pd.read_sql(query, conn)
    except Exception as e:
        st.error(f"Query Error: {e}")
        return pd.DataFrame()

# -----------------------------------------------------------------------------
# App Logic
# -----------------------------------------------------------------------------

# Metrics
c1, c2, c3 = st.columns(3)
with c1:
    val = run_query("SELECT COUNT(*) FROM CHURN_METRICS_LIVE WHERE RISK_STATUS = 'CRITICAL'").iloc[0,0]
    st.metric("Critical Users", val)
with c2:
    val = run_query("SELECT COUNT(*) FROM HIGH_RISK_ALERTS_HISTORY WHERE PREPARED_EMAIL IS NOT NULL").iloc[0,0]
    st.metric("AI Emails Generated", val)
with c3:
    ts = run_query("SELECT MAX(LAST_UPDATED) FROM CHURN_METRICS_LIVE").iloc[0,0]
    st.metric("Last Refresh", str(ts))

st.divider()

# Main View
col_left, col_right = st.columns([1, 1])

with col_left:
    st.subheader("Critical Alerts Queue")
    df_alerts = run_query("""
        SELECT USER_ID, RISK_SCORE, REASON, ALERT_TIME, PREPARED_EMAIL 
        FROM HIGH_RISK_ALERTS_HISTORY 
        ORDER BY ALERT_TIME DESC LIMIT 20
    """)
    
    if not df_alerts.empty:
        st.dataframe(df_alerts[['USER_ID', 'RISK_SCORE', 'REASON']], use_container_width=True)
        # Selection
        users = df_alerts['USER_ID'].unique().tolist()
        selected_user = st.selectbox("Select User Review:", users)
    else:
        st.info("No active alerts.")
        selected_user = None

with col_right:
    if selected_user:
        st.subheader(f"👤 Review: {selected_user}")
        row = df_alerts[df_alerts['USER_ID'] == selected_user].iloc[0]
        
        # Risk Meter
        st.progress(row['RISK_SCORE'], text=f"Churn Risk: {row['RISK_SCORE']:.2f}")
        
        # GenAI Content
        st.markdown("#### Cortex Agent Suggestion")
        if row['PREPARED_EMAIL']:
            st.info(row['PREPARED_EMAIL'])
        else:
            st.warning("Agent has not generated an email yet (Check Tasks).")
            
        # Actions
        if st.button("Approve & Send Offer"):
            st.toast(f"Offer sent to {selected_user}!", icon='✅')
            # Mock update
