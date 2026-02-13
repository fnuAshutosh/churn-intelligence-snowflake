import snowflake.connector
from src.core.config import get_snowflake_connection_params

def setup_ml_pipeline():
    params = get_snowflake_connection_params()
    conn = snowflake.connector.connect(**params)
    cursor = conn.cursor()
    print("Creating Snowpark ML Objects...")

    try:
        cursor.execute("CREATE STAGE IF NOT EXISTS MODEL_STAGE")

        # 1. Training Stored Procedure
        print("-> Creating Training Procedure...")
        sp_sql = """
        CREATE OR REPLACE PROCEDURE TRAIN_CHURN_MODEL()
        RETURNS VARCHAR
        LANGUAGE PYTHON
        RUNTIME_VERSION = '3.11'
        PACKAGES = ('snowflake-snowpark-python', 'scikit-learn', 'joblib', 'pandas')
        HANDLER = 'train_model'
        AS
        $$
        import snowflake.snowpark as snowpark
        from sklearn.ensemble import RandomForestClassifier
        import pandas as pd
        import joblib
        import os

        def train_model(session):
            df = session.table("CHURN_SCORES_RAW").to_pandas()
            if len(df) < 5:
                return "Not enough data to train."

            df['TARGET'] = (df['CHURN_SCORE'] > 0.8).astype(int)
            X = df[['DECLINE_COUNT', 'DISPUTE_COUNT', 'SPEND_AMOUNT']]
            y = df['TARGET']
            
            clf = RandomForestClassifier(n_estimators=10)
            clf.fit(X, y)
            
            joblib.dump(clf, '/tmp/churn_rf.joblib')
            session.file.put('/tmp/churn_rf.joblib', "@MODEL_STAGE", auto_compress=False, overwrite=True)
            return "Success: Trained Random Forest"
        $$;
        """
        cursor.execute(sp_sql)

        # 2. Prediction UDF
        print("-> Creating Scoring UDF...")
        udf_sql = """
            CREATE OR REPLACE FUNCTION PREDICT_CHURN(decline_cnt INT, dispute_cnt INT, spend FLOAT)
            RETURNS FLOAT
            LANGUAGE PYTHON
            RUNTIME_VERSION = '3.11'
            PACKAGES = ('scikit-learn', 'joblib', 'pandas')
            IMPORTS = ('@MODEL_STAGE/churn_rf.joblib')
            HANDLER = 'predict'
            AS
            $$
            import joblib
            import pandas as pd
            import sys
            import os

            model = None
            def predict(decline_cnt, dispute_cnt, spend):
                global model
                if model is None:
                    import_dir = sys._xoptions.get("snowflake_import_directory")
                    model_path = os.path.join(import_dir, "churn_rf.joblib")
                    model = joblib.load(model_path)
                
                if decline_cnt is None: return 0.0
                input_df = pd.DataFrame([[decline_cnt, dispute_cnt, spend]], columns=['DECLINE_COUNT', 'DISPUTE_COUNT', 'SPEND_AMOUNT'])
                prob = model.predict_proba(input_df)[0][1]
                return float(prob)
            $$;
            """
        cursor.execute(udf_sql)
        print("Created UDF: PREDICT_CHURN")

    except Exception as e:
        print(f"Failed to create ML Objects: {e}")
    finally:
        conn.close()

if __name__ == "__main__":
    setup_ml_pipeline()
