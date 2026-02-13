import json
import time
from typing import List, Dict
from kafka import KafkaConsumer
from snowflake.connector.pandas_tools import write_pandas
from pandas import DataFrame
import snowflake.connector
from src.core.config import load_config

class SnowflakeIngestion:
    def __init__(self):
        self.config = load_config()
        self.batch_size = 100
        self.buffer: List[Dict] = []
        self.kafka_topic = self.config['kafka']['topic']
        
    def get_snowflake_conn(self):
        return snowflake.connector.connect(**self.config['snowflake'])

    def flush_buffer(self):
        if not self.buffer:
            return

        print(f"-> Flushing {len(self.buffer)} records to Snowflake...")
        conn = self.get_snowflake_conn()
        try:
            # Convert to DataFrame matching CHURN_SCORES_RAW schema
            # We assume incoming JSON matches schema or we map it here
            df = DataFrame(self.buffer)
            
            # Ensure columns match strict Schema if needed, or use a VARIANT column for true ELT
            # For this demo, we map to the columns defined in pipeline.py
            # Expected: USER_ID, WINDOW_START, WINDOW_END, CHURN_SCORE, ...
            # If input is raw events, we might need a different table. 
            # BUT: The 'simulator' generates pre-scored events in the current proto.
            # So we just dump them.
            
            write_pandas(conn, df, "CHURN_SCORES_RAW", quote_identifiers=False, auto_create_table=False)
            print("✓ Flush Successful")
            self.buffer = []
        except Exception as e:
            print(f"✗ Flush Failed: {e}")
        finally:
            conn.close()

    def run(self):
        print(f"Starting Ingestion from {self.kafka_topic}...")
        consumer = KafkaConsumer(
            self.kafka_topic,
            bootstrap_servers=self.config['kafka']['bootstrap_servers'],
            value_deserializer=lambda m: json.loads(m.decode('utf-8')),
            # SASL Configs if needed (from config)
            # security_protocol=...
        )

        for message in consumer:
            self.buffer.append(message.value)
            if len(self.buffer) >= self.batch_size:
                self.flush_buffer()

if __name__ == "__main__":
    ingestor = SnowflakeIngestion()
    ingestor.run()
