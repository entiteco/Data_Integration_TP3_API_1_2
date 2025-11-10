import mysql.connector
from kafka import KafkaConsumer
import json
import logging
import sys
from datetime import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# --- Configuration ---
MYSQL_CONFIG = {
    'host': 'localhost', 'port': 3306, 'user': 'root',
    'password': 'admin123', 'database': 'tp3'
}
KAFKA_BROKER = 'localhost:9092'
# We only listen to KSQLDB TABLE topics for the Data Warehouse
TABLE_TOPICS = ['TOTAL_DEPENSE_PAR_USER_TYPE', 'TRANSACTION_LIFECYCLE', 'PRODUCT_PURCHASE_COUNTS', 'TOTAL_PAR_TYPE_5MIN_SLIDING']

# SQL UPSERT (INSERT ... ON DUPLICATE KEY UPDATE) templates for each table
SQL_UPSERT_TEMPLATES = {
    'TOTAL_DEPENSE_PAR_USER_TYPE': """
        INSERT INTO total_depense_par_user_type (user_id, transaction_type, total_depense_usd)
        VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE total_depense_usd = VALUES(total_depense_usd)
    """,
    'TRANSACTION_LIFECYCLE': """
        INSERT INTO transaction_lifecycle (transaction_id, status_history, last_update_time)
        VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE
            status_history = VALUES(status_history), last_update_time = VALUES(last_update_time)
    """,
    'PRODUCT_PURCHASE_COUNTS': """
        INSERT INTO product_purchase_counts (product_id, purchase_count)
        VALUES (%s, %s) ON DUPLICATE KEY UPDATE purchase_count = VALUES(purchase_count)
    """,
    'TOTAL_PAR_TYPE_5MIN_SLIDING': """
        INSERT INTO total_par_type_5min_sliding (window_start_time, transaction_type, total_amount_5min)
        VALUES (%s, %s, %s) ON DUPLICATE KEY UPDATE total_amount_5min = VALUES(total_amount_5min)
    """
}

# --- Beam Processing Logic ---
class WriteToMySQL(beam.DoFn):
    """A Beam PTransform (DoFn) to write each message to MySQL."""
    
    def setup(self):
        """Opens one DB connection per Beam worker."""
        self.conn = mysql.connector.connect(**MYSQL_CONFIG)
        self.cursor = self.conn.cursor()
        logging.info("Beam worker opened MySQL connection.")

    def process(self, message):
        """Processes each message from the PCollection."""
        topic = message['topic']
        msg_key = message['key']
        msg_value = message['value']
        
        # Get the correct SQL template and format the data
        sql_template = SQL_UPSERT_TEMPLATES.get(topic)
        sql_data = self.get_sql_data(topic, msg_key, msg_value)

        if not sql_template or not sql_data:
            logging.warning(f"Pas de template SQL ou de données valides (Beam).")
            return

        try:
            self.cursor.execute(sql_template, sql_data)
            logging.info(f"Beam UPSERT pour {topic} - Clé: {sql_data[0]}")
        except Exception as e:
            logging.error(f"Erreur Beam UPSERT: {e} | Données: {sql_data}")

    def finish(self):
        """Commits the transaction batch and closes the connection when the worker is done."""
        self.conn.commit()
        self.cursor.close()
        self.conn.close()
        logging.info("Beam worker closed MySQL connection.")
        
    def get_sql_data(self, topic, msg_key, msg_value):
        """Helper function to parse the message key/value and format for SQL."""
        if not msg_key: return None
        try:
            if topic == 'TOTAL_DEPENSE_PAR_USER_TYPE':
                # Key is JSON: {"USER_ID": "...", "TRANSACTION_TYPE": "..."}
                key_data = json.loads(msg_key) 
                return (key_data.get('USER_ID'), key_data.get('TRANSACTION_TYPE'), msg_value.get('TOTAL_DEPENSE_USD'))
            
            elif topic == 'TRANSACTION_LIFECYCLE':
                # Key is a simple string: "TXN-..."
                ts_string = msg_value.get('LAST_UPDATE_TIME')
                dt_obj = None
                if ts_string:
                    # Convert ISO 8601 string to a datetime object
                    dt_obj = datetime.strptime(ts_string, '%Y-%m-%dT%H:%M:%S.%fZ')
                return (msg_key, json.dumps(msg_value.get('STATUS_HISTORY')), dt_obj)
            
            elif topic == 'PRODUCT_PURCHASE_COUNTS':
                # La clé est un simple STRING (le product_id)
                # La valeur est un JSON (ex: {"PURCHASE_COUNT": 5})
                return (
                    msg_key, 
                    msg_value.get('PURCHASE_COUNT')
                )
            
            elif topic == 'TOTAL_PAR_TYPE_5MIN_SLIDING':
                # La clé est un JSON (ex: {"TRANSACTION_TYPE":"purchase", "WINDOWSTART":1678886400000})
                key_data = json.loads(msg_key)
                # La valeur est un JSON (ex: {"TOTAL_AMOUNT_5MIN": 1234.56})
                
                # Convertit le timestamp de ksqlDB (millisecondes) en datetime
                window_start = datetime.fromtimestamp(key_data.get('WINDOWSTART') / 1000)
                return (
                    window_start,
                    key_data.get('TRANSACTION_TYPE'),
                    msg_value.get('TOTAL_AMOUNT_5MIN')
                )
        except Exception as e:
            logging.error(f"Error in get_sql_data (Beam): {e}")
            return None

# --- Main Job Function ---
def run_pipeline():
    """Defines and runs the batch pipeline."""
    logging.info("Starting Data Warehouse BATCH pipeline...")

    # 1. Poll Kafka for a batch of messages
    try:
        consumer = KafkaConsumer(
            *TABLE_TOPICS,
            bootstrap_servers=KAFKA_BROKER,
            auto_offset_reset='earliest', # Start from last committed offset
            group_id='datawarehouse_batch_consumer_group', # Different group ID
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        # Poll Kafka for 5 seconds or a max of 10000 records
        raw_messages = consumer.poll(timeout_ms=5000, max_records=10000)
        consumer.commit() # Commit offsets for this batch
        consumer.close() # Close the consumer
    except Exception as e:
        logging.error(f"Error connecting to Kafka: {e}")
        return

    # 2. Prepare the data for the Beam pipeline
    # Convert Kafka records into a simple list of dicts
    message_list = []
    for tp, msgs in raw_messages.items():
        for msg in msgs:
            message_list.append({
                'topic': msg.topic,
                'key': msg.key,
                'value': msg.value
            })
            
    if not message_list:
        logging.info("No new messages found for the Data Warehouse.")
        return

    logging.info(f"Processing {len(message_list)} messages with Apache Beam...")

    # 3. Define and run the Beam pipeline
    options = PipelineOptions(runner='DirectRunner') # Use local runner
    with beam.Pipeline(options=options) as p:
        (
            p | 'Create Batch' >> beam.Create(message_list)
              | 'Write to MySQL' >> beam.ParDo(WriteToMySQL())
        )
    logging.info("Data Warehouse BATCH pipeline finished.")

# --- Script execution ---
if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    run_pipeline()