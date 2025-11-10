import mysql.connector
from kafka import KafkaConsumer
import json
import logging
import sys
from datetime import datetime

# --- Configuration ---
MYSQL_CONFIG = {
    'host': 'localhost',
    'port': 3306,
    'user': 'root',
    'password': 'admin123',
    'database': 'tp3'
}

KAFKA_BROKER = 'localhost:9092'
TABLE_TOPICS = [
    'TOTAL_DEPENSE_PAR_USER_TYPE',
    'TRANSACTION_LIFECYCLE'
]

SQL_UPSERT_TEMPLATES = {
    'TOTAL_DEPENSE_PAR_USER_TYPE': """
        INSERT INTO total_depense_par_user_type (user_id, transaction_type, total_depense_usd)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE
            total_depense_usd = VALUES(total_depense_usd)
    """,
    
    'TRANSACTION_LIFECYCLE': """
        INSERT INTO transaction_lifecycle (transaction_id, status_history, last_update_time)
        VALUES (%s, %s, %s)
        ON DUPLICATE KEY UPDATE
            status_history = VALUES(status_history),
            last_update_time = VALUES(last_update_time)
    """
}

# --- Initialization ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

# MODIFIED: The function handles date conversion
def get_sql_data(topic, msg_key, msg_value):
    """Extracts relevant data from the KSQLDB message (key + value) for our SQL."""
    
    if not msg_key:
        logging.warning(f"Message received without key for topic {topic}. Ignored.")
        return None
    
    try:
        if topic == 'TOTAL_DEPENSE_PAR_USER_TYPE':
            key_data = json.loads(msg_key) 
            return (
                key_data.get('USER_ID'),
                key_data.get('TRANSACTION_TYPE'),
                msg_value.get('TOTAL_DEPENSE_USD')
            )
            
        elif topic == 'TRANSACTION_LIFECYCLE':
            
            # --- MODIFICATION HERE ---
            ts_string = msg_value.get('LAST_UPDATE_TIME')
            dt_obj = None
            if ts_string:
                try:
                    # Parse the '...Z' string into a datetime object
                    # %f handles microseconds
                    dt_obj = datetime.strptime(ts_string, '%Y-%m-%dT%H:%M:%S.%fZ')
                except ValueError:
                    logging.warning(f"Invalid date format: {ts_string}. Ignored.")
                    return None
            
            return (
                msg_key,
                json.dumps(msg_value.get('STATUS_HISTORY')),
                dt_obj # Pass the datetime object instead of the string
            )
            # --- END MODIFICATION ---
            
    except json.JSONDecodeError:
        logging.error(f"JSON decoding error for key: {msg_key}")
    except Exception as e:
        logging.error(f"Unexpected error in get_sql_data: {e}")
        
    return None

def main():
    try:
        consumer = KafkaConsumer(
            *TABLE_TOPICS,
            bootstrap_servers=KAFKA_BROKER,
            auto_offset_reset='earliest',
            key_deserializer=lambda k: k.decode('utf-8') if k else None,
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        logging.info(f"Connecting to Kafka at {KAFKA_BROKER}, listening to {TABLE_TOPICS}...")

    except Exception as e:
        logging.error(f"Error connecting to Kafka: {e}")
        sys.exit(1)

    try:
        db_conn = mysql.connector.connect(**MYSQL_CONFIG)
        cursor = db_conn.cursor()
        logging.info(f"Connecting to MySQL at {MYSQL_CONFIG['host']}...")

        for message in consumer:
            topic = message.topic
            msg_key = message.key
            msg_value = message.value
            
            logging.info(f"Message received from {topic}: Key={msg_key}, Value={msg_value}")

            sql_template = SQL_UPSERT_TEMPLATES.get(topic)
            sql_data = get_sql_data(topic, msg_key, msg_value)

            if not sql_template or not sql_data:
                logging.warning(f"No SQL template or valid data for the message.")
                continue

            try:
                cursor.execute(sql_template, sql_data)
                db_conn.commit()
                # Log the key (user_id or transaction_id)
                logging.info(f"Data UPSERTED for {topic} - Key: {sql_data[0]}")
                
            except Exception as e:
                logging.error(f"Error during SQL UPSERT: {e} | Data: {sql_data}")
                db_conn.rollback()

    except KeyboardInterrupt:
        logging.info("Stopping Data Warehouse consumer...")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    finally:
        if 'consumer' in locals():
            consumer.close()
        if 'db_conn' in locals() and db_conn.is_connected():
            cursor.close()
            db_conn.close()
            logging.info("Kafka and MySQL connections closed.")

if __name__ == "__main__":
    main()