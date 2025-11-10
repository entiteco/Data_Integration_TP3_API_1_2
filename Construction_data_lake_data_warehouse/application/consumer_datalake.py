from kafka import KafkaConsumer
import json
import os
import logging
import sys
from datetime import datetime

# --- Configuration ---
KAFKA_BROKER = 'localhost:9092'
DATA_LAKE_PATH = '../data_lake' # The folder is at the parent level

# Topic configuration: Topic name -> Type ('streams' or 'tables')
# This is for the /data_lake/streams/ or /data_lake/tables/ subfolders
TOPIC_CONFIG = {
    # Streams
    'TRANSACTIONS_FLAT': 'streams',
    'TRANSACTIONS_PROCESSED': 'streams',
    'TRANSACTIONS_BLACKLISTED': 'streams',
    'TRANSACTIONS_COMPLETED': 'streams',
    'TRANSACTIONS_PENDING': 'streams',
    'TRANSACTIONS_FAILED': 'streams',
    'TRANSACTIONS_PROCESSING': 'streams',
    'TRANSACTIONS_CANCELLED': 'streams',
    
    # Tables
    'TOTAL_DEPENSE_PAR_USER_TYPE': 'tables',
    'TRANSACTION_LIFECYCLE': 'tables',
    'TOTAL_PAR_TYPE_5MIN_SLIDING': 'tables',
    'PRODUCT_PURCHASE_COUNTS': 'tables',
    'TOTAL_PAR_TRANSACTION_TYPE': 'tables'
}

ALL_TOPICS = list(TOPIC_CONFIG.keys())

# --- Initialisation ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_partition_path(topic_type, topic_name, kafka_timestamp):
    """Builds the partitioning path (Hive-style)."""
    
    # 1. Convert the Kafka message timestamp (in ms) to a datetime object
    dt = datetime.fromtimestamp(kafka_timestamp / 1000)
    
    # 2. Extract year, month, day
    year = dt.strftime('%Y')
    month = dt.strftime('%m')
    day = dt.strftime('%d')
    
    # 3. Build the path
    # ex: ../data_lake/streams/TRANSACTIONS_FLAT/year=2025/month=10/day=28
    return os.path.join(
        DATA_LAKE_PATH,
        topic_type,
        topic_name,
        f"year={year}",
        f"month={month}",
        f"day={day}"
    )

def main():
    try:
        # Kafka Consumer Initialization
        consumer = KafkaConsumer(
            *ALL_TOPICS,
            bootstrap_servers=KAFKA_BROKER,
            auto_offset_reset='earliest',
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        logging.info(f"Connecting to Kafka at {KAFKA_BROKER}, listening to {len(ALL_TOPICS)} topics...")

    except Exception as e:
        logging.error(f"Error connecting to Kafka: {e}")
        sys.exit(1)

    try:
        # --- Main Loop ---
        for message in consumer:
            topic = message.topic
            data = message.value
            
            logging.debug(f"Message received from {topic}")

            # 1. Determine the destination path
            topic_type = TOPIC_CONFIG.get(topic)
            if not topic_type:
                logging.warning(f"Topic {topic} not configured. Message ignored.")
                continue
                
            path = get_partition_path(topic_type, topic, message.timestamp)
            
            # 2. Create directories if they don't exist
            os.makedirs(path, exist_ok=True)
            
            # 3. Define the filename
            # We use the message offset for a unique filename
            filename = f"data_offset_{message.offset}.json"
            filepath = os.path.join(path, filename)

            # 4. Write the message to the file
            try:
                with open(filepath, 'w') as f:
                    json.dump(data, f)
                logging.info(f"Message written to {filepath}")
                
            except Exception as e:
                logging.error(f"Error writing file {filepath}: {e}")

    except KeyboardInterrupt:
        logging.info("Stopping Data Lake consumer...")
    except Exception as e:
        logging.error(f"Unexpected error: {e}")
    finally:
        if 'consumer' in locals():
            consumer.close()
            logging.info("Kafka connection closed.")

if __name__ == "__main__":
    main()