from kafka import KafkaConsumer
import json
import os
import logging
import sys
from datetime import datetime
import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions

# --- Configuration ---
KAFKA_BROKER = 'localhost:9092'
DATA_LAKE_PATH = '../data_lake' # Path to the parent data_lake directory
TOPIC_CONFIG = {
    # Streams
    'TRANSACTIONS_FLAT': 'streams', 'TRANSACTIONS_PROCESSED': 'streams',
    'TRANSACTIONS_BLACKLISTED': 'streams', 'TRANSACTIONS_COMPLETED': 'streams',
    'TRANSACTIONS_PENDING': 'streams', 'TRANSACTIONS_FAILED': 'streams',
    'TRANSACTIONS_PROCESSING': 'streams', 'TRANSACTIONS_CANCELLED': 'streams',
    # Tables
    'TOTAL_DEPENSE_PAR_USER_TYPE': 'tables', 'TRANSACTION_LIFECYCLE': 'tables',
    'TOTAL_PAR_TYPE_5MIN_SLIDING': 'tables', 'PRODUCT_PURCHASE_COUNTS': 'tables',
    'TOTAL_PAR_TRANSACTION_TYPE': 'tables'
}
ALL_TOPICS = list(TOPIC_CONFIG.keys())

# --- Partitioning Function ---
def get_partition_path(topic_type, topic_name, kafka_timestamp):
    """Builds the Hive-style partitioning path."""
    
    # Convert Kafka message timestamp (ms) to a datetime object
    dt = datetime.fromtimestamp(kafka_timestamp / 1000)
    
    # Extract year, month, day
    year, month, day = dt.strftime('%Y'), dt.strftime('%m'), dt.strftime('%d')
    
    # Build the full path, e.g.:
    # ../data_lake/streams/TRANSACTIONS_FLAT/year=2025/month=10/day=28
    return os.path.join(
        DATA_LAKE_PATH, topic_type, topic_name,
        f"year={year}", f"month={month}", f"day={day}"
    )

# --- Beam Processing Logic ---
class WriteToFile(beam.DoFn):
    """A Beam PTransform (DoFn) to write each message to a file."""
    
    def process(self, message):
        # Extract data from the message dictionary
        topic = message['topic']
        data = message['value']
        
        # Determine if it's a 'stream' or 'table'
        topic_type = TOPIC_CONFIG.get(topic)
        if not topic_type:
            logging.warning(f"Topic {topic} not configured. Message ignored.")
            return

        # Get the full Hive-style path
        path = get_partition_path(topic_type, topic, message['timestamp'])
        
        # Ensure the directory structure exists
        os.makedirs(path, exist_ok=True)
        
        # Create a unique filename based on the Kafka offset
        filename = f"data_offset_{message['offset']}.json"
        filepath = os.path.join(path, filename)

        # Write the message data as a JSON file
        try:
            with open(filepath, 'w') as f:
                json.dump(data, f)
            logging.info(f"Beam worker wrote to {filepath}")
        except Exception as e:
            logging.error(f"Beam worker failed to write file {filepath}: {e}")

# --- Main Job Function ---
def run_pipeline():
    """Defines and runs the batch pipeline."""
    logging.info("Starting Data Lake BATCH pipeline...")
    
    # 1. Connect to Kafka and poll for a batch of messages
    try:
        consumer = KafkaConsumer(
            *ALL_TOPICS,
            bootstrap_servers=KAFKA_BROKER,
            auto_offset_reset='earliest', # Start from the beginning if no offset is stored
            group_id='datalake_batch_consumer_group', # Crucial for batch processing
            value_deserializer=lambda v: json.loads(v.decode('utf-8'))
        )
        
        # Poll Kafka for 5 seconds or a max of 10000 records
        raw_messages = consumer.poll(timeout_ms=5000, max_records=10000)
        
        # Commit the offsets for this batch
        consumer.commit() 
        consumer.close()
        logging.info(f"{len(raw_messages)} partitions returned data.")

    except Exception as e:
        logging.error(f"Error connecting to Kafka: {e}")
        return

    # 2. Prepare the data for the Beam pipeline
    # We convert the Kafka ConsumerRecord objects into a simple list of dicts
    message_list = []
    for tp, msgs in raw_messages.items(): # Iterate over partitions
        for msg in msgs: # Iterate over messages in that partition
            message_list.append({
                'topic': msg.topic,
                'value': msg.value,
                'timestamp': msg.timestamp,
                'offset': msg.offset
            })
            
    if not message_list:
        logging.info("No new messages found for the Data Lake.")
        return

    logging.info(f"Processing {len(message_list)} messages with Apache Beam...")
    
    # 3. Define and run the Beam pipeline
    options = PipelineOptions(runner='DirectRunner') # Use local runner
    with beam.Pipeline(options=options) as p:
        (
            p | 'Create Batch' >> beam.Create(message_list) # Create a PCollection from our list
              | 'Write Files' >> beam.ParDo(WriteToFile())   # Apply our custom DoFn to each element
        )
    logging.info("Data Lake BATCH pipeline finished.")

# --- Script execution ---
if __name__ == "__main__":
    # Configure logging
    logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
    # Run the main pipeline function
    run_pipeline()