import schedule
import time
import logging
import sys

# Import the new Beam pipeline scripts from Phase 3
import beam_consumer_datalake
import beam_consumer_datawarehouse

# --- Initialization ---
logging.basicConfig(level=logging.INFO, format='%(asctime)s - [Orchestrator] - %(levelname)s - %(message)s')

def run_all_jobs():
    """The function that 'schedule' will call."""
    
    logging.info("Starting orchestration cycle...")
    
    try:
        logging.info("--- Starting Data Lake job ---")
        # Call the Beam pipeline function
        beam_consumer_datalake.run_pipeline()
        logging.info("--- Data Lake job finished ---")
    except Exception as e:
        logging.error(f"ERROR during Data Lake job execution: {e}")
        
    try:
        logging.info("--- Starting Data Warehouse job ---")
        # Call the Beam pipeline function
        beam_consumer_datawarehouse.run_pipeline()
        logging.info("--- Data Warehouse job finished ---")
    except Exception as e:
        logging.error(f"ERROR during Data Warehouse job execution: {e}")
        
    logging.info(f"Cycle finished. Next run in 10 minutes.")


# --- Main Loop ---
if __name__ == "__main__":
    
    # Run once at startup to process the backlog
    logging.info("Running initial cycle at startup to process backlog...")
    run_all_jobs()
    logging.info("First cycle finished. Switching to 10-minute schedule.")

    # Define the schedule
    schedule.every(10).minutes.do(run_all_jobs)
    
    try:
        while True:
            schedule.run_pending()
            time.sleep(1) # Check every second if a job is due
    except KeyboardInterrupt:
        logging.info("Orchestrator stopping.")
        sys.exit(0)