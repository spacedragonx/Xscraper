import logging
from src.scraper import run_selenium_scraper
from src.processor import process_and_store_data
from src.analysis import run_analysis
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def main():
    """Runs the full data collection, processing, and analysis pipeline."""
    
    start_time = time.time()
    
    # --- PHASE 1: DATA COLLECTION (Now using Selenium) ---
    logging.info("Starting Phase 1: Data Collection (Selenium)...")
    try:
        raw_tweets = run_selenium_scraper()
        if not raw_tweets:
            logging.error("Data collection failed. No tweets were fetched.")
            return
        logging.info(f"Phase 1 Complete. Collected {len(raw_tweets)} raw tweets.")
    except Exception as e:
        logging.error(f"Fatal error during data collection: {e}")
        return

    # --- PHASE 2: DATA PROCESSING & STORAGE ---
    logging.info("Starting Phase 2: Data Processing...")
    try:
        processed_df = process_and_store_data(raw_tweets)
        if processed_df.empty:
            logging.error("Data processing failed. No data was saved.")
            return
        logging.info(f"Phase 2 Complete. Processed and saved {len(processed_df)} tweets.")
    except Exception as e:
        logging.error(f"Fatal error during data processing: {e}")
        return

    # --- PHASE 3 & 4: ANALYSIS & VISUALIZATION ---
    logging.info("Starting Phase 3 & 4: Analysis and Visualization...")
    try:
        run_analysis()
        logging.info("Phase 3 & 4 Complete. Analysis finished.")
    except Exception as e:
        logging.error(f"Fatal error during analysis: {e}")
        return
        
    end_time = time.time()
    logging.info(f"Full pipeline finished in {end_time - start_time:.2f} seconds.")

if __name__ == "__main__":
    main()
