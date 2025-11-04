import pandas as pd
import logging
import os
import re
from datetime import datetime, timedelta

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

OUTPUT_DIR = 'output'
PARQUET_FILE = os.path.join(OUTPUT_DIR, 'tweets.parquet')
TIMEFRAME_HOURS = 24

def clean_tweet_content(text):
    """Cleans the tweet text."""
    text = re.sub(r'http\S+', '', text)  # Remove URLs
    text = re.sub(r'@\w+', '', text)       # Remove mentions
    text = re.sub(r'#\w+', '', text)       # Remove hashtags (we have them in a separate col)
    text = re.sub(r'[^A-Za-z0-9\s.,!?$₹]+', '', text) # Remove non-ASCII/emojis, keep punctuation and currency
    text = text.strip()
    return text

def process_and_store_data(raw_tweets):
    """
    Cleans, normalizes, and stores the collected tweet data in Parquet format.
    """
    if not raw_tweets:
        logging.warning("No raw tweets to process.")
        return pd.DataFrame()

    logging.info(f"Processing {len(raw_tweets)} raw tweets...")
    df = pd.DataFrame(raw_tweets)
    
    # --- 1. Deduplication ---
    df.drop_duplicates(subset=['tweet_id'], keep='first', inplace=True)
    
    # --- 2. Normalization & Type Conversion ---
    # Timestamp
    # Selenium gives a standard ISO 8601 datetime
    df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    df.dropna(subset=['timestamp'], inplace=True) # Drop rows where date conversion failed
    
    # --- 3. Time Filtering (Last 24 Hours) ---
    # Ensure timestamps are timezone-aware (UTC)
    df['timestamp'] = df['timestamp'].dt.tz_localize('UTC')
    cutoff_time = datetime.now(datetime.timezone.utc) - timedelta(hours=TIMEFRAME_HOURS)
    
    df = df[df['timestamp'] >= cutoff_time].copy()
    
    if df.empty:
        logging.warning("No tweets found from the last 24 hours after filtering.")
        return pd.DataFrame()

    logging.info(f"Found {len(df)} tweets from the last 24 hours.")

    # Engagement Metrics
    df['likes'] = pd.to_numeric(df['likes'], errors='coerce').fillna(0).astype(int)
    df['retweets'] = pd.to_numeric(df['retweets'], errors='coerce').fillna(0).astype(int)
    df['comments'] = pd.to_numeric(df['comments'], errors='coerce').fillna(0).astype(int)
    
    # Text Content
    df['cleaned_content'] = df['content'].apply(clean_tweet_content)
    
    # Handle list columns (mentions, hashtags)
    df['mentions'] = df['mentions'].apply(lambda x: x if isinstance(x, list) else [])
    df['hashtags'] = df['hashtags'].apply(lambda x: x if isinstance(x, list) else [])
    
    # --- 4. Schema Selection ---
    final_columns = [
        'tweet_id', 'timestamp', 'username', 'cleaned_content', 
        'likes', 'retweets', 'comments', 'mentions', 'hashtags', 'content'
    ]
    df_final = df[final_columns]
    
    # --- 5. Storage ---
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    try:
        df_final.to_parquet(PARQUET_FILE, engine='pyarrow', index=False)
        logging.info(f"Successfully saved {len(df_final)} processed tweets to {PARQUET_FILE}")
    except Exception as e:
        logging.error(f"Failed to save data to Parquet: {e}")
        
    return df_final
