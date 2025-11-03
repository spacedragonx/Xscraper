import logging
from ntscraper import Nitter
from concurrent.futures import ThreadPoolExecutor, as_completed
import time

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

HASHTAGS = ['#nifty50', '#sensex', '#intraday', '#banknifty']
TWEETS_PER_HASHTAG = 600
TIMEFRAME_HOURS = 24

def fetch_tweets_for_hashtag(hashtag):
    logging.info(f"Starting scrape for: {hashtag}")
    scraper = Nitter()
    try:
        search_query = f"{hashtag} near:india"
        
        tweets_data = scraper.get_tweets(search_query, mode='hashtag', number=TWEETS_PER_HASHTAG)
        
        final_tweets = []
        if 'tweets' in tweets_data:
            for tweet in tweets_data['tweets']:
                extracted_data = {
                    "username": tweet.get('user', {}).get('username', 'N/A'),
                    "timestamp": tweet.get('date', 'N/A'),
                    "content": tweet.get('text', ''),
                    "likes": tweet.get('stats', {}).get('likes', 0),
                    "retweets": tweet.get('stats', {}).get('retweets', 0),
                    "comments": tweet.get('stats', {}).get('comments', 0),
                    "mentions": tweet.get('mentions', []),
                    "hashtags": tweet.get('hashtags', []),
                    "tweet_id": tweet.get('link', '').split('/')[-1]
                }
                final_tweets.append(extracted_data)
        
        logging.info(f"Finished scrape for: {hashtag}. Found {len(final_tweets)} tweets.")
        return final_tweets
        
    except Exception as e:
        logging.error(f"Error scraping {hashtag}: {e}")
        return []

def run_concurrent_scraper():
    all_tweets = []
    seen_ids = set()
    
    with ThreadPoolExecutor(max_workers=len(HASHTAGS)) as executor:
        future_to_hashtag = {executor.submit(fetch_tweets_for_hashtag, tag): tag for tag in HASHTAGS}
        
        for future in as_completed(future_to_hashtag):
            hashtag = future_to_hashtag[future]
            try:
                tweets = future.result()
                logging.info(f"Processing results for {hashtag}...")
                for tweet in tweets:
                    if tweet['tweet_id'] and tweet['tweet_id'] not in seen_ids:
                        all_tweets.append(tweet)
                        seen_ids.add(tweet['tweet_id'])
            except Exception as e:
                logging.error(f"Error processing future for {hashtag}: {e}")
                
    logging.info(f"Total unique tweets collected: {len(all_tweets)}")
    return all_tweets

if __name__ == "__main__":
    raw_tweets = run_concurrent_scraper()
    print(f"Collected {len(raw_tweets)} total tweets.")
    if raw_tweets:
        print("\n--- Sample Tweet ---")
        print(raw_tweets[0])