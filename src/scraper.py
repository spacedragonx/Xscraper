import logging
import time
import random
import json
import os
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import NoSuchElementException, TimeoutException
from webdriver_manager.chrome import ChromeDriverManager

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

HASHTAGS = ['#nifty50', '#sensex', '#intraday', '#banknifty']
TWEETS_PER_HASHTAG = 250
MIN_TWEETS_TO_MEET_GOAL = 500
COOKIE_FILE = 'cookies.json'

def get_driver():
    """Initializes the WebDriver for Brave Browser."""
    options = webdriver.ChromeOptions()

    # --- ADD THIS SECTION FOR BRAVE ---
    # You must find the correct path to the Brave executable on your system
    #
    # Common paths:
    # Windows:
    # brave_path = r"C:\Program Files\BraveSoftware\Brave-Browser\Application\brave.exe"
    # macOS:
    # brave_path = "/Applications/Brave Browser.app/Contents/MacOS/Brave Browser"
    #
    # Update this line with your actual path:
    brave_path = r"C:\Program Files\BraveSoftware\Brave-Browser\Application\brave.exe" # <-- UPDATE THIS
    
    if not os.path.exists(brave_path):
        logging.error(f"Brave executable not found at: {brave_path}")
        logging.error("Please find the correct path to 'brave.exe' (Windows) or 'Brave Browser' (macOS) and update the get_driver() function.")
        return None # Return None to stop the script

    options.binary_location = brave_path
    # --- END OF SECTION ---

    options.add_argument("--start-maximized")
    options.add_argument("--disable-infobars")
    options.add_argument("--disable-extensions")
    options.add_argument("--disable-notifications")
    options.add_argument("--user-agent=Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
    
    # We can still use ChromeDriverManager, as Brave uses the same driver
    s = Service(ChromeDriverManager().install())
    driver = webdriver.Chrome(service=s, options=options)
    return driver

def load_cookies(driver):
    """Loads session cookies from a file to bypass login."""
    if not os.path.exists(COOKIE_FILE):
        logging.error(f"{COOKIE_FILE} not found. Please log in to x.com, export your cookies, and save them as {COOKIE_FILE}.")
        driver.quit()
        return False

    logging.info(f"Loading cookies from {COOKIE_FILE}...")
    with open(COOKIE_FILE, 'r') as f:
        cookies = json.load(f)
        
    # We must visit the domain *before* adding cookies
    driver.get("https://x.com")
    
    for cookie in cookies:
        # Selenium has issues with 'expiry' field, so we use 'expires' if present
        if 'expires' in cookie:
            cookie['expires'] = int(cookie['expires'])
        
        # Ensure 'sameSite' is valid if present
        if 'sameSite' in cookie and cookie['sameSite'] not in ['Strict', 'Lax', 'None']:
            del cookie['sameSite']
             
        try:
            driver.add_cookie(cookie)
        except Exception as e:
            logging.debug(f"Could not add cookie: {cookie.get('name')}. Error: {e}")
            
    logging.info("Cookies loaded. Refreshing page to apply session.")
    driver.refresh()
    time.sleep(3) # Wait for refresh
    return True

def parse_tweet(element):
    """
    Parses a single tweet element to extract data.
    Uses data-testid for robust selection.
    """
    try:
        # Tweet ID and Timestamp
        tweet_id = "N/A"
        timestamp = "N/A"
        try:
            time_element = element.find_element(By.TAG_NAME, 'time')
            timestamp = time_element.get_attribute('datetime')
            link_element = time_element.find_element(By.XPATH, "./..")
            tweet_id = link_element.get_attribute('href').split('/')[-1]
        except NoSuchElementException:
            pass # Continue if timestamp not found

        # Username and Handle
        try:
            user_data = element.find_element(By.XPATH, ".//div[@data-testid='User-Name']")
            username = user_data.find_element(By.TAG_NAME, 'span').text
        except NoSuchElementException:
            username = "N/A"

        # Tweet Content
        try:
            content = element.find_element(By.XPATH, ".//div[@data-testid='tweetText']").text
        except NoSuchElementException:
            content = ""

        # Engagement Metrics
        def get_stat(testid):
            try:
                stat_element = element.find_element(By.XPATH, f".//div[@data-testid='{testid}']")
                stat_text = stat_element.find_element(By.XPATH, ".//span[@data-testid='app-text']").text
                # Handle 'K' (thousands) and 'M' (millions)
                if 'K' in stat_text:
                    return int(float(stat_text.replace('K', '')) * 1000)
                if 'M' in stat_text:
                    return int(float(stat_text.replace('M', '')) * 1000000)
                return int(stat_text) if stat_text else 0
            except (NoSuchElementException, ValueError):
                return 0

        comments = get_stat('reply')
        retweets = get_stat('retweet')
        likes = get_stat('like')

        # Mentions and Hashtags
        mentions = [m.text for m in element.find_elements(By.XPATH, ".//a[contains(text(), '@')]")]
        hashtags = [h.text for h in element.find_elements(By.XPATH, ".//a[contains(text(), '#')]")]

        tweet_data = {
            "tweet_id": tweet_id,
            "timestamp": timestamp,
            "username": username,
            "content": content,
            "likes": likes,
            "retweets": retweets,
            "comments": comments,
            "mentions": mentions,
            "hashtags": hashtags,
        }
        
        # We only want tweets with content
        if content and tweet_id != "N/A":
            return tweet_data
        return None

    except Exception as e:
        logging.warning(f"Error parsing tweet: {e}")
        return None

def fetch_tweets_for_hashtag(driver, hashtag, seen_ids):
    """
    Fetches tweets for a single hashtag by scrolling and parsing.
    Includes client-side rate limiting.
    """
    search_url = f"https://x.com/search?q={hashtag.replace('#', '%23')}&src=typed_query&f=live"
    driver.get(search_url)
    logging.info(f"Scraping for: {hashtag}")
    
    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.XPATH, "//article[@data-testid='tweet']"))
        )
    except TimeoutException:
        logging.error(f"Timed out waiting for tweets to load for {hashtag}")
        return []

    collected_tweets = []
    tweets_on_page = 0
    
    while len(collected_tweets) < TWEETS_PER_HASHTAG:
        elements = driver.find_elements(By.XPATH, "//article[@data-testid='tweet']")
        
        if not elements or len(elements) == tweets_on_page:
            logging.info(f"No new tweets found for {hashtag}. Ending search.")
            break
            
        tweets_on_page = len(elements)
        
        for el in elements:
            tweet = parse_tweet(el)
            if tweet and tweet['tweet_id'] not in seen_ids:
                collected_tweets.append(tweet)
                seen_ids.add(tweet['tweet_id'])
                if len(collected_tweets) >= TWEETS_PER_HASHTAG:
                    break
        
        driver.execute_script("window.scrollTo(0, document.body.scrollHeight);")
        
        # --- CLIENT-SIDE RATE LIMITER ---
        # This is what you asked for. It's a delay to mimic a human
        # and avoid sending too many requests too quickly.
        sleep_time = random.uniform(2.5, 4.5)
        logging.debug(f"Rate limiter sleeping for {sleep_time:.2f} seconds...")
        time.sleep(sleep_time)

    logging.info(f"Collected {len(collected_tweets)} new tweets for {hashtag}")
    return collected_tweets

def run_selenium_scraper():
    """
    Main function to run the Selenium-based scraper.
    """
    driver = None
    try:
        driver = get_driver()
        
        if not load_cookies(driver):
            logging.error("Cookie loading failed. Exiting.")
            return []
        
        all_tweets = []
        seen_ids = set()

        for hashtag in HASHTAGS:
            new_tweets = fetch_tweets_for_hashtag(driver, hashtag, seen_ids)
            all_tweets.extend(new_tweets)
            logging.info(f"Total unique tweets collected so far: {len(all_tweets)}")
            
            if len(all_tweets) >= MIN_TWEETS_TO_MEET_GOAL:
                logging.info(f"Met minimum goal of {MIN_TWEETS_TO_MEET_GOAL}. Stopping scrape.")
                break

    except Exception as e:
        logging.error(f"An error occurred during the scraping process: {e}")
    finally:
        if driver:
            driver.quit()
            
    logging.info(f"Total unique tweets collected: {len(all_tweets)}")
    return all_tweets

if __name__ == "__main__":
    raw_tweets = run_selenium_scraper()
    if raw_tweets:
        print(f"\nCollected {len(raw_tweets)} total tweets.")
        print("\n--- Sample Tweet ---")
        print(raw_tweets[0])
    else:
        print("No tweets were collected.")


