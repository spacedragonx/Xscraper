# Xscraper

This project is a data collection and analysis system for real-time market intelligence, focusing on Indian stock market discussions on Twitter/X.
This version implements the entire data collection module using the Selenium framework to fulfill the assignment's suggestion. It is designed for robust operation by using cookie-based session management and mimicking human-like scrolling behavior.

## 🚀 Key Features
* **Selenium-Based Scraping:** Uses Selenium and webdriver-manager to control a live Chrome/Brave browser for real-time tweet extraction.
* **Automated Session Management:** Requires a `cookies.json` file for login automation, eliminating the need to store username/password directly in the code.
* **Robust Parsing:** Parses tweet data using stable DOM attributes (`data-testid`) for reliable content extraction.
* **Client-Side Rate Limiter:** Implements randomized scrolling delays (`random.uniform(2.5, 4.5)`) to minimize bot-detection risks and ensure ethical scraping.
* **Data-to-Signal Pipeline:**

## Setup and Execution
1.  **Clone the repository:**
    ```bash
    git clone [https://github.com/your-username/market-intelligence-system.git](https://github.com/your-username/market-intelligence-system.git)
    cd market-intelligence-system
    ```
2.  **Prepare the Session File (Crucial Step):**
    * Log in to `x.com` in your browser (Chrome or Brave).
    * Use a browser extension (like **Cookie-Editor**) to export all cookies for the `x.com` domain as a **JSON** file.
    * Save this file as `cookies.json` in the project's root directory.
3.  **Security Best Practice:**
    * Add the file to your Git ignore list to prevent exposing session tokens:
        ```bash
        echo "cookies.json" >> .gitignore
        ```
4.  **Create a virtual environment:**
    ```bash
    python -m venv venv
    source venv/bin/activate  # On Windows: venv\Scripts\activate
    ```
5.  **Install dependencies:**
    * This will install `selenium`, `webdriver-manager`, and all other required libraries.
    ```bash
    pip install -r requirements.txt
    ```
6.  **Browser Configuration Check:**
    * **If using Brave:** Ensure the `brave_path` variable in `src/scraper.py` is updated to point to the correct location of your Brave executable (`brave.exe` on Windows or `Brave Browser` on macOS).
7.  **Run the main pipeline:**
    ```bash
    python -m src.main
    ```
8.  **Check the results:**
    * After the script finishes, you will find the following in the `output/` directory:
    * `tweets.parquet`: The cleaned, processed data.
    * `signals_analysis.png`: A plot of the aggregated hourly market sentiment and tweet volume.

## Scalability (Handling 10x Data)
This Selenium solution is resource-intensive and requires architectural changes for massive scale:

* **Horizontal Scaling (Scraping):** Replace single-machine operation with **Selenium Grid** to distribute browser instances across multiple servers and IP addresses.
* **IP Rotation:** Implement a robust, commercial **Proxy Rotation Service** (e.g., Bright Data) to avoid site-wide bans.
* **Big Data Processing:** Migrate the data cleaning and analysis steps from single-threaded `pandas` to a distributed framework like **Dask** or **Apache Spark** to handle datasets that exceed local machine memory.

##  Tech Stack

* **Core Language:** Python 3.10+
* **Web Scraping:** Selenium
* **Browser Driver:** `webdriver-manager` (for automated driver installation)
* **Data Processing:** Pandas
* **Storage:** PyArrow (for Parquet file format)
* **Analysis:** NLTK (VADER for sentiment analysis)
* **Visualization:** Matplotlib


##  License

This project is licensed under the MIT License.