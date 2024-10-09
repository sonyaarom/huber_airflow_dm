import requests
import logging
import json
from airflow.hooks.S3_hook import S3Hook
from typing import Optional
import pandas as pd


logger = logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)


session = requests.Session()

def get_html_content(url: str) -> Optional[str]:
    """
    Retrieves the HTML content of a given URL.

    This function attempts to retrieve the HTML content of a specified URL.
    If successful, it returns the HTML content as a string.
    If an error occurs during the retrieval process, it logs the error and returns None.
    """
    try:
        response = session.get(url, timeout=10)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        logging.error(f"Failed to retrieve HTML content from {url}: {e}")
        return None


def add_html_content_to_df(df: pd.DataFrame) -> pd.DataFrame:
    """
    Adds the HTML content of each URL to a DataFrame.

    This function takes a DataFrame containing URLs and retrieves the HTML content of each URL.
    It appends the HTML content to a new column in the DataFrame and returns the updated DataFrame.
    """
    total_urls = len(df)
    html_contents = []
    for i, row in df.iterrows():
        url = row['url']
        logging.info(f"Processing URL {i+1}/{total_urls}: {url}")
        html_content = get_html_content(url)
        html_contents.append(html_content)
    
    df['html_content'] = html_contents
    return df


def load_json_from_s3(s3_hook, bucket, key):
    """
    Loads a JSON file from S3.

    This function attempts to load a JSON file from S3.
    If successful, it returns the JSON content as a dictionary.
    If an error occurs during the loading process, it logs the error and returns None.
    """
    try:
        content = s3_hook.read_key(key, bucket)
        return json.loads(content)
    except Exception as e:
        logger.error(f"Error reading {key} from S3: {str(e)}")
        return None

def save_json_to_s3(s3_hook, bucket, key, data):
    """
    Saves a JSON file to S3.

    This function attempts to save a JSON file to S3.
    If successful, it logs a success message.
    If an error occurs during the saving process, it logs the error.
    """
    try:
        json_str = json.dumps(data)
        s3_hook.load_string(json_str, key, bucket, replace=True)
        logger.info(f"Successfully saved to {key}")
    except Exception as e:
        logger.error(f"Error saving to {key}: {str(e)}")

def get_or_create_bm25_values(s3_hook, bucket, bm25_file_key, df, text_column):
    """
    Retrieves or creates BM25 values from S3.

    This function attempts to retrieve BM25 values from S3.
    If not found, it creates and saves the BM25 values to S3.
    It returns the BM25 values.
    """
    bm25_values = load_json_from_s3(s3_hook, bucket, bm25_file_key)
    if not bm25_values:
        bm25_values = create_and_save_bm25_corpus(df, text_column, None)
        save_json_to_s3(s3_hook, bucket, bm25_file_key, bm25_values)
    return bm25_values
    
def send_telegram_message(message):
    """
    Sends a message to a Telegram chat.

    This function sends a message to a Telegram chat using the Telegram Bot API.
    It sends a message with the specified content and parse mode.
    """
    if not TELEGRAM_BOT_TOKEN or not TELEGRAM_CHAT_ID:
        logger.warning("Telegram credentials not set. Skipping Telegram notification.")
        return

    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    try:
        response = requests.post(url, json=payload, timeout=10)
        response.raise_for_status()
        logger.info("Telegram message sent successfully")
    except RequestException as e:
        logger.error(f"Failed to send Telegram message: {str(e)}")
    except Exception as e:
        logger.error(f"Unexpected error when sending Telegram message: {str(e)}")
