import requests
import logging

logging.basicConfig(
    level=logging.INFO,
    format='[%(asctime)s] {%(filename)s:%(lineno)d} %(levelname)s - %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
)
from typing import Optional
import pandas as pd

session = requests.Session()

def get_html_content(url: str) -> Optional[str]:
    try:
        response = session.get(url, timeout=10)
        response.raise_for_status()
        return response.text
    except requests.RequestException as e:
        logging.error(f"Failed to retrieve HTML content from {url}: {e}")
        return None


def add_html_content_to_df(df: pd.DataFrame) -> pd.DataFrame:
    total_urls = len(df)
    html_contents = []
    for i, row in df.iterrows():
        url = row['url']
        logging.info(f"Processing URL {i+1}/{total_urls}: {url}")
        html_content = get_html_content(url)
        html_contents.append(html_content)
    
    df['html_content'] = html_contents
    return df