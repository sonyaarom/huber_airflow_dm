from bs4 import BeautifulSoup
import re
import pandas as pd
import logging

logger = logging.getLogger(__name__)

def extract_info(html_content):
    if not html_content:
        # Handle None or empty html_content
        return {
            "title": "Title not found",
            "content": "Main content not found"
        }
    
    soup = BeautifulSoup(html_content, 'html.parser')
    
    # Extract title
    title = soup.find('h2', class_='documentFirstHeading')
    if not title:
        title = soup.find('h2')
    title_text = title.text.strip() if title else "Title not found"
    
    # Extract main content
    main_content = soup.find('div', id='parent-fieldname-text')
    if not main_content:
        main_content = soup.find('div', id='content-core')
    
    if main_content:
        # Remove script tags
        for script in main_content(["script", "style"]):
            script.decompose()
        
        # Get text and remove extra whitespace
        content_text = re.sub(r'\s+', ' ', main_content.get_text().strip())
    else:
        content_text = "Main content not found"
    
    return {
        "title": title_text,
        "content": title_text + " " + content_text
    }

def add_extracted_content_to_df(df: pd.DataFrame) -> pd.DataFrame:
    extracted_data = df['html_content'].apply(extract_info)
    
    df['extracted_title'] = extracted_data.apply(lambda x: x['title'])
    df['extracted_content'] = extracted_data.apply(lambda x: x['content'])
    
    return df

def combine_files(df_new, df_existing, id_column):
    logger.info(f"Combining files. New data shape: {df_new.shape}, Existing data shape: {df_existing.shape}")

    # Convert id column to string in both dataframes
    df_new[id_column] = df_new[id_column].astype(str)
    df_existing[id_column] = df_existing[id_column].astype(str)

    # Ensure all columns from df_new are in df_existing
    new_columns = set(df_new.columns) - set(df_existing.columns)
    if new_columns:
        logger.info(f"Adding new columns to existing data: {new_columns}")
        for col in new_columns:
            df_existing[col] = None

    # Create a set of existing IDs for faster lookup
    existing_ids = set(df_existing[id_column])

    # Identify updated and new rows
    updated_mask = df_new[id_column].isin(existing_ids)
    new_mask = ~updated_mask

    num_updated = updated_mask.sum()
    num_new = new_mask.sum()

    logger.info(f"Number of rows to update: {num_updated}")
    logger.info(f"Number of new rows to append: {num_new}")

    # Update existing rows
    if num_updated > 0:
        df_existing = df_existing.set_index(id_column)
        df_existing.update(df_new[updated_mask].set_index(id_column))
        df_existing = df_existing.reset_index()

    # Append new rows
    if num_new > 0:
        df_combined = pd.concat([df_existing, df_new[new_mask]], ignore_index=True)
    else:
        df_combined = df_existing

    # Ensure all columns from df_existing are in the final df_combined
    missing_columns = set(df_existing.columns) - set(df_combined.columns)
    if missing_columns:
        logger.info(f"Adding missing columns to combined data: {missing_columns}")
        for col in missing_columns:
            df_combined[col] = df_existing[col]

    logger.info(f"Combined data shape: {df_combined.shape}")

    return df_combined
