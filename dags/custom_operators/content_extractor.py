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
    
    # Identify new and updated rows
    updated_mask = df_existing[id_column].isin(df_new[id_column])
    num_updated = updated_mask.sum()
    logger.info(f"Number of rows to update: {num_updated}")
    
    # Update existing rows only for columns present in df_new
    columns_to_update = df_new.columns.intersection(df_existing.columns)
    df_existing.loc[updated_mask, columns_to_update] = df_new[df_new[id_column].isin(df_existing[id_column])][columns_to_update]
    
    # Append new rows
    new_rows = df_new[~df_new[id_column].isin(df_existing[id_column])]
    num_new = len(new_rows)
    logger.info(f"Number of new rows to append: {num_new}")
    
    df_combined = pd.concat([df_existing, new_rows], ignore_index=True)
    
    # Ensure all columns from df_existing are in the final df_combined
    missing_columns = set(df_existing.columns) - set(df_combined.columns)
    if missing_columns:
        logger.info(f"Adding missing columns to combined data: {missing_columns}")
        for col in missing_columns:
            df_combined[col] = df_existing[col]
    
    logger.info(f"Combined data shape: {df_combined.shape}")
    
    return df_combined
