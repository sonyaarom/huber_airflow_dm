from bs4 import BeautifulSoup
import re
import pandas as pd

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

def combine_files(new_df, old_df, id_column):
    """
    Combines two DataFrames based on a common ID column. If the IDs match, the rows from the new DataFrame are kept.
    
    Args:
    new_df (pd.DataFrame): DataFrame containing the new data.
    old_df (pd.DataFrame): DataFrame containing the old data.
    id_column (str): The name of the column that contains the IDs to match on.
    
    Returns:
    pd.DataFrame: The resulting combined DataFrame.
    """
    
    # Merge the old and new DataFrames, keeping rows from the new DataFrame
    combined_df = pd.merge(new_df, old_df, on=id_column, how='outer', suffixes=('', '_old'))
    
    # For matching IDs, use the data from the new DataFrame
    for col in new_df.columns:
        if col != id_column:
            combined_df[col] = combined_df[col].fillna(combined_df[f'{col}_old'])
    
    # Drop any columns that were duplicated (from the old DataFrame)
    columns_to_drop = [col for col in combined_df.columns if col.endswith('_old')]
    combined_df.drop(columns=columns_to_drop, inplace=True)
    
    return combined_df
