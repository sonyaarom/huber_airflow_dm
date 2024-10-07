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

def combine_files(new_file_path, old_file_path, id_column, output_file_path):
    """
    Combines two CSV files based on a common ID column. If the IDs match, the rows from the new file are kept.
    
    Args:
    new_file_path (str): Path to the new file (CSV format).
    old_file_path (str): Path to the old file (CSV format).
    id_column (str): The name of the column that contains the IDs to match on.
    output_file_path (str): Path to save the resulting combined file (CSV format).
    
    Returns:
    None: The function writes the combined DataFrame to a CSV file.
    """
    
    # Load the old and new files into dataframes
    new_df = pd.read_csv(new_file_path)
    old_df = pd.read_csv(old_file_path)
    
    # Merge the old and new files, keeping rows from the new file
    combined_df = pd.merge(new_df, old_df, on=id_column, how='left', suffixes=('', '_old'))
    
    # Drop any columns that were duplicated (from the old file) if you don't need them
    for col in combined_df.columns:
        if '_old' in col:
            combined_df.drop(columns=[col], inplace=True)
    
    # Save the resulting DataFrame to a new CSV file
    combined_df.to_csv(output_file_path, index=False)
