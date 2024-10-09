from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from datetime import datetime, timedelta
import logging
import json
from typing import Dict, Any, List
import numpy as np
import requests
# Logging setup
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
BUCKET_NAME = 'huber-chatbot-project'
UNIQUE_IDS_KEY = 'unique_ids/unique_ids.json'
UNIQUE_IDS_PREFIX = 'unique_ids/'
ENRICHED_DATA_PREFIX = 'enriched_data/'
EMBEDDINGS_PREFIX = 'embeddings/'
BM25_S3_PREFIX = 'bm25/'
EMBEDDINGS_FILE_NAME = 'recursive-vectors-256chunksize-all-MiniLM-L6-v2-sparse.json'
TMP_S3_PREFIX = 'tmp/'
PROCESSED_S3_PREFIX = 'processed_triggers/'

# Telegram setup
TELEGRAM_BOT_TOKEN = Variable.get("TELEGRAM_BOT_TOKEN", default_var=None)
TELEGRAM_CHAT_ID = Variable.get("TELEGRAM_CHAT_ID", default_var=None)

# DAG definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'pinecone_update_and_notify_pipeline',
    default_args=default_args,
    description='A DAG to update Pinecone based on sitemap changes and send notifications',
    schedule_interval=None,
    catchup=False
)


def numpy_to_python(data):
    if isinstance(data, dict):
        return {key: numpy_to_python(value) for key, value in data.items()}
    elif isinstance(data, list):
        return [numpy_to_python(element) for element in data]
    elif isinstance(data, np.ndarray):
        return data.tolist()
    elif isinstance(data, np.generic):  # For scalar numpy types
        return data.item()
    else:
        return data


# Helper functions
def combine_added_and_modified_data(data: Dict[str, Any]) -> Dict[str, Any]:
    if not isinstance(data, dict):
        raise TypeError("Input must be a dictionary")

    combined_data = {}
    for category in ['added', 'modified']:
        if category in data:
            combined_data.update(data[category])
            logger.info(f"Added {len(data[category])} items from '{category}' data")

    if not combined_data:
        logger.warning("Neither 'added' nor 'modified' keys found in the input data")
        raise ValueError("Input dictionary must contain either 'added' or 'modified' key")

    return combined_data

def find_all_matching_unique_ids(unique_id_list, id_list):
    return [unique_id for unique_id in unique_id_list if any(unique_id.startswith(id) for id in id_list)]

def send_telegram_message(message):
    from requests.exceptions import RequestException
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

# Task functions
def retrieve_trigger_data(**kwargs):
    trigger_file_s3_key = kwargs['dag_run'].conf.get('trigger_file_s3_key')
    if not trigger_file_s3_key:
        raise ValueError("No trigger file S3 key provided")

    s3_hook = S3Hook(aws_conn_id='aws_default')
    trigger_data = s3_hook.read_key(trigger_file_s3_key, BUCKET_NAME)
    changes = json.loads(trigger_data)
    
    updated_ids = set()
    for category in ['removed', 'modified']:
        if category in changes:
            updated_ids.update(changes[category].keys())
    
    return {'updated_ids': list(updated_ids), 'changes': changes}


def get_unique_ids():
    s3_hook = S3Hook(aws_conn_id='aws_default')
    unique_ids_data = s3_hook.read_key(UNIQUE_IDS_KEY, BUCKET_NAME)
    return json.loads(unique_ids_data)

### PART 1: deleting old  and modified data

def process_ids_for_deletion(**kwargs):
    import pandas as pd
    from io import StringIO
    from custom_operators.pinecone_func import get_pinecone_credentials, initialize_pinecone
    ti = kwargs['ti']
    trigger_data = ti.xcom_pull(task_ids='retrieve_trigger_data')
    updated_ids = trigger_data['updated_ids']
    unique_ids = ti.xcom_pull(task_ids='get_unique_ids')
    
    ids_to_delete = find_all_matching_unique_ids(unique_ids, updated_ids)
    
    logger.info(f"Number of IDs to delete: {len(ids_to_delete)}")
    logger.info(f"IDs to delete: {ids_to_delete}")
    
    # Deleting IDs from Pinecone
    api_key, environment, host, index_name = get_pinecone_credentials()
    pc = initialize_pinecone(api_key, environment)
    index = pc.Index(index_name, host=host)
    successful_deletions = 0
    for id in ids_to_delete:
        try:
            delete_result = index.delete(ids=[id])
            logger.info(f"Deletion result for ID {id}: {delete_result}")
            if delete_result:
                successful_deletions += 1
                logger.info(f"Processed deletion for ID {id}")
            else:
                logger.warning(f"Unexpected result when deleting ID {id} from Pinecone")
        except Exception as e:
            logger.error(f"Error deleting ID {id} from Pinecone: {str(e)}")
    
    logger.info(f"Processed deletion for {successful_deletions} out of {len(ids_to_delete)} IDs from Pinecone")

    s3_hook = S3Hook(aws_conn_id='aws_default')
    BUCKET = 'huber-chatbot-project'
    EMBEDDINGS_PREFIX = 'embeddings/'
    ENRICHED_PREFIX = 'enriched_data/'

    def remove_unique_ids_from_embeddings(unique_ids_to_remove):
        deleted_unique_ids = []
        try:
            keys = s3_hook.list_keys(bucket_name=BUCKET, prefix=EMBEDDINGS_PREFIX)
            for key in keys:
                if key.endswith('.json'):
                    try:
                        file_content = s3_hook.read_key(key, bucket_name=BUCKET)
                        enriched_data = json.loads(file_content)
                        
                        original_length = len(enriched_data)
                        new_enriched_data = [item for item in enriched_data if item.get('unique_id') not in unique_ids_to_remove]
                        removed_count = original_length - len(new_enriched_data)
                        
                        if removed_count > 0:
                            s3_hook.load_string(json.dumps(new_enriched_data), key, bucket_name=BUCKET, replace=True)
                            logger.info(f"Updated embeddings file {key}, removed {removed_count} items")
                            
                            removed_ids = [item['unique_id'] for item in enriched_data if item.get('unique_id') in unique_ids_to_remove]
                            deleted_unique_ids.extend(removed_ids)
                            logger.info(f"Removed IDs from embeddings file {key}: {removed_ids}")
                    
                    except Exception as e:
                        logger.error(f"Error processing embeddings file {key}: {str(e)}")
        except Exception as e:
            logger.error(f"Failed to list objects in embeddings folder: {e}")
        
        logger.info(f"Total unique IDs found and deleted from embeddings: {deleted_unique_ids}")
        return deleted_unique_ids

    def remove_modified_ids_from_enriched(unique_ids_to_remove):
        total_removed = 0
        try:
            keys = s3_hook.list_keys(bucket_name=BUCKET, prefix=ENRICHED_PREFIX)
            for key in keys:
                if key.endswith('.csv'):
                    try:
                        file_content = s3_hook.read_key(key, bucket_name=BUCKET)
                        enriched_data = pd.read_csv(StringIO(file_content))

                        original_length = len(enriched_data)
                        enriched_data = enriched_data[~enriched_data['id'].isin(unique_ids_to_remove)]
                        removed_count = original_length - len(enriched_data)

                        if removed_count > 0:
                            csv_buffer = StringIO()
                            enriched_data.to_csv(csv_buffer, index=False)
                            s3_hook.load_string(csv_buffer.getvalue(), key, bucket_name=BUCKET, replace=True)
                            logger.info(f"Updated enriched file {key}, removed {removed_count} items")
                            total_removed += removed_count

                    except Exception as e:
                        logger.error(f"Error processing enriched file {key}: {str(e)}")
        except Exception as e:
            logger.error(f"Failed to list objects in enriched folder: {e}")
        logger.info(f"Total items removed from enriched data across all files: {total_removed}")

    def update_unique_ids_list(unique_ids_to_remove):
        total_removed_uid = 0
        try:
            keys = s3_hook.list_keys(bucket_name=BUCKET, prefix=UNIQUE_IDS_PREFIX)
            for key in keys:
                if key.endswith('.json'):
                    try:
                        file_content = s3_hook.read_key(key, bucket_name=BUCKET)
                        unique_ids = json.loads(file_content)
                        original_length = len(unique_ids)
                        unique_ids = [uid for uid in unique_ids if uid not in unique_ids_to_remove]
                        removed_count = original_length - len(unique_ids)
                        if removed_count > 0:
                            s3_hook.load_string(json.dumps(unique_ids), key, bucket_name=BUCKET, replace=True)
                            logger.info(f"Updated unique IDs list {key}, removed {removed_count} items")
                            total_removed_uid += removed_count
                    except Exception as e:
                        logger.error(f"Error processing unique IDs file {key}: {str(e)}")
        except Exception as e:
            logger.error(f"Failed to list objects in unique IDs folder: {e}")
        logger.info(f"Total unique IDs removed: {total_removed_uid}")
        return total_removed_uid
                

    # Process embeddings
    deleted_from_embeddings = remove_unique_ids_from_embeddings(ids_to_delete)
    
    # Process enriched data
    removed_from_enriched = remove_modified_ids_from_enriched(updated_ids)

    # Process unique IDs
    removed_from_unique_ids = update_unique_ids_list(ids_to_delete)
    
    return {
        'deleted_from_pinecone': successful_deletions,
        'deleted_from_embeddings': deleted_from_embeddings,
        'removed_from_enriched': removed_from_enriched,
        'removed_from_unique_ids': removed_from_unique_ids
    }


def process_and_notify(**kwargs):
    ti = kwargs['ti']
    deletion_results = ti.xcom_pull(task_ids='process_ids_for_deletion')
    
    deleted_from_pinecone = deletion_results['deleted_from_pinecone']
    deleted_from_embeddings = deletion_results['deleted_from_embeddings']
    removed_from_enriched = deletion_results['removed_from_enriched']
    removed_from_unique_ids = deletion_results['removed_from_unique_ids']
    
    message = (f"*Update Notification*\n\n"
               f"Number of unique IDs deleted from Pinecone: {deleted_from_pinecone}\n"
               f"Number of unique IDs deleted from embeddings: {len(deleted_from_embeddings)}\n"
               f"Number of items removed from enriched data: {removed_from_enriched}\n"
               f"Number of unique IDs removed from unique IDs list: {removed_from_unique_ids}")
    
    send_telegram_message(message)
    logger.info(f"Processed deletions and sent Telegram notification.")


###PART2: uploading new data
#task that checks trigger fil;e and combines modified and added entries


def combine_added_and_modified_data(**kwargs):
    ti = kwargs['ti']
    trigger_data = ti.xcom_pull(task_ids='retrieve_trigger_data')
    changes = trigger_data['changes']

    combined_data = {}
    for category in ['added', 'modified']:
        if category in changes:
            combined_data.update(changes[category])
            logger.info(f"Added {len(changes[category])} items from '{category}' data")

    if not combined_data:
        logger.warning("Neither 'added' nor 'modified' keys found in the input data")
        return None

    return combined_data

def enrich_data(**kwargs):
    import pandas as pd
    from io import StringIO
    from custom_operators.web_utils import add_html_content_to_df
    from custom_operators.content_extractor import add_extracted_content_to_df, combine_files
    
    ti = kwargs['ti']
    combined_data = ti.xcom_pull(task_ids='combine_added_and_modified_data')

    if not combined_data:
        logger.warning("No data to enrich")
        return None

    df_new = pd.DataFrame.from_dict(combined_data, orient='index').reset_index()
    df_new.columns = ['id', 'url', 'last_updated']
    logger.info(f"New data to enrich: {len(df_new)} rows")
    logger.info(f"New data columns: {df_new.columns.tolist()}")
    logger.info(f"New data sample: \n{df_new.head().to_string()}")

    # Add HTML content to the DataFrame
    df_new = add_html_content_to_df(df_new)

    # Drop rows with null html_content
    df_new = df_new.dropna(subset=['html_content'])
    logger.info(f"Rows after dropping null html_content: {len(df_new)}")

    # Extract and add content to the DataFrame
    df_new = add_extracted_content_to_df(df_new)

    # Optionally, drop rows with missing extracted content
    df_new = df_new[df_new['extracted_content'] != "Title not found Main content not found"]
    logger.info(f"Rows after filtering extracted content: {len(df_new)}")
    logger.info(f"Enriched new data columns: {df_new.columns.tolist()}")
    logger.info(f"Enriched new data sample: \n{df_new.head().to_string()}")

    # Setup S3 hook and check for existing file
    s3_hook = S3Hook(aws_conn_id='aws_default')
    existing_file_key = f'{ENRICHED_DATA_PREFIX}enriched_data_{datetime.now().strftime("%Y")}.csv'
    
    try:
        # Fetch existing file from S3
        existing_file_obj = s3_hook.get_key(existing_file_key, bucket_name=BUCKET_NAME)
        
        if existing_file_obj is None:
            logger.info("No existing file found in S3. Using new data as the main file.")
            df_combined = df_new
        else:
            # Read existing file
            existing_file_content = existing_file_obj.get()['Body'].read().decode('utf-8')
            df_existing = pd.read_csv(StringIO(existing_file_content))
            logger.info(f"Successfully retrieved the existing file from S3. Rows: {len(df_existing)}")
            logger.info(f"Existing data columns: {df_existing.columns.tolist()}")
            logger.info(f"Existing data sample: \n{df_existing.head().to_string()}")

            # Combine new data with existing data
            df_combined = combine_files(df_new, df_existing, 'id')
            logger.info("New and existing data combined.")
            logger.info(f"Combined data rows: {len(df_combined)}")
            logger.info(f"Combined data columns: {df_combined.columns.tolist()}")
            logger.info(f"Combined data sample: \n{df_combined.head().to_string()}")
            
            # Check for NaN values
            nan_count = df_combined.isna().sum().sum()
            if nan_count > 0:
                logger.warning(f"Found {nan_count} NaN values in the combined data.")
                logger.warning(f"Columns with NaN: {df_combined.columns[df_combined.isna().any()].tolist()}")
                
                # Fill NaN values with a placeholder or drop rows with NaN
                df_combined = df_combined.dropna(subset=['html_content', 'extracted_content'])
                logger.info(f"Rows after dropping NaN: {len(df_combined)}")

    except Exception as e:
        logger.error(f"Error processing existing file: {str(e)}")
        logger.exception("Full traceback:")
        # If there's an error, treat the new data as the main file
        df_combined = df_new
        logger.info("Error occurred, using new data as the main file.")

    # Save combined (or new) enriched data to S3
    enriched_file_path = existing_file_key
    
    csv_buffer = StringIO()
    df_combined.to_csv(csv_buffer, index=False)
    s3_hook.load_string(csv_buffer.getvalue(), enriched_file_path, bucket_name=BUCKET_NAME, replace=True)

    logger.info(f"Enriched data saved to S3: {enriched_file_path}")

    # Save only the new or updated data for the next task
    new_data_file_name = f'new_enriched_data_{datetime.now().strftime("%Y%m")}.csv'
    new_data_file_path = f'{TMP_S3_PREFIX}{new_data_file_name}'
    
    csv_buffer = StringIO()
    df_new.to_csv(csv_buffer, index=False)
    s3_hook.load_string(csv_buffer.getvalue(), new_data_file_path, bucket_name=BUCKET_NAME, replace=True)

    logger.info(f"New enriched data saved to S3: {new_data_file_path}")
    kwargs['ti'].xcom_push(key='new_enriched_file_path', value=new_data_file_path)
    
    return {
        'enriched_file_path': enriched_file_path,
        'new_enriched_file_path': new_data_file_path,
        'num_enriched_items': len(df_combined),
        'num_new_items': len(df_new)
    }

def recursive_chunking_and_embedding_task(**kwargs):
    import pandas as pd
    import numpy as np
    import gc
    from collections import defaultdict
    from tqdm import tqdm
    from sentence_transformers import SentenceTransformer

    # Import custom functions
    from langchain_text_splitters import RecursiveCharacterTextSplitter
    from custom_operators.shared_utils import (
        apply_bm25_sparse_vectors,
        embed_dataframe,
        generate_documents,
        save_documents_to_json,
        create_and_save_bm25_corpus,
        get_overlap
    )

    # Retrieve the task instance to use XCom
    ti = kwargs['ti']
    enriched_file_path = ti.xcom_pull(key='enriched_file_path', task_ids='enrich_data')

    logger.info(f"Loading enriched data from: {enriched_file_path}")

    # Read the DataFrame from the CSV file
    df = pd.read_csv(enriched_file_path)
    #remove!
    df = df.head(15)

    # Initialize models inside the function
    embed_model = SentenceTransformer('all-MiniLM-L6-v2', trust_remote_code=True)
    embed_model_name = 'all-MiniLM-L6-v2'

    # Define base paths
    base_path = '/opt/airflow/dags/embeddings'

    # Ensure the directories exist
    os.makedirs(base_path, exist_ok=True)

    chunk_lengths = [256]  # Adjust as needed
    doc_type = 'recursive'  # Set doc_type to match your use case

    # Create BM25 directory if it doesn't exist
    os.makedirs(BM25_DIR, exist_ok=True)

    bm25_file_path = os.path.join(BM25_DIR, 'bm25_values.json')
    if not os.path.exists(bm25_file_path):
        logger.info(f"Creating BM25 corpus at: {bm25_file_path}")
        bm25_values = create_and_save_bm25_corpus(df, 'extracted_content', bm25_file_path)
    else:
        logger.info(f"Loading BM25 corpus from: {bm25_file_path}")
        with open(bm25_file_path, 'r') as f:
            bm25_values = json.load(f)

    # List to hold embeddings file paths
    embeddings_file_paths = []

    # Process data for each chunk length
    chunk_stats = []
    for chunk_length in chunk_lengths:
        logger.info(f"Processing recursive chunks with max length: {chunk_length}")

        text_splitter = RecursiveCharacterTextSplitter(
            chunk_size=chunk_length,
            chunk_overlap=get_overlap(chunk_length),
            length_function=len,
            is_separator_regex=False,
        )

        all_chunks = []
        unique_ids = []
        general_ids = []
        urls = []
        last_updateds = []
        html_contents = []

        # Chunk the texts
        for idx, row in tqdm(df.iterrows(), total=len(df), desc="Chunking texts"):
            text = row.get('extracted_content', '')
            if isinstance(text, str):
                chunks = text_splitter.split_text(text)
                all_chunks.extend(chunks)
                unique_ids.extend([f"{row['id']}_{i+1}" for i in range(len(chunks))])
                general_ids.extend([row['id']] * len(chunks))
                urls.extend([row['url']] * len(chunks))
                last_updateds.extend([row.get('last_updated', '')] * len(chunks))
                html_contents.extend([row.get('html_content', '')] * len(chunks))

        # Create a DataFrame for the chunks
        chunked_df = pd.DataFrame({
            'unique_id': unique_ids,
            'url': urls,
            'last_updated': last_updateds,
            'html_content': html_contents,
            'text': all_chunks,
            'len': [len(chunk) for chunk in all_chunks],
            'general_id': general_ids,
        })

        # Filter out short chunks
        chunked_df = chunked_df[chunked_df['len'] >= 50]

        logger.info(f"Created {len(chunked_df)} chunks")

        if len(chunked_df) == 0:
            logger.warning(f"No chunks created for chunk_length {chunk_length}. Skipping.")
            continue

        # Compute statistics
        min_length = chunked_df['len'].min()
        max_length = chunked_df['len'].max()
        mean_length = chunked_df['len'].mean()
        median_length = chunked_df['len'].median()
        chunk_stats.append((chunk_length, min_length, max_length, mean_length, median_length))

        logger.info(f"Chunk length statistics for target length {chunk_length}:")
        logger.info(f"  Minimum length: {min_length}")
        logger.info(f"  Maximum length: {max_length}")
        logger.info(f"  Mean length: {mean_length:.2f}")
        logger.info(f"  Median length: {median_length:.2f}")

        # Apply BM25 sparse vectorization
        logger.info("Applying BM25 sparse vectorization")
        try:
            chunked_df = apply_bm25_sparse_vectors(chunked_df, 'text', bm25_values)
        except Exception as e:
            logger.error(f"Error applying BM25 sparse vectors: {str(e)}")
            continue

        # Embed the chunked texts
        logger.info("Embedding chunked texts")
        embedded_df = embed_dataframe(chunked_df, embed_model)

        # Generate document dictionaries
        logger.info("Generating document dictionaries")
        documents = generate_documents(embedded_df, chunk_length, doc_type)

        # Save documents to JSON
        logger.info("Saving documents to JSON")
        save_documents_to_json(documents, chunk_length, embed_model_name, doc_type, base_path)

        # Construct the embeddings file name using the same logic as save_documents_to_json
        filename = f"{doc_type}-vectors-{chunk_length}chunksize-{embed_model_name}-sparse.json"
        embeddings_file_path = os.path.join(base_path, filename)
        logger.info(f"Embeddings file saved to: {embeddings_file_path}")

        # Append the embeddings file path to the list
        embeddings_file_paths.append(embeddings_file_path)

        # Clean up to free memory
        del chunked_df, embedded_df, documents
        gc.collect()

        logger.info(f"Finished processing chunks of max length {chunk_length}.")

    # Push the embeddings file paths to XCom for the next task
    ti.xcom_push(key='embeddings_file_paths', value=embeddings_file_paths)
    logger.info(f"Embeddings file paths pushed to XCom: {embeddings_file_paths}")

    # Optionally, return the chunk statistics
    return chunk_stats

#right now it handles only unique ids, but should also update embeddings and enriched data

def update_storage(**kwargs):
    from custom_operators.pinecone_func import get_pinecone_credentials, initialize_pinecone, upload_to_pinecone
    ti = kwargs['ti']
    embedding_results = ti.xcom_pull(task_ids='embed_and_update_s3')

    logger.info(f"Received embedding results: {embedding_results}")

    if not embedding_results:
        logger.warning("No embedding results received")
        return None

    new_documents = ti.xcom_pull(key='new_documents', task_ids='embed_and_update_s3')

    if not new_documents:
        logger.warning("No new documents to update in Pinecone")
        return None

    # Update Pinecone
    api_key, environment, host, index_name = get_pinecone_credentials()
    
    logger.info(f"Initializing Pinecone with API key: {api_key[:5]}..., environment: {environment}, host: {host}")
    
    pc = initialize_pinecone(api_key, environment)
    
    logger.info(f"Attempting to access index: {index_name}")
    index = pc.Index(index_name, host=host)

    try:
        stats = index.describe_index_stats()
        logger.info(f"Successfully connected to index. Stats: {stats}")
    except Exception as e:
        logger.error(f"Error accessing index: {str(e)}")
        raise

    # Get Pinecone configuration from Airflow Variables
    project_name = Variable.get("PINECONE_PROJECT_NAME", default_var='huber-chatbot-project')
    metric = Variable.get("PINECONE_METRIC", default_var='cosine')

    # Prepare documents for upload
    chunk_size = 256  # Adjust this if you're using different chunk sizes
    documents_dict = {chunk_size: new_documents}

    # Upload to Pinecone
    upload_to_pinecone(
        api_key=api_key,
        documents=documents_dict,
        index_name=index_name,
        project_name=project_name,
        metric=metric,
        host=host
    )

    logger.info(f"Completed upload of {len(new_documents)} new documents to Pinecone")

    # Update unique IDs list
    s3_hook = S3Hook(aws_conn_id='aws_default')
    
    # Read the embeddings file
    embeddings_key = f'{EMBEDDINGS_PREFIX}{EMBEDDINGS_FILE_NAME}'
    try:
        embeddings_data = s3_hook.read_key(embeddings_key, BUCKET_NAME)
        embeddings = json.loads(embeddings_data)
        logger.info(f"Successfully read embeddings file: {embeddings_key}")
    except Exception as e:
        logger.error(f"Error reading embeddings file: {str(e)}")
        embeddings = []

    # Extract all unique IDs from the embeddings
    all_unique_ids = list(set(doc.get('unique_id') for doc in embeddings if 'unique_id' in doc))
    logger.info(f"Extracted {len(all_unique_ids)} unique IDs from embeddings")

    # Read the existing unique IDs file
    try:
        existing_unique_ids_data = s3_hook.read_key(UNIQUE_IDS_KEY, BUCKET_NAME)
        existing_unique_ids = set(json.loads(existing_unique_ids_data))
        logger.info(f"Read {len(existing_unique_ids)} existing unique IDs")
    except Exception as e:
        logger.warning(f"Error reading existing unique IDs file: {str(e)}. Starting with empty set.")
        existing_unique_ids = set()

    # Combine existing and new unique IDs
    updated_unique_ids = list(existing_unique_ids.union(all_unique_ids))
    
    # Write the updated unique IDs back to S3
    try:
        s3_hook.load_string(json.dumps(updated_unique_ids), UNIQUE_IDS_KEY, bucket_name=BUCKET_NAME, replace=True)
        logger.info(f"Successfully updated unique IDs file with {len(updated_unique_ids)} IDs")
    except Exception as e:
        logger.error(f"Error writing updated unique IDs file: {str(e)}")

    return {
        'num_updated_items': len(new_documents),
        'num_unique_ids': len(updated_unique_ids)
    }

def final_notification(**kwargs):
    ti = kwargs['ti']
    deletion_results = ti.xcom_pull(task_ids='process_ids_for_deletion')
    update_results = ti.xcom_pull(task_ids='update_storage')
    
    deleted_from_pinecone = deletion_results['deleted_from_pinecone']
    deleted_from_embeddings = deletion_results['deleted_from_embeddings']
    removed_from_enriched = deletion_results['removed_from_enriched']
    removed_from_unique_ids = deletion_results['removed_from_unique_ids']
    
    num_updated_items = update_results['num_updated_items'] if update_results else 0
    
    message = (f"*Update Notification*\n\n"
               f"Number of unique IDs deleted from Pinecone: {deleted_from_pinecone}\n"
               f"Number of unique IDs deleted from embeddings: {len(deleted_from_embeddings)}\n"
               f"Number of items removed from enriched data: {removed_from_enriched}\n"
               f"Number of unique IDs removed from unique IDs list: {removed_from_unique_ids}\n"
               f"Number of items added or updated: {num_updated_items}")
    
    send_telegram_message(message)
    logger.info(f"Processed deletions, additions, and modifications, and sent Telegram notification.")

class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, np.integer):
            return int(obj)
        if isinstance(obj, np.floating):
            return float(obj)
        return super(NumpyEncoder, self).default(obj)

def embed_and_update_s3(**kwargs):
    import pandas as pd
    from io import StringIO
    from sentence_transformers import SentenceTransformer
    from custom_operators.shared_utils import (
        apply_bm25_sparse_vectors,
        embed_dataframe,
        generate_documents,
        create_and_save_bm25_corpus,
        get_overlap
    )
    from langchain_text_splitters import RecursiveCharacterTextSplitter

    logger.info("Starting embed_and_update_s3 function")

    ti = kwargs['ti']
    enriched_data_info = ti.xcom_pull(task_ids='enrich_data')
    
    if not enriched_data_info or 'new_enriched_file_path' not in enriched_data_info:
        logger.warning("No new enriched data to embed")
        return None

    s3_hook = S3Hook(aws_conn_id='aws_default')
    new_enriched_file_content = s3_hook.read_key(enriched_data_info['new_enriched_file_path'], BUCKET_NAME)
    df = pd.read_csv(StringIO(new_enriched_file_content))
    logger.info(f"Loaded new enriched data: {len(df)} rows")

    # Initialize embedding model
    embed_model = SentenceTransformer('all-MiniLM-L6-v2', trust_remote_code=True)
    logger.info("Initialized SentenceTransformer model")
    
    # Create chunks
    text_splitter = RecursiveCharacterTextSplitter(
        chunk_size=256,
        chunk_overlap=get_overlap(256),
        length_function=len,
        is_separator_regex=False,
    )

    all_chunks = []
    unique_ids = []
    general_ids = []
    urls = []
    last_updateds = []
    html_contents = []

    for idx, row in df.iterrows():
        text = row.get('extracted_content', '')
        if isinstance(text, str):
            chunks = text_splitter.split_text(text)
            all_chunks.extend(chunks)
            unique_ids.extend([f"{row['id']}_{i+1}" for i in range(len(chunks))])
            general_ids.extend([row['id']] * len(chunks))
            urls.extend([row['url']] * len(chunks))
            last_updateds.extend([row.get('last_updated', '')] * len(chunks))
            html_contents.extend([row.get('html_content', '')] * len(chunks))

    chunked_df = pd.DataFrame({
        'unique_id': unique_ids,
        'url': urls,
        'last_updated': last_updateds,
        'html_content': html_contents,
        'text': all_chunks,
        'len': [len(chunk) for chunk in all_chunks],
        'general_id': general_ids,
    })

    chunked_df = chunked_df[chunked_df['len'] >= 50]
    logger.info(f"Created {len(chunked_df)} chunks after filtering")

    # Apply BM25 sparse vectorization
    bm25_file_key = f'{BM25_S3_PREFIX}bm25_values.json'
    try:
        bm25_values = json.loads(s3_hook.read_key(bm25_file_key, BUCKET_NAME))
        logger.info("Loaded existing BM25 values from S3")
    except Exception as e:
        logger.info(f"Creating new BM25 corpus and saving to S3: {bm25_file_key}")
        bm25_values = create_and_save_bm25_corpus(df, 'extracted_content', None)
        s3_hook.load_string(json.dumps(bm25_values), bm25_file_key, bucket_name=BUCKET_NAME, replace=True)
    
    chunked_df = apply_bm25_sparse_vectors(chunked_df, 'text', bm25_values)
    logger.info("Applied BM25 sparse vectorization")

    # Embed the chunked texts
    embedded_df = embed_dataframe(chunked_df, embed_model)
    logger.info("Completed embedding of chunked texts")

    # Generate document dictionaries
    new_documents = generate_documents(embedded_df, 256, 'recursive')
    logger.info(f"Generated {len(new_documents)} new document dictionaries")

    # Update or create the JSON file in S3
    embeddings_key = f'{EMBEDDINGS_PREFIX}{EMBEDDINGS_FILE_NAME}'
    
    # Read existing embeddings file
    try:
        existing_embeddings_str = s3_hook.read_key(embeddings_key, BUCKET_NAME)
        existing_embeddings = json.loads(existing_embeddings_str) if existing_embeddings_str else []
        logger.info(f"Loaded existing embeddings from S3: {len(existing_embeddings)} documents")
    except Exception as e:
        logger.warning(f"Error reading existing embeddings: {str(e)}. Starting with empty list.")
        existing_embeddings = []

    # Update existing documents and add new ones
    updated_count = 0
    added_count = 0
    existing_ids = {doc['unique_id']: i for i, doc in enumerate(existing_embeddings)}
    
    for doc in new_documents:
        if doc['unique_id'] in existing_ids:
            existing_embeddings[existing_ids[doc['unique_id']]] = doc
            updated_count += 1
        else:
            existing_embeddings.append(doc)
            existing_ids[doc['unique_id']] = len(existing_embeddings) - 1
            added_count += 1

    logger.info(f"Updated {updated_count} existing documents and added {added_count} new documents")

    # Prepare JSON for upload
    embeddings_json = json.dumps(existing_embeddings, cls=NumpyEncoder)
    logger.info(f"Prepared JSON string for S3 upload: {len(embeddings_json)} characters")

    # Upload to S3
    s3_hook.load_string(
        embeddings_json,
        embeddings_key,
        bucket_name=BUCKET_NAME,
        replace=True
    )
    logger.info(f"Uploaded updated embeddings to S3: {embeddings_key}")
    
    logger.info(f"Total documents in updated embeddings file: {len(existing_embeddings)}")
    logger.info(f"Number of new or updated documents: {len(new_documents)}")

    ti.xcom_push(key='new_documents', value=numpy_to_python(new_documents))

    return {
        'embeddings_key': embeddings_key, 
        'num_new_embeddings': len(new_documents)
    }

def move_trigger_file(**kwargs):
    from datetime import datetime
    
    ti = kwargs['ti']
    dag_run = kwargs['dag_run']
    
    # Get the original trigger file S3 key
    trigger_file_s3_key = dag_run.conf.get('trigger_file_s3_key')
    if not trigger_file_s3_key:
        raise ValueError("No trigger file S3 key provided")
    
    s3_hook = S3Hook(aws_conn_id='aws_default')
    
    # Generate the new key for the processed trigger file
    current_date = datetime.now().strftime("%Y%m%d%H%M%S")
    new_key = f'{PROCESSED_S3_PREFIX}processed_trigger_{current_date}.json'
    
    try:
        # Copy the file to the new location
        s3_hook.copy_object(
            source_bucket_key=trigger_file_s3_key,
            dest_bucket_key=new_key,
            source_bucket_name=BUCKET_NAME,
            dest_bucket_name=BUCKET_NAME
        )
        logger.info(f"Successfully copied trigger file to {new_key}")
        
        # Delete the original file
        s3_hook.delete_objects(bucket=BUCKET_NAME, keys=[trigger_file_s3_key])
        logger.info(f"Successfully deleted original trigger file: {trigger_file_s3_key}")
    
    except Exception as e:
        logger.error(f"Error moving trigger file: {str(e)}")
        raise
    
    return {"processed_trigger_file": new_key}

retrieve_trigger_data_task = PythonOperator(
    task_id='retrieve_trigger_data',
    python_callable=retrieve_trigger_data,
    provide_context=True,
    dag=dag,
)

get_unique_ids_task = PythonOperator(
    task_id='get_unique_ids',
    python_callable=get_unique_ids,
    dag=dag,
)

process_ids_task = PythonOperator(
    task_id='process_ids_for_deletion',
    python_callable=process_ids_for_deletion,
    provide_context=True,
    dag=dag,
)

combine_data_task = PythonOperator(
    task_id='combine_added_and_modified_data',
    python_callable=combine_added_and_modified_data,
    provide_context=True,
    dag=dag,
)

enrich_data_task = PythonOperator(
    task_id='enrich_data',
    python_callable=enrich_data,
    provide_context=True,
    dag=dag,
)

embed_and_update_s3_task = PythonOperator(
    task_id='embed_and_update_s3',
    python_callable=embed_and_update_s3,
    provide_context=True,
    dag=dag,
)

update_storage_task = PythonOperator(
    task_id='update_storage',
    python_callable=update_storage,
    provide_context=True,
    dag=dag,
)

final_notification_task = PythonOperator(
    task_id='final_notification',
    python_callable=final_notification,
    provide_context=True,
    dag=dag,
)

move_trigger_file_task = PythonOperator(
    task_id='move_trigger_file',
    python_callable=move_trigger_file,
    provide_context=True,
    dag=dag,
)

# Dependencies
retrieve_trigger_data_task >> [get_unique_ids_task, combine_data_task]
get_unique_ids_task >> process_ids_task
[process_ids_task, combine_data_task] >> enrich_data_task
enrich_data_task >> embed_and_update_s3_task >> update_storage_task
[process_ids_task, update_storage_task] >> final_notification_task
final_notification_task >> move_trigger_file_task
