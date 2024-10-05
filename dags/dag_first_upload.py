from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.models import Variable  # For Airflow Variables
from airflow.hooks.base import BaseHook  # For Airflow Connections
from datetime import datetime, timedelta
import os
import json
import logging

# Initialize logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
BUCKET_NAME = 'huber-chatbot-project'
S3_KEY_PREFIX = f'sitemap_data/sitemap_data_{datetime.now().strftime("%Y")}.json'
ENRICHED_DATA_S3_PREFIX = f'enriched_data/enriched_data_{datetime.now().strftime("%Y%m")}.csv'
EMBEDDINGS_S3_PREFIX = 'embeddings/'
BM25_S3_PREFIX = 'bm25/'
ENRICHED_DATA_DIR = '/opt/airflow/dags/enriched'
EMBEDDINGS_DIR = '/opt/airflow/dags/embeddings'
BM25_DIR = '/opt/airflow/dags/bm25'

# Airflow default arguments
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# DAG definition
dag = DAG(
    'sitemap_retrieval_and_enrichment',
    default_args=default_args,
    description='A DAG for retrieving sitemaps and enriching data',
    schedule_interval=None,
    catchup=False
)

def process_sitemap_task(**kwargs):
    from custom_operators.sitemap_processor import process_sitemap

    url = 'https://www.wiwi.hu-berlin.de/sitemap.xml.gz'
    exclude_extensions = ['.jpg', '.pdf', '.jpeg', '.png']
    exclude_patterns = ['view']
    include_patterns = ['/en/']
    allowed_base_url = 'https://www.wiwi.hu-berlin.de'

    data_dict, total, filtered, safe, unsafe = process_sitemap(
        url, exclude_extensions, exclude_patterns, include_patterns, allowed_base_url
    )

    logger.info(f"Total entries: {total}")
    logger.info(f"Filtered entries: {filtered}")
    logger.info(f"Safe entries: {safe}")
    logger.info(f"Unsafe entries: {unsafe}")

    # Save the data_dict to a JSON file and pass the file path
    os.makedirs('/opt/airflow/dags/data', exist_ok=True)
    data_file_path = '/opt/airflow/dags/data/sitemap_data.json'
    with open(data_file_path, 'w') as f:
        json.dump(data_dict, f)
    kwargs['ti'].xcom_push(key='data_file_path', value=data_file_path)


def upload_to_s3_task(**kwargs):
    from airflow.hooks.S3_hook import S3Hook
    ti = kwargs['ti']
    data_file_path = ti.xcom_pull(key='data_file_path', task_ids='process_sitemap')

    s3_hook = S3Hook(aws_conn_id='aws_default')
    s3_hook.load_file(
        filename=data_file_path,
        key=S3_KEY_PREFIX,
        bucket_name=BUCKET_NAME,
        replace=True
    )
    logger.info(f"Data uploaded to S3 bucket: {BUCKET_NAME}")
    logger.info(f"S3 key: {S3_KEY_PREFIX}")

def enrich_data_task(**kwargs):
    import pandas as pd  # Moved inside the function
    ti = kwargs['ti']
    data_file_path = ti.xcom_pull(key='data_file_path', task_ids='process_sitemap')

    # Load the data
    with open(data_file_path, 'r') as f:
        data_dict = json.load(f)

    # Process the data
    df = pd.DataFrame.from_dict(data_dict, orient='index').reset_index()
    df.columns = ['id', 'url', 'last_updated']

    df = df.head(10)

    # Import inside the function
    from custom_operators.web_utils import add_html_content_to_df
    from custom_operators.content_extractor import add_extracted_content_to_df

    # Add HTML content to the DataFrame
    df = add_html_content_to_df(df)

    # Log number of None html_content entries
    num_none_html = df['html_content'].isnull().sum()
    logger.info(f"Number of entries with None html_content: {num_none_html}")

    # Extract and add content to the DataFrame
    df = add_extracted_content_to_df(df)

    # Optionally, drop rows with missing extracted content
    df = df[df['extracted_content'] != "Title not found Main content not found"]

    # Save enriched data locally
    os.makedirs(ENRICHED_DATA_DIR, exist_ok=True)
    enriched_file_path = os.path.join(
        ENRICHED_DATA_DIR, f'enriched_data_{datetime.now().strftime("%Y%m")}.csv')
    df.to_csv(enriched_file_path, index=False)
    logger.info(f"Enriched data saved to: {enriched_file_path}")
    #push the enriched file path to xcom
    ti = kwargs['ti']
    ti.xcom_push(key='enriched_file_path', value=enriched_file_path)

def upload_enriched_data_to_s3_task(**kwargs):
    from airflow.hooks.S3_hook import S3Hook
    ti = kwargs['ti']
    enriched_file_path = ti.xcom_pull(key='enriched_file_path', task_ids='enrich_data')

    s3_hook = S3Hook(aws_conn_id='aws_default')
    s3_hook.load_file(
        filename=enriched_file_path,
        key=ENRICHED_DATA_S3_PREFIX,
        bucket_name=BUCKET_NAME,
        replace=True
    )
    logger.info(f"Enriched data uploaded to S3 bucket: {BUCKET_NAME}")
    logger.info(f"S3 key: {ENRICHED_DATA_S3_PREFIX}")
    


def recursive_chunking_and_embedding_task(**kwargs):
    import pandas as pd
    import numpy as np
    import os
    import gc
    from collections import defaultdict
    from tqdm import tqdm
    from sentence_transformers import SentenceTransformer
    import json  # Added import for JSON handling

    # Import custom functions
    from langchain_text_splitters import RecursiveCharacterTextSplitter
    from custom_operators.shared_utils import (
        apply_bm25_sparse_vectors,
        embed_dataframe,
        generate_documents,
        save_documents_to_json,
        create_and_save_bm25_corpus
    )

    # Retrieve the task instance to use XCom
    ti = kwargs['ti']
    enriched_file_path = ti.xcom_pull(key='enriched_file_path', task_ids='enrich_data')

    logger.info(f"Loading enriched data from: {enriched_file_path}")

    # Read the DataFrame from the CSV file
    df = pd.read_csv(enriched_file_path)

    # Initialize models inside the function
    embed_model = SentenceTransformer('all-MiniLM-L6-v2', trust_remote_code=True)
    embed_model_name = 'all-MiniLM-L6-v2'

    # Define base paths
    base_path = '/opt/airflow/dags/embeddings'
    main_file = '/opt/airflow/dags/complete_files'

    # Ensure the directories exist
    os.makedirs(base_path, exist_ok=True)
    os.makedirs(main_file, exist_ok=True)

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


def upload_embeddings_to_s3_task(**kwargs):
    from airflow.hooks.S3_hook import S3Hook
    ti = kwargs['ti']
    embeddings_file_paths = ti.xcom_pull(key='embeddings_file_paths', task_ids='recursive_chunking_and_embedding')

    s3_hook = S3Hook(aws_conn_id='aws_default')
    for file_path in embeddings_file_paths:
        file_name = os.path.basename(file_path)
        s3_key = f"{EMBEDDINGS_S3_PREFIX}{file_name}"
        s3_hook.load_file(
            filename=file_path,
            key=s3_key,
            bucket_name=BUCKET_NAME,
            replace=True
        )
        logger.info(f"Embeddings file uploaded to S3 bucket: {BUCKET_NAME}")
        logger.info(f"S3 key: {s3_key}")

def upload_bm25_to_s3_task(**kwargs):
    from airflow.hooks.S3_hook import S3Hook
    bm25_file_path = os.path.join(BM25_DIR, 'bm25_values.json')

    s3_hook = S3Hook(aws_conn_id='aws_default')
    s3_key = f"{BM25_S3_PREFIX}bm25_values.json"
    s3_hook.load_file(
        filename=bm25_file_path,
        key=s3_key,
        bucket_name=BUCKET_NAME,
        replace=True
    )
    logger.info(f"BM25 file uploaded to S3 bucket: {BUCKET_NAME}")
    logger.info(f"S3 key: {s3_key}")


def get_overlap(chunk_size: int) -> int:
    return 50 if chunk_size <= 256 else 200


def upload_to_pinecone_task(**kwargs):
    from custom_operators.pinecone_func import upload_to_pinecone, get_pinecone_credentials, initialize_pinecone
    ti = kwargs['ti']
    embeddings_file_paths = ti.xcom_pull(key='embeddings_file_paths', task_ids='recursive_chunking_and_embedding')

    if not embeddings_file_paths:
        raise ValueError("No embeddings file paths received from XCom.")

    api_key, environment, host, index_name = get_pinecone_credentials()
    
    logger.info(f"Initializing Pinecone with API key: {api_key[:5]}..., environment: {environment}, host: {host}")
    
    pc = initialize_pinecone(api_key, environment)
    
    logger.info(f"Attempting to access index: {index_name}")
    index = pc.Index(index_name, host=host)  # Pass the host parameter here
    
    try:
        stats = index.describe_index_stats()
        logger.info(f"Successfully connected to index. Stats: {stats}")
    except Exception as e:
        logger.error(f"Error accessing index: {str(e)}")
        raise

    # Get Pinecone configuration from Airflow Variables
    project_name = Variable.get("PINECONE_PROJECT_NAME", default_var='huber-chatbot-project')
    metric = Variable.get("PINECONE_METRIC", default_var='cosine')

    for embeddings_file_path in embeddings_file_paths:
        logger.info(f"Processing embeddings file: {embeddings_file_path}")
        
        # Load the embeddings data
        with open(embeddings_file_path, 'r') as file:
            documents = json.load(file)
        
        logger.info(f"Loaded {len(documents)} documents from {embeddings_file_path}")

        # Prepare documents for upload
        chunk_size = 256  # Adjust this if you're using different chunk sizes
        documents_dict = {chunk_size: documents}

        # Upload to Pinecone
        upload_to_pinecone(
            api_key=api_key,
            documents=documents_dict,
            index_name=index_name,
            project_name=project_name,
            metric=metric,
            host=host
        )

        logger.info(f"Completed upload for {embeddings_file_path}")

    logger.info("All uploads completed successfully")

# Define the tasks
t1 = PythonOperator(
    task_id='process_sitemap',
    python_callable=process_sitemap_task,
    provide_context=True,
    dag=dag,
)

t2 = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_to_s3_task,
    provide_context=True,
    dag=dag,
)

t3 = PythonOperator(
    task_id='enrich_data',
    python_callable=enrich_data_task,
    provide_context=True,
    dag=dag,
)

t4 = PythonOperator(
    task_id='upload_enriched_data_to_s3',
    python_callable=upload_enriched_data_to_s3_task,
    provide_context=True,
    dag=dag,
)

t5 = PythonOperator(
    task_id='recursive_chunking_and_embedding',
    python_callable=recursive_chunking_and_embedding_task,
    provide_context=True,
    dag=dag,
)

t6 = PythonOperator(
    task_id='upload_embeddings_to_s3',
    python_callable=upload_embeddings_to_s3_task,
    provide_context=True,
    dag=dag,
)

t7 = PythonOperator(
    task_id='upload_bm25_to_s3',
    python_callable=upload_bm25_to_s3_task,
    provide_context=True,
    dag=dag,
)

t8 = PythonOperator(
    task_id='upload_to_pinecone',
    python_callable=upload_to_pinecone_task,
    provide_context=True,
    dag=dag,
)

# Set the task dependencies
t1 >> t2 >> t3 >> t4 >> t5 >> t6 >> t7 >> t8