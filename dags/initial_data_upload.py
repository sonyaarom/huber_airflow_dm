from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import sys
import os
import boto3
import json
import pandas as pd
import numpy as np
from sentence_transformers import SentenceTransformer
from typing import List, Tuple, Any, Dict
from tqdm import tqdm
from logging import getLogger
import gc
from langchain_text_splitters import RecursiveCharacterTextSplitter
from gliner import GLiNER
from collections import defaultdict

# Import custom functions
from custom_operators.sitemap_processor import process_sitemap
from custom_operators.web_utils import add_html_content_to_df
from custom_operators.content_extractor import add_extracted_content_to_df
from custom_operators.download_funcs import upload_to_s3
from custom_operators.shared_utils import apply_bm25_sparse_vectors, embed_dataframe, generate_documents, save_documents_to_json

logger = getLogger(__name__)

# Initialize GLiNER model
ner_model = GLiNER.from_pretrained("urchade/gliner_small-v2.1")
labels = ["person", "course", "date", "research_paper", "research_project", "teams", "city", "address", "organisation", "phone_number", "url", "other"]

def get_overlap(chunk_size: int) -> int:
    return 50 if chunk_size <= 256 else 200

def convert_entities_to_label_name_dict(entities: List[Dict[str, Any]]) -> Dict[str, List[str]]:
    label_name_dict = defaultdict(set)
    for entity in entities:
        text = entity['text'].strip().lower()
        label_name_dict[entity['label']].add(text)
    return {k: list(v) for k, v in label_name_dict.items()}

# Import custom functions
from custom_operators.sitemap_processor import process_sitemap
from custom_operators.web_utils import add_html_content_to_df
from custom_operators.content_extractor import add_extracted_content_to_df

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

BUCKET_NAME = 'huber-chatbot-project'
S3_KEY_PREFIX = f'sitemap_data/sitemap_data_{datetime.now().strftime("%Y")}.json'
ENRICHED_DATA_DIR = '/opt/airflow/dags/enriched'

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
    'sitemap_retrieval_and_enrichment',
    default_args=default_args,
    description='A DAG for retrieving sitemaps and enriching data',
    schedule_interval=None,
    catchup=False
)

def process_sitemap_task(**kwargs):
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
    
    # Pass the data_dict to the next task
    kwargs['ti'].xcom_push(key='data_dict', value=data_dict)
    return data_dict

def upload_to_s3_task(**kwargs):
    ti = kwargs['ti']
    data_dict = ti.xcom_pull(key='data_dict', task_ids='process_sitemap')
    
    json_data = json.dumps(data_dict)
    
    s3_hook = S3Hook(aws_conn_id='aws_default')
    s3_hook.load_string(
        string_data=json_data,
        key=S3_KEY_PREFIX,
        bucket_name=BUCKET_NAME,
        replace=True
    )
    logger.info(f"Data uploaded to S3 bucket: {BUCKET_NAME}")
    logger.info(f"S3 key: {S3_KEY_PREFIX}")

def enrich_data_task(**kwargs):
    ti = kwargs['ti']
    data_dict = ti.xcom_pull(key='data_dict', task_ids='process_sitemap')
    
    # Process the data
    df = pd.DataFrame.from_dict(data_dict, orient='index').reset_index()
    df.columns = ['id', 'url', 'last_updated']
    
    # Take a sample for testing (remove this line for full processing)
    df = df.head(5)
    
    # Add HTML content to the DataFrame
    df = add_html_content_to_df(df)
    
    # Extract and add content to the DataFrame
    df = add_extracted_content_to_df(df)
    
    # Display results
    logger.info(df[['url', 'extracted_title', 'extracted_content']])
    
    # Save enriched data locally
    os.makedirs(ENRICHED_DATA_DIR, exist_ok=True)
    enriched_file_path = os.path.join(ENRICHED_DATA_DIR, f'enriched_data_{datetime.now().strftime("%Y%m%d")}.csv')
    df.to_csv(enriched_file_path, index=False)
    logger.info(f"Enriched data saved to: {enriched_file_path}")


def recursive_chunking_and_embedding_task(**kwargs):
    ti = kwargs['ti']
    df = ti.xcom_pull(key='processed_df', task_ids='process_data')
    
    chunk_lengths = [256]  # You can adjust these as needed
    embed_model = SentenceTransformer('all-MiniLM-L6-v2', trust_remote_code=True)
    embed_model_name = 'all-MiniLM-L6-v2'
    base_path = '/opt/airflow/dags/embeddings'
    
    # Initialize BM25 (you might need to adjust this based on your shared_utils implementation)
    bm25_values = {}  # This should be initialized properly based on your implementation
    
    chunk_stats = process_data_recursive_langchain(df, chunk_lengths, embed_model, embed_model_name, base_path, bm25_values)
    
    # Log chunk statistics
    for stat in chunk_stats:
        logger.info(f"Chunk length {stat[0]}: min={stat[1]}, max={stat[2]}, mean={stat[3]:.2f}, median={stat[4]:.2f}")

def process_data_recursive_langchain(df: pd.DataFrame, chunk_lengths: List[int], embed_model: Any, embed_model_name: str, base_path: str, bm25_values: dict) -> List[Tuple[int, int, int, float, float]]:
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
        entities = []

        for _, row in tqdm(df.iterrows(), total=len(df), desc="Chunking texts"):
            if isinstance(row['text'], str):
                chunks = text_splitter.split_text(row['text'])
                all_chunks.extend(chunks)
                unique_ids.extend([f"{row['id']}_{i+1}" for i in range(len(chunks))])
                general_ids.extend([row['id']] * len(chunks))
                urls.extend([row['url']] * len(chunks))
                last_updateds.extend([row.get('last_updated', '')] * len(chunks))
                html_contents.extend([row.get('html_content', '')] * len(chunks))
                
                chunk_entities = [convert_entities_to_label_name_dict(ner_model.predict_entities(chunk, labels)) for chunk in chunks]
                entities.extend(chunk_entities)

        chunked_df = pd.DataFrame({
            'unique_id': unique_ids,
            'url': urls,
            'last_updated': last_updateds,
            'html_content': html_contents,
            'text': all_chunks,
            'len': [len(chunk) for chunk in all_chunks],
            'general_id': general_ids,
            'entities': entities
        })

        chunked_df = chunked_df[chunked_df['len'] >= 50]

        logger.info(f"Created {len(chunked_df)} chunks")

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

        logger.info("Applying BM25 sparse vectorization")
        chunked_df = apply_bm25_sparse_vectors(chunked_df, 'text', bm25_values)

        logger.info("Embedding chunked texts")
        embedded_df = embed_dataframe(chunked_df, embed_model)

        logger.info("Generating document dictionaries")
        documents = generate_documents(embedded_df, chunk_length, 'recursive')

        logger.info("Saving documents to JSON")
        save_documents_to_json(documents, chunk_length, embed_model_name, 'recursive', base_path)

        del chunked_df, embedded_df, documents
        gc.collect()

        logger.info(f"Finished processing chunks of max length {chunk_length}.")

    return chunk_stats


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
    task_id='recursive_chunking_and_embedding',
    python_callable=recursive_chunking_and_embedding_task,
    provide_context=True,
    dag=dag,
)



# Set the task dependencies
t1 >> t2 >> t3 >> t4
