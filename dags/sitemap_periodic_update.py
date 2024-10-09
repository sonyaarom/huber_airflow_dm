"""
This DAG is designed to periodically update the sitemap data and trigger the sitemap_update_pipeline.
It retrieves the sitemap data, processes it, and uploads it to S3. After the upload, it triggers the sitemap_update_pipeline DAG.
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime, timedelta
import os
import json
import logging
from custom_operators.web_utils import send_telegram_message

# Initialize logger
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
BUCKET_NAME = 'huber-chatbot-project'
SITEMAP_DATA_DIR = '/opt/airflow/dags/data'

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
    'sitemap_periodic_update',
    default_args=default_args,
    description='A DAG for retrieving sitemaps and enriching data',
    schedule_interval=timedelta(weeks=2),
    catchup=False
)

#Process Sitemap Task (process_sitemap_task)
def process_sitemap_task(**kwargs):
    """
    Processes the sitemap and extracts relevant data.

    This function retrieves the sitemap URL, excludes certain extensions and patterns,
    includes specific patterns, and allows a base URL. It then processes the sitemap,
    calculates various counts, and saves the data to a JSON file. 
    The safety check in there is a check of the base url. It prevents upload from the wrong, potentially harmful sites.
    """
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
    os.makedirs(SITEMAP_DATA_DIR, exist_ok=True)
    data_file_path = os.path.join(SITEMAP_DATA_DIR, 'sitemap_data.json')
    with open(data_file_path, 'w') as f:
        json.dump(data_dict, f)
    kwargs['ti'].xcom_push(key='data_file_path', value=data_file_path)

#Upload to S3 Task (upload_to_s3_task)
def upload_to_s3_task(**kwargs):
    """
    Uploads the processed sitemap data to S3.

    This function retrieves the data file path from XCom, generates a dynamic S3 key
    with the current timestamp, and uploads the file to the specified S3 bucket.
    """
    ti = kwargs['ti']
    data_file_path = ti.xcom_pull(key='data_file_path', task_ids='process_sitemap')
    
    # Generate a dynamic S3 key with the current timestamp
    s3_key_prefix = f'sitemap_data/sitemap_data_{datetime.now().strftime("%Y")}.json'
    
    s3_hook = S3Hook(aws_conn_id='aws_default')
    s3_hook.load_file(
        filename=data_file_path,
        key=s3_key_prefix,
        bucket_name=BUCKET_NAME,
        replace=True
    )
    logger.info(f"Data uploaded to S3 bucket: {BUCKET_NAME}")
    logger.info(f"S3 key: {s3_key_prefix}")
    
    # Push the S3 key to XCom for use in the trigger task
    kwargs['ti'].xcom_push(key='s3_key', value=s3_key_prefix)

#Tasks
process_sitemap = PythonOperator(
    task_id='process_sitemap',
    python_callable=process_sitemap_task,
    provide_context=True,
    dag=dag,
)

upload_to_s3 = PythonOperator(
    task_id='upload_to_s3',
    python_callable=upload_to_s3_task,
    provide_context=True,
    dag=dag,
)

trigger_update = TriggerDagRunOperator(
    task_id='trigger_update',
    trigger_dag_id='sitemap_update_pipeline',
    conf={'s3_key': "{{ task_instance.xcom_pull(task_ids='upload_to_s3', key='s3_key') }}"},
    reset_dag_run=True,
    wait_for_completion=False,
    dag=dag,
)

#Dependencies
process_sitemap >> upload_to_s3 >> trigger_update