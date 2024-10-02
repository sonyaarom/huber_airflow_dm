from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
import json
import pandas as pd
import os
import logging
import requests


logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


# Import custom functions
from custom_operators.sitemap_processor import process_sitemap
from custom_operators.web_utils import add_html_content_to_df
from custom_operators.content_extractor import add_extracted_content_to_df
from custom_operators.download_funcs import upload_to_s3

TRIGGER_DIRECTORY = '/opt/airflow/dags/triggers'
BUCKET_NAME = 'huber-chatbot-project'
FILE_PATH = 'sitemap_data/sitemap_data_2024.json'

telegram_conn = BaseHook.get_connection("telegram_default")
TELEGRAM_BOT_TOKEN = telegram_conn.password
TELEGRAM_CHAT_ID = telegram_conn.login

# Define default_args for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Create the DAG
dag = DAG(
    'sitemap_processing_pipeline',
    default_args=default_args,
    description='A DAG for processing sitemaps and extracting content',
    schedule_interval='@monthly',  # Run monthly
    catchup=False
)

# Define the tasks
def process_sitemap_task(**kwargs):
    url = 'https://www.wiwi.hu-berlin.de/sitemap.xml.gz'
    exclude_extensions = ['.jpg', '.pdf', '.jpeg', '.png']
    exclude_patterns = ['view']
    include_patterns = ['/en/']
    allowed_base_url = 'https://www.wiwi.hu-berlin.de'
    
    data_dict, total, filtered, safe, unsafe = process_sitemap(
        url, exclude_extensions, exclude_patterns, include_patterns, allowed_base_url
    )
    
    print(f"Total entries: {total}")
    print(f"Filtered entries: {filtered}")
    print(f"Safe entries: {safe}")
    print(f"Unsafe entries: {unsafe}")
    
    # Pass the data_dict to the next task
    kwargs['ti'].xcom_push(key='data_dict', value=data_dict)
    return data_dict

def upload_to_s3_task(**kwargs):
    ti = kwargs['ti']
    data_dict = ti.xcom_pull(key='data_dict', task_ids='process_sitemap')
    
    bucket_name = 'huber-chatbot-project'
    s3_key_prefix = f'sitemap_data/sitemap_data_{datetime.now().strftime("%Y")}.json'
    
    json_data = json.dumps(data_dict)
    
    s3_hook = S3Hook(aws_conn_id='aws_default')
    s3_hook.load_string(
        string_data=json_data,
        key=s3_key_prefix,
        bucket_name=bucket_name,
        replace=True
    )
    print(f"Data uploaded to S3 bucket: {bucket_name}")
    print(f"S3 key: {s3_key_prefix}")
    
    # Pass the bucket name and key to the next task
    ti.xcom_push(key='bucket_name', value=bucket_name)
    ti.xcom_push(key='s3_key', value=s3_key_prefix)


def process_data_task(**kwargs):
    ti = kwargs['ti']
    data_dict = ti.xcom_pull(key='data_dict', task_ids='process_sitemap')
    
    df = pd.DataFrame.from_dict(data_dict, orient='index').reset_index()
    df.columns = ['id', 'url', 'last_updated']
    
    # Remove this line for full processing
    # df = df.head(5)
    
    df = add_html_content_to_df(df)
    df = add_extracted_content_to_df(df)
    
    print(df[['url', 'extracted_title', 'extracted_content']])
    
    # Uncomment and adjust the following line to save processed data to S3
    # upload_to_s3(df.to_json(), 'huber-chatbot-project', f'processed_data/processed_data_{datetime.now().strftime("%Y%m")}.json')


def get_changes(old_content, new_content):
    removed = {}
    delta = {}
    added = {}
    
    for key, value in old_content.items():
        if key not in new_content:
            removed[key] = value
        elif new_content[key] != value:
            delta[key] = value
    
    for key, value in new_content.items():
        if key not in old_content:
            added[key] = value
    
    return removed, delta, added

def create_trigger_file(removed, delta, added):
    os.makedirs(TRIGGER_DIRECTORY, exist_ok=True)
    current_date = datetime.now()
    month = current_date.month 
    year = current_date.year
    trigger_filename = f"triggerfile_{year}_{month}.json"
    trigger_path = os.path.join(TRIGGER_DIRECTORY, trigger_filename)
    
    trigger_content = {
        "removed": removed,
        "modified": delta,
        "added": added,
        "timestamp": current_date.isoformat()
    }
    
    with open(trigger_path, 'w') as file:
        json.dump(trigger_content, file, indent=2)
    print(f"Trigger file created: {trigger_filename}")


def compare_versions_and_create_trigger(**kwargs):
    ti = kwargs['ti']
    bucket_name = ti.xcom_pull(key='bucket_name', task_ids='upload_to_s3')
    s3_key = ti.xcom_pull(key='s3_key', task_ids='upload_to_s3')
    
    if not bucket_name or not s3_key:
        raise ValueError(f"bucket_name ({bucket_name}) or s3_key ({s3_key}) not set")
    
    s3_hook = S3Hook(aws_conn_id='aws_default')
    
    print(f"Attempting to read from bucket: {bucket_name}, key: {s3_key}")
    
    # Get latest version
    latest_content = s3_hook.read_key(key=s3_key, bucket_name=bucket_name)
    if not latest_content:
        print(f"No content found for key {s3_key} in bucket {bucket_name}")
        return
    
    latest_data = json.loads(latest_content)
    
    # Get previous version
    versions = s3_hook.list_keys(bucket_name=bucket_name, prefix=s3_key)
    if len(versions) < 2:
        print("There is no previous version available.")
        return
    
    previous_version = versions[-2]  # Get the second most recent version
    previous_content = s3_hook.read_key(key=previous_version, bucket_name=bucket_name)
    previous_data = json.loads(previous_content)
    
    # Compare versions
    removed, delta, added = get_changes(previous_data, latest_data)
    
    # Create trigger file
    create_trigger_file(removed, delta, added)


def get_version(s3_client, bucket_name, file_path, version='latest'):
    try:
        if version == 'latest':
            response = s3_client.get_object(Bucket=bucket_name, Key=file_path)
        else:
            versions = s3_client.list_object_versions(Bucket=bucket_name, Prefix=file_path)
            version_list = versions.get('Versions', [])
            if len(version_list) < 2:
                print("There is no previous version available.")
                return None
            version_id = version_list[1]['VersionId']
            response = s3_client.get_object(Bucket=bucket_name, Key=file_path, VersionId=version_id)
        return response['Body'].read().decode('utf-8')
    except ClientError as e:
        print(f"An error occurred: {e}")
        return None

def get_changes(old_content, new_content):
    removed = {}
    delta = {}
    added = {}
    
    for key, value in old_content.items():
        if key not in new_content:
            removed[key] = value
        elif new_content[key] != value:
            delta[key] = new_content[key]
    
    for key, value in new_content.items():
        if key not in old_content:
            added[key] = value
    
    return removed, delta, added

def create_trigger_file(removed, delta, added):
    os.makedirs(TRIGGER_DIRECTORY, exist_ok=True)
    current_date = datetime.now()
    month = current_date.month 
    year = current_date.year
    trigger_filename = f"triggerfile_{year}_{month}.json"
    trigger_path = os.path.join(TRIGGER_DIRECTORY, trigger_filename)
    
    trigger_content = {
        "removed": removed,
        "modified": delta,
        "added": added,
        "timestamp": current_date.isoformat()
    }
    
    try:
        with open(trigger_path, 'w') as file:
            json.dump(trigger_content, file, indent=2)
        print(f"Trigger file created: {trigger_filename}")
    except IOError as e:
        print(f"An error occurred while creating the trigger file: {e}")


def send_telegram_message(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message
    }
    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
        logger.info("Telegram message sent successfully")
    except requests.RequestException as e:
        logger.error(f"Failed to send Telegram message: {e}")


def compare_versions_and_create_trigger(**kwargs):
    s3_hook = S3Hook(aws_conn_id='aws_default')
    s3_client = s3_hook.get_conn()
    
    # Get latest version
    latest_content = get_version(s3_client, BUCKET_NAME, FILE_PATH, 'latest')
    
    # Get previous version
    previous_content = get_version(s3_client, BUCKET_NAME, FILE_PATH, 'previous')
    
    if latest_content and previous_content:
        # Parse JSON content
        latest_data = json.loads(latest_content)
        previous_data = json.loads(previous_content)
        
        # Compare versions
        removed, delta, added = get_changes(previous_data, latest_data)
        
        # Create trigger file
        create_trigger_file(removed, delta, added)
        
        message = f"Changes detected:\nRemoved: {len(removed)} items\nModified: {len(delta)} items\nAdded: {len(added)} items"
        logger.info(message)
        send_telegram_message(message)
    else:
        message = "Failed to retrieve both versions for comparison."
        logger.error(message)
        send_telegram_message(message)

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
    task_id='compare_and_trigger',
    python_callable=compare_versions_and_create_trigger,
    provide_context=True,
    dag=dag,
)

# Set the task dependencies
t1 >> t2 >> t3

#TODO: add telegram notifications