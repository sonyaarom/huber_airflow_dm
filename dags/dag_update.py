from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook
from airflow.utils.trigger_rule import TriggerRule
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
import json
import os
import logging
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
TRIGGER_DIRECTORY = '/opt/airflow/dags/triggers'
BUCKET_NAME = 'huber-chatbot-project'
FILE_PATH = 'sitemap_data/sitemap_data_2024.json'

# Telegram setup
telegram_conn = BaseHook.get_connection("telegram_default")
TELEGRAM_BOT_TOKEN = telegram_conn.password
TELEGRAM_CHAT_ID = telegram_conn.login

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
    'sitemap_update_pipeline',
    default_args=default_args,
    description='A DAG for checking sitemap changes and updating data',
    schedule_interval='@daily',
    catchup=False
)

def get_s3_version(s3_client, bucket_name, file_path, version='latest'):
    try:
        if version == 'latest':
            response = s3_client.get_object(Bucket=bucket_name, Key=file_path)
        else:
            versions = s3_client.list_object_versions(Bucket=bucket_name, Prefix=file_path)
            version_list = versions.get('Versions', [])
            if len(version_list) < 2:
                logger.warning("No previous version available.")
                return None
            version_id = version_list[1]['VersionId']
            response = s3_client.get_object(Bucket=bucket_name, Key=file_path, VersionId=version_id)
        return response['Body'].read().decode('utf-8')
    except ClientError as e:
        logger.error(f"Error retrieving S3 object: {e}")
        return None

def get_changes(old_content, new_content):
    removed = {k: v for k, v in old_content.items() if k not in new_content}
    delta = {k: new_content[k] for k in set(old_content) & set(new_content) if old_content[k] != new_content[k]}
    added = {k: v for k, v in new_content.items() if k not in old_content}
    return removed, delta, added

def create_trigger_file(removed, delta, added):
    os.makedirs(TRIGGER_DIRECTORY, exist_ok=True)
    current_date = datetime.now()
    trigger_filename = f"triggerfile_{current_date.strftime('%Y_%m')}.json"
    trigger_path = os.path.join(TRIGGER_DIRECTORY, trigger_filename)
    
    trigger_content = {
        "removed": removed,
        "modified": delta,
        "added": added
    }
    
    try:
        with open(trigger_path, 'w') as file:
            json.dump(trigger_content, file, indent=2)
        logger.info(f"Trigger file created: {trigger_filename}")
        return trigger_path
    except IOError as e:
        logger.error(f"Error creating trigger file: {e}")
        return None

def send_telegram_message(message):
    url = f"https://api.telegram.org/bot{TELEGRAM_BOT_TOKEN}/sendMessage"
    payload = {
        "chat_id": TELEGRAM_CHAT_ID,
        "text": message,
        "parse_mode": "Markdown"
    }
    try:
        response = requests.post(url, json=payload)
        response.raise_for_status()
        logger.info("Telegram message sent successfully")
    except requests.RequestException as e:
        logger.error(f"Failed to send Telegram message: {e}")

def compare_versions(**kwargs):
    s3_hook = S3Hook(aws_conn_id='aws_default')
    s3_client = s3_hook.get_conn()
    
    latest_content = get_s3_version(s3_client, BUCKET_NAME, FILE_PATH, 'latest')
    previous_content = get_s3_version(s3_client, BUCKET_NAME, FILE_PATH, 'previous')
    
    if latest_content and previous_content:
        latest_data = json.loads(latest_content)
        previous_data = json.loads(previous_content)
        
        removed, delta, added = get_changes(previous_data, latest_data)
        
        if removed or delta or added:
            trigger_file_path = create_trigger_file(removed, delta, added)
            
            if trigger_file_path:
                kwargs['ti'].xcom_push(key='trigger_file_path', value=trigger_file_path)
                kwargs['ti'].xcom_push(key='changes_detected', value=True)
                
                message = f"*Sitemap Changes Detected*\n"
                message += f"- Removed: {len(removed)} items\n"
                message += f"- Modified: {len(delta)} items\n"
                message += f"- Added: {len(added)} items\n"
                message += f"Trigger file created: {os.path.basename(trigger_file_path)}"
                
                send_telegram_message(message)
                return 'update_data'
            else:
                logger.error("Failed to create trigger file")
        else:
            logger.info("No changes detected in the sitemap")
            kwargs['ti'].xcom_push(key='changes_detected', value=False)
            return 'no_updates_needed'
    else:
        logger.error("Failed to retrieve both versions for comparison")
    
    kwargs['ti'].xcom_push(key='changes_detected', value=False)
    return 'no_updates_needed'

def update_data(**kwargs):
    # Placeholder for data update logic
    logger.info("Updating data based on detected changes")
    # Add your data update logic here
    send_telegram_message("Data update process started")

def no_updates_needed(**kwargs):
    logger.info("No updates needed")
    send_telegram_message("No sitemap changes detected. No updates needed.")

def cleanup_old_trigger_files(**kwargs):
    current_date = datetime.now()
    for filename in os.listdir(TRIGGER_DIRECTORY):
        if filename.startswith("triggerfile_"):
            file_path = os.path.join(TRIGGER_DIRECTORY, filename)
            file_date = datetime.strptime(filename, "triggerfile_%Y_%m.json")
            if (current_date - file_date).days > 30:
                os.remove(file_path)
                logger.info(f"Removed old trigger file: {filename}")

# Task definitions
compare_task = BranchPythonOperator(
    task_id='compare_versions',
    python_callable=compare_versions,
    provide_context=True,
    dag=dag,
)

update_task = PythonOperator(
    task_id='update_data',
    python_callable=update_data,
    provide_context=True,
    dag=dag,
)

no_update_task = PythonOperator(
    task_id='no_updates_needed',
    python_callable=no_updates_needed,
    provide_context=True,
    dag=dag,
)

cleanup_task = PythonOperator(
    task_id='cleanup_old_trigger_files',
    python_callable=cleanup_old_trigger_files,
    provide_context=True,
    trigger_rule=TriggerRule.ALL_DONE,
    dag=dag,
)

# Set up task dependencies
compare_task >> [update_task, no_update_task] >> cleanup_task