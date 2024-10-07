from airflow import DAG
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.hooks.base import BaseHook
from airflow.models import Variable
from airflow.utils.trigger_rule import TriggerRule
from requests.exceptions import RequestException
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
import json
import os
import logging
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
TRIGGER_FILE_PREFIX = 'triggers/'
BUCKET_NAME = 'huber-chatbot-project'
FILE_PATH = 'sitemap_data/sitemap_data_2024.json'

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
    'sitemap_update_pipeline',
    default_args=default_args,
    description='A DAG for checking sitemap changes and updating data',
    schedule_interval=None,  # Changed from '@daily' to None
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


def create_trigger_file(removed, delta, added, s3_hook):
    current_date = datetime.now()
    trigger_filename = f"triggerfile_{current_date.strftime('%Y_%m_%d')}.json"
    s3_key = f"{TRIGGER_FILE_PREFIX}{trigger_filename}"
    
    trigger_content = {
        "removed": removed,
        "modified": delta,
        "added": added,
        "timestamp": current_date.isoformat()
    }
    
    try:
        s3_hook.load_string(
            string_data=json.dumps(trigger_content, indent=2),
            key=s3_key,
            bucket_name=BUCKET_NAME,
            replace=True
        )
        logger.info(f"Trigger file uploaded to S3: {s3_key}")
        return s3_key
    except Exception as e:
        logger.error(f"Error uploading trigger file to S3: {e}")
        return None


def send_telegram_message(message):
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
            trigger_file_s3_key = create_trigger_file(removed, delta, added, s3_hook)
            
            if trigger_file_s3_key:
                kwargs['ti'].xcom_push(key='trigger_file_s3_key', value=trigger_file_s3_key)
                kwargs['ti'].xcom_push(key='changes_detected', value=True)
                
                message = f"*Sitemap Changes Detected*\n\n"
                message += f"📊 *Statistics:*\n"
                message += f"- Removed: {len(removed)} items\n"
                message += f"- Modified: {len(delta)} items\n"
                message += f"- Added: {len(added)} items\n\n"
                message += f"🔗 *Details:*\n"
                message += f"- Total items before: {len(previous_data)}\n"
                message += f"- Total items after: {len(latest_data)}\n"
                message += f"- Net change: {len(latest_data) - len(previous_data)} items\n\n"
                message += f"📁 Trigger file uploaded to S3: `{trigger_file_s3_key}`\n\n"
                message += f"⏳ Update process will start soon."
                
                send_telegram_message(message)
                logger.info(f"Changes detected and Telegram notification sent. Triggering update DAG.")
                return 'trigger_update_dag'
            else:
                logger.error("Failed to upload trigger file to S3")
        else:
            logger.info("No changes detected in the sitemap")
            kwargs['ti'].xcom_push(key='changes_detected', value=False)
            return 'no_updates_needed'
    else:
        logger.error("Failed to retrieve both versions for comparison")
    
    kwargs['ti'].xcom_push(key='changes_detected', value=False)
    return 'no_updates_needed'

def no_updates_needed(**kwargs):
    logger.info("No updates needed")
    try:
        send_telegram_message("New sitemap upload. No changes detected. No schema update needed.")
    except Exception as e:
        logger.error(f"Error occurred while sending Telegram message: {str(e)}")

# def cleanup_old_trigger_files(**kwargs):
#     current_date = datetime.now()
#     for filename in os.listdir(TRIGGER_DIRECTORY):
#         if filename.startswith("triggerfile_"):
#             file_path = os.path.join(TRIGGER_DIRECTORY, filename)
#             file_date = datetime.strptime(filename, "triggerfile_%Y_%m.json")
#             if (current_date - file_date).days > 30:
#                 os.remove(file_path)
#                 logger.info(f"Removed old trigger file: {filename}")

# Task definitions
compare_task = BranchPythonOperator(
    task_id='compare_versions',
    python_callable=compare_versions,
    provide_context=True,
    dag=dag,
)

trigger_update_dag_task = TriggerDagRunOperator(
    task_id='trigger_update_dag',
    trigger_dag_id='pinecone_update_and_notify_pipeline',
    conf={'trigger_file_s3_key': "{{ task_instance.xcom_pull(task_ids='compare_versions', key='trigger_file_s3_key') }}"},
    dag=dag,
)

no_update_task = PythonOperator(
    task_id='no_updates_needed',
    python_callable=no_updates_needed,
    provide_context=True,
    dag=dag,
)

# Set up task dependencies
compare_task >> [trigger_update_dag_task, no_update_task]