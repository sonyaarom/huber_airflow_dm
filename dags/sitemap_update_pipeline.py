"""
This DAG is designed to compare the current and previous versions of the sitemap data,
detect any changes, and trigger the pinecone_update_and_notify_pipeline DAG if changes are detected.
It also sends a Telegram notification if changes are detected.
"""

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
import logging
import requests
from custom_operators.web_utils import send_telegram_message

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
    """
    Retrieves the content of a specific version of a file from S3.

    This function retrieves the content of a file from S3 based on the specified version.
    If the version is 'latest', it retrieves the latest version of the file.
    Otherwise, it retrieves a specific version of the file by its version ID.
    """
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
    """
    Compares two versions of sitemap data and identifies changes.

    This function takes two versions of sitemap data, old_content and new_content,
    and returns three sets: removed, delta, and added.
    """
    removed = {k: v for k, v in old_content.items() if k not in new_content}
    delta = {k: new_content[k] for k in set(old_content) & set(new_content) if old_content[k] != new_content[k]}
    added = {k: v for k, v in new_content.items() if k not in old_content}
    return removed, delta, added


def create_trigger_file(removed, delta, added, s3_hook):
    """
    Creates a trigger file with the changes detected in the sitemap data.

    This function takes the removed, delta, and added sets, and creates a JSON file
    containing the changes detected. It also includes the current date in the file.
    """
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




#Compare Versions Task (compare_task)
def compare_versions(**kwargs):
    """
    Compares the current and previous versions of the sitemap data.

    This function retrieves the latest and previous versions of the sitemap data from S3,
    compares them, and identifies any changes. It then creates a trigger file with the changes.
    """
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

#No Updates Needed Task (no_update_task)
def no_updates_needed(**kwargs):
    """
    Handles the case where no updates are needed.

    This function logs a message indicating that no updates are needed and sends a Telegram notification.
    """
    logger.info("No updates needed")
    try:
        send_telegram_message("New sitemap upload. No changes detected. No schema update needed.")
    except Exception as e:
        logger.error(f"Error occurred while sending Telegram message: {str(e)}")

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