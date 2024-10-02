from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from botocore.exceptions import ClientError
from datetime import datetime, timedelta
import json
import os
import logging

TRIGGER_DIRECTORY = '/opt/airflow/dags/triggers'
BUCKET_NAME = 'huber-chatbot-project'
FILE_PATH = 'sitemap_data/sitemap_data_2024.json'

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
    'sitemap_version_comparison',
    default_args=default_args,
    description='A DAG for comparing sitemap versions and creating trigger files',
    schedule_interval=None,
    catchup=False
)

logger = logging.getLogger(__name__)

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
        
        print("Changes detected:")
        print(f"Removed: {len(removed)} items")
        print(f"Modified: {len(delta)} items")
        print(f"Added: {len(added)} items")
    else:
        print("Failed to retrieve both versions for comparison.")

t1 = PythonOperator(
    task_id='compare_versions_and_create_trigger',
    python_callable=compare_versions_and_create_trigger,
    provide_context=True,
    dag=dag,
)

# Set task dependencies (not nee
t1