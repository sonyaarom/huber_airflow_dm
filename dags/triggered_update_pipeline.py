from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.amazon.aws.hooks.s3 import S3Hook
from airflow.models import Variable
from datetime import datetime, timedelta
import json
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# Constants
BUCKET_NAME = 'huber-chatbot-project'

# Telegram setup (if you want to send notifications from this DAG as well)
TELEGRAM_BOT_TOKEN = Variable.get("TELEGRAM_BOT_TOKEN", default_var=None)
TELEGRAM_CHAT_ID = Variable.get("TELEGRAM_CHAT_ID", default_var=None)

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
    'triggered_update_pipeline',
    default_args=default_args,
    description='A DAG triggered by the sitemap update pipeline when changes are detected',
    schedule_interval=None,  # This DAG will be triggered externally
    catchup=False
)

def process_changes(**kwargs):
    trigger_file_s3_key = kwargs['dag_run'].conf.get('trigger_file_s3_key')
    if not trigger_file_s3_key:
        raise ValueError("No trigger file S3 key provided")

    s3_hook = S3Hook(aws_conn_id='aws_default')
    
    # Read the trigger file content
    trigger_data = s3_hook.read_key(trigger_file_s3_key, BUCKET_NAME)
    changes = json.loads(trigger_data)
    
    logger.info(f"Processing changes from trigger file: {trigger_file_s3_key}")
    logger.info(f"Removed items: {len(changes['removed'])}")
    logger.info(f"Modified items: {len(changes['modified'])}")
    logger.info(f"Added items: {len(changes['added'])}")
    