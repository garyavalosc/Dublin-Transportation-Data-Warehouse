# Basic imports for our cycling data pipeline
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from transport_etl import (
    extract_cycle_counts_data,
    load_to_snowflake
)
import pandas as pd

# Standard DAG config - same as other pipelines for consistency
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Running this one at 4am - after all other transports
dag = DAG(
    'cycle_counts_pipeline',
    default_args=default_args,
    description='Pipeline for Dublin Cycle Counts data',
    schedule_interval='0 4 * * *',  # 4am daily - last in our sequence
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dublin_transport'],
)

def extract_data(**context):
    """Grab the cycling counts data - lots of columns in this one!"""
    try:
        # Super long URL - maybe should move this to a config file later
        url = "https://data.smartdublin.ie/dataset/d26ce6c0-2e1c-4b72-8fbd-cb9f9cbbc118/resource/770a192c-12df-4620-b642-b895164dd6e5/download/cycle-counts-1-jan-3-november-2024.csv"
        df = extract_cycle_counts_data(url)
        context['task_instance'].xcom_push(key='cycle_counts_raw_data', value=df.to_dict())
    except Exception as e:
        print(f"Failed to get cycle counts :( Error: {str(e)}")
        raise

def load_data(**context):
    """Load everything to Snowflake - raw data, ELT style"""
    try:
        ti = context['task_instance']
        data_dict = ti.xcom_pull(key='cycle_counts_raw_data', task_ids='extract_cycle_counts_data')
        df = pd.DataFrame(data_dict)
        
        # Need to fix the timestamp format - Snowflake is picky
        if 'TIME' in df.columns:
            df['TIME'] = pd.to_datetime(df['TIME'], format="%d/%m/%Y %H:%M", errors='coerce')
        
        load_to_snowflake(df, 'CYCLE_COUNTS')
    except Exception as e:
        print(f"Snowflake load failed - timestamp issues maybe? Error: {str(e)}")
        raise

# Setting up tasks - extract and load only now with ELT
extract_task = PythonOperator(
    task_id='extract_cycle_counts_data',
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_cycle_counts_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

# Simple flow: extract -> load
extract_task >> load_task
