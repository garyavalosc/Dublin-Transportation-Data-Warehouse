# Standard imports - the usual stuff we need
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from transport_etl import (
    extract_dublin_bus_data,
    load_to_snowflake
)
import pandas as pd

# DAG settings - pretty much standard config
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,  # might need to increase this if API is flaky
    'retry_delay': timedelta(minutes=5),
}

# Main DAG - runs at 2am when traffic is low
dag = DAG(
    'dublin_bus_pipeline',
    default_args=default_args,
    description='Pipeline for Dublin Bus passenger data',
    schedule_interval='0 2 * * *',  # 2am seemed like a good time
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dublin_transport'],
)

def extract_data(**context):
    """Get the daily bus passenger numbers - super straightforward"""
    try:
        url = "https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/TOA14/CSV/1.0/en"
        df = extract_dublin_bus_data(url)
        context['task_instance'].xcom_push(key='dublin_bus_raw_data', value=df.to_dict())
    except Exception as e:
        print(f"Data extraction failed :( Error: {str(e)}")
        raise

def load_data(**context):
    """Load the raw data into Snowflake - ELT ftw!"""
    try:
        ti = context['task_instance']
        data_dict = ti.xcom_pull(key='dublin_bus_raw_data', task_ids='extract_dublin_bus_data')
        df = pd.DataFrame(data_dict)
        load_to_snowflake(df, 'DUBLIN_BUS_PASSENGERS')
    except Exception as e:
        print(f"Failed to load data to Snowflake: {str(e)}")
        raise

# Define tasks - just extract and load now that we're doing ELT
extract_task = PythonOperator(
    task_id='extract_dublin_bus_data',
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_dublin_bus_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

# Simple flow: extract -> load
extract_task >> load_task
