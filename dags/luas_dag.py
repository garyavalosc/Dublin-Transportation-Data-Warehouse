# Imports for datetime stuff and airflow modules
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from transport_etl import (
    extract_luas_data,
    transform_luas_data,
    load_to_snowflake
)
import logging
import pandas as pd

# Set up logging - need this for debugging
logger = logging.getLogger('dublin_transport')

# URL for getting Luas data - hope they dont change it lol
LUAS_DATA_URL = "https://ws.cso.ie/public/api.restful/PxStat.Data.Cube_API.ReadDataset/TOA11/CSV/1.0/en"

# Basic DAG settings - keeping it simple
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,  # might enable this later if needed
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Main DAG definition - runs daily to get Luas passenger numbers
dag = DAG(
    'luas_pipeline',
    default_args=default_args,
    description='ETL pipeline for Luas passenger data',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),  # starting from 2023 to get historical data
    catchup=False,
)

def extract_data(**context):
    """Grab Luas data and save it to XCom - pretty straightforward"""
    try:
        df = extract_luas_data(LUAS_DATA_URL)
        # Need to convert to records for XCom to handle it
        context['task_instance'].xcom_push(
            key='luas_raw_data',
            value=df.to_dict(orient='records')
        )
        logger.info("Got the Luas data successfully!")
    except Exception as e:
        logger.error(f"Ugh, failed to get Luas data: {str(e)}")
        raise


def load_data(**context):
    """Push everything to Snowflake - fingers crossed"""
    try:
        ti = context['task_instance']
        # One last pull from XCom
        data_dict = ti.xcom_pull(key='luas_transformed_data', task_ids='transform_luas_data')
        df = pd.DataFrame.from_records(data_dict)
        
        load_to_snowflake(df, 'LUAS_PASSENGERS')
        logger.info("Data is in Snowflake - we're good!")
    except Exception as e:
        logger.error(f"Snowflake load failed: {str(e)}")
        raise

# Setting up the tasks - extract -> transform -> load
extract_task = PythonOperator(
    task_id='extract_luas_data',
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

transform_task = PythonOperator(
    task_id='transform_luas_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_to_snowflake',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

# Connect the tasks in order
extract_task >> load_task
