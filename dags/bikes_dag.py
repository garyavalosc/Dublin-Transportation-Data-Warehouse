# Got all our imports - datetime for filtering the 3-month window
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from transport_etl import (
    extract_bikes_data,
    load_to_snowflake
)
import pandas as pd

# Basic settings for the DAG - nothing fancy
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# This one runs every 30 mins cuz bike availability changes a lot
dag = DAG(
    'bikes_pipeline',
    default_args=default_args,
    description='Pipeline for Dublin Bikes data',
    schedule_interval='*/30 * * * *',  # Frequent updates needed for this one
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dublin_transport'],
)

def extract_data(**context):
    """Get the latest bikes data - only keeping last 3 months as requested"""
    try:
        url = "https://data.smartdublin.ie/dublinbikes-api/bikes/dublin_bikes/historical/stations"
        df = extract_bikes_data(url)
        
        # Convert timestamps to strings so XCom doesnt complain
        df['last_reported'] = pd.to_datetime(df['last_reported']).dt.strftime('%Y-%m-%d %H:%M:%S')
        
        # Filter to last 3 months - requirement from the task
        three_months_ago = (datetime.now() - timedelta(days=90)).strftime('%Y-%m-%d %H:%M:%S')
        df = df[df['last_reported'] >= three_months_ago]
        
        # Make column names match Snowflake
        df.columns = map(str.upper, df.columns)
        
        # Save to XCom for the load task
        data_dict = df.to_dict(orient='records')
        context['task_instance'].xcom_push(key='bikes_raw_data', value=data_dict)
        
    except Exception as e:
        print(f"Bikes data extraction failed... Error: {str(e)}")
        raise

def load_data(**context):
    """Push the raw data to Snowflake - keeping it simple with ELT"""
    try:
        ti = context['task_instance']
        data_dict = ti.xcom_pull(key='bikes_raw_data', task_ids='extract_bikes_data')
        df = pd.DataFrame(data_dict)
        load_to_snowflake(df, 'DUBLIN_BIKES')
    except Exception as e:
        print(f"Snowflake load failed :( Error: {str(e)}")
        raise

# Set up the tasks - just extract and load now
extract_task = PythonOperator(
    task_id='extract_bikes_data',
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_bikes_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

# Simple pipeline: extract -> load
extract_task >> load_task
