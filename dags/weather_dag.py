# Basic imports - datetime for scheduling, pandas for data handling
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from transport_etl import (
    extract_weather_data,
    load_to_snowflake,
)
import pandas as pd

# DAG settings - copied from my other DAGs but tweaked schedule
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Weather data updates once per day so 3am seemed good
dag = DAG(
    'weather_pipeline',
    default_args=default_args,
    description='Pipeline for Dublin Weather data',
    schedule_interval='0 3 * * *',  # 3am - after Luas and Dublin Bus
    start_date=datetime(2024, 1, 1),
    catchup=False,
    tags=['dublin_transport'],
)

def extract_data(**context):
    """Get weather data from Met Eireann - Merrion Square station"""
    try:
        url = "http://cli.fusio.net/cli/climate_data/webdata/dly3923.csv"
        df = extract_weather_data(url)
        context['task_instance'].xcom_push(key='weather_raw_data', value=df.to_dict())
    except Exception as e:
        print(f"Weather data extract failed... typical Irish weather! Error: {str(e)}")
        raise  
     
def load_data(**context):
    """Push raw data to Snowflake - we'll transform it there"""
    try:
        ti = context['task_instance']
        data_dict = ti.xcom_pull(key='weather_raw_data', task_ids='extract_weather_data')
        df = pd.DataFrame(data_dict)
        load_to_snowflake(df, 'WEATHER_DATA')
    except Exception as e:
        print(f"Snowflake load failed - check connection maybe? Error: {str(e)}")
        raise

# Just two tasks now - keeping it simple with ELT
extract_task = PythonOperator(
    task_id='extract_weather_data',
    python_callable=extract_data,
    provide_context=True,
    dag=dag,
)

load_task = PythonOperator(
    task_id='load_weather_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag,
)

# Simple flow: extract -> load (transforming in Snowflake)
extract_task >> load_task
