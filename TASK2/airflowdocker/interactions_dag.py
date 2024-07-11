from datetime import datetime, timedelta
import pandas as pd
import os
import sqlite3
from airflow import DAG
from airflow.operators.python import task
from airflow.utils.dates import days_ago
from airflow.operators.dummy import DummyOperator

# Default arguments for the DAG
default_args = {
    'owner': 'iCustomerDataEngineeringTeam',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}



dag = DAG(
    dag_id = 'etl_process_taskflow_api',
    default_args=default_args,
    description='ETL Process with TaskFlow API',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2024, 7, 7),
    catchup=False,
    tag = ['data_eng']
) 

start = DummyOperator(task_id ='start')

@task(task='init_task',dag=dag)
def init_object():
    import json
    import ETLProcess
    csv_file = 'interaction_data.csv'
    db_file = 'iCustomerDB.db'
    etl_process = ETLProcess(csv_file, db_file)
    

@task(task='ingest_data',dag=dag)
def ingest_data():
    etl_process.ingest_data()

# Define functions for each task
@task(task='ingest_data',dag=dag)
def ingest_data():
    etl_process.ingest_data()

@task(task='clean_data',dag=dag)
def clean_data():
    etl_process.clean_data()

@task(task='transform_data',dag=dag)
def transform_data():
    etl_process.transform_data()
    
@task(task='load_data',dag=dag)
def load_data():
    etl_process.load_data()

end = DummyOperator(task_id ='end')



# Define the tasks
ingest_task = ingest_data()
clean_task = clean_data()
transform_task = transform_data()
load_task = load_data()

# Set the task dependencies
start >> ingest_task >> clean_task >> transform_task >> load_task >> end
