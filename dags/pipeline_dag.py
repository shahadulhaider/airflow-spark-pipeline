from datetime import datetime, timedelta
from airflow import DAG
from airflow.models import Variable
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.operators.python import PythonOperator
import requests
import csv
import os

project_root = Variable.get("project_root_path")

EXTRACTED_FILE_PATH = os.path.join(project_root, 'data', 'extracted_book_data.csv')
TRANSFORMED_FILE_PATH = os.path.join(project_root, 'data', 'transformed_book_data.csv')


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 28),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


dag = DAG(
    'open_library_book_etl',
    default_args=default_args,
    description='ETL pipeline for Open Library Book data using Airflow, Spark, and Postgres',
    schedule_interval=timedelta(days=1),
)


# Task to extract data from Open Library API
def extract_book_data(**kwargs):
    search_query = 'python programming'
    url = f'http://openlibrary.org/search.json?q={search_query}'
    response = requests.get(url)
    data = response.json()['docs'][:100] 
    
    # Save the data to a CSV file
    with open(EXTRACTED_FILE_PATH, 'w', newline='', encoding='utf-8') as csvfile:
        fieldnames = ['key', 'title', 'author_name', 'first_publish_year', 'language', 'number_of_pages_median']
        writer = csv.DictWriter(csvfile, fieldnames=fieldnames, extrasaction='ignore')
        writer.writeheader()
        for book in data:
            writer.writerow(book)
    
    # Push the file path to XCom for the next task
    kwargs['ti'].xcom_push(key='extracted_data_path', value=EXTRACTED_FILE_PATH)


extract_task = PythonOperator(
    task_id='extract_book_data',
    python_callable=extract_book_data,
    dag=dag,
)


# Task to transform data using Spark
transform_task = SparkSubmitOperator(
    task_id='transform_book_data',
    application=os.path.join(project_root, 'scripts', 'data_processing.py'),
    application_args=[EXTRACTED_FILE_PATH, TRANSFORMED_FILE_PATH],
    conn_id='spark_default',
    dag=dag,
)


# Task to create table in Postgres
create_table_task = PostgresOperator(
    task_id='create_book_table',
    postgres_conn_id='postgres_default',
    sql="""
    CREATE TABLE IF NOT EXISTS open_library_books (
        id SERIAL PRIMARY KEY,
        book_key VARCHAR(255),
        title TEXT,
        authors TEXT,
        publish_year INTEGER,
        languages TEXT,
        pages INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
    """,
    dag=dag,
)

# Task to load data into Postgres
load_task = SparkSubmitOperator(
    task_id='load_book_data',
    application=os.path.join(project_root, 'scripts', 'load_data.py'),
    application_args=[TRANSFORMED_FILE_PATH],
    conn_id='spark_default',
    jars=os.path.join(project_root, 'jars', 'postgresql-42.7.3.jar'),
    driver_class_path=os.path.join(project_root, 'jars', 'postgresql-42.7.3.jar'),
    env_vars={'PROJECT_ROOT': project_root},
    dag=dag,
)

# Set up task dependencies
extract_task >> transform_task >> create_table_task >> load_task

