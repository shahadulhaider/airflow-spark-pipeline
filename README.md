# Airflow Spark ETL Pipeline for Open Library Data

This project implements an ETL (Extract, Transform, Load) pipeline using Apache Airflow and Apache Spark to process book data from the Open Library API and store it in a PostgreSQL database.

## Setup

1. Create and activate virtualenv
2. Install required packages: `pip install -r requirements.txt`
3. Set up Airflow: `airflow db init`
4. Set the project_root_path variable in Airflow: `airflow variables set project_root_path <project_root_path>`
5. Configure Airflow connections for Spark and PostgreSQL
6. Place the PostgreSQL JDBC driver in the jars directory

## Running the Pipeline

1. Start the Airflow webserver: airflow webserver
2. Start the Airflow scheduler: airflow scheduler
3. Trigger the DAG open_library_book_etl from the Airflow UI

## Pipeline Steps

1. Extract book data from Open Library API
2. Transform data using Spark
3. Create table in PostgreSQL
4, Load transformed data into PostgreSQL

## Notes

- Ensure all paths and connections are correctly configured
- The PostgreSQL JDBC driver is required for the load step
- For more detailed information, refer to the individual script files and the DAG definition.
