from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
import requests
import pandas as pd
from google.cloud import bigquery
import random
from datetime import datetime, timedelta

def fetch_data():
    url = "http://v0.ovapi.nl/line/"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except Exception as err:
        print(f"Error fetching data: {err}")
        return None

def process_data(**kwargs):
    data = kwargs['ti'].xcom_pull(task_ids='fetch_data')
    records = []
    for key, value in data.items():
        record = {
            "id": key,
            "transport_type": value.get("TransportType", "N/A"),
            "line_name": value.get("LineName", "N/A"),
            "line_public_number": value.get("LinePublicNumber", "N/A"),
            "data_owner_code": value.get("DataOwnerCode", "N/A"),
            "destination_name_50": value.get("DestinationName50", "N/A"),
            "line_planning_number": value.get("LinePlanningNumber", "N/A"),
            "line_direction": str(value.get("LineDirection", "N/A")),
            "load_date": (datetime.now() - timedelta(days=random.randint(0, 180))).strftime('%Y-%m-%d'),  # Generate a random load_date within the last 180 days
            "updated_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S') # Record the timestamp of the update
        }
        records.append(record)
    return records

def load_data_to_bigquery(records, project_id, dataset_id, table_id):
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    df = pd.DataFrame(records)
    df['load_date'] = pd.to_datetime(df['load_date']).dt.date
    df['updated_at'] = pd.to_datetime(df['updated_at']).dt.date

    # Check for existing records in BigQuery
    existing_ids = set()
    query = f"SELECT id FROM `{project_id}.{dataset_id}.{table_id}` WHERE id IN UNNEST(@ids)"
    job_config = bigquery.QueryJobConfig(
        query_parameters=[
            bigquery.ArrayQueryParameter("ids", "STRING", df['id'].tolist())
        ]
    )
    query_job = client.query(query, job_config=job_config)
    
    for row in query_job.result():
        existing_ids.add(row.id)

    # Filter out records that already exist
    new_records = df[~df['id'].isin(existing_ids)]

    if not new_records.empty:
        job = client.load_table_from_dataframe(new_records, table_ref, job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND))
        job.result()
        print(f"Loaded {job.output_rows} rows into {table_ref}.")
    else:
        print("No new data to load into BigQuery.")

def bigquery_task(**kwargs):
    # Pull processed records from XCom
    records = kwargs['ti'].xcom_pull(task_ids='process_data')
    project_id = "develop-431503"
    dataset_id = "transportation_netherlands"
    table_id = "ovapi"
    load_data_to_bigquery(records, project_id, dataset_id, table_id)

# Define the DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
}

dag = DAG(
    'fetch_and_load_data',
    default_args=default_args,
    schedule_interval='@daily',  # Change this according to your needs
)

# Define the tasks
fetch_data_task = PythonOperator(
    task_id='fetch_data',
    python_callable=fetch_data,
    dag=dag,
)

process_data_task = PythonOperator(
    task_id='process_data',
    python_callable=process_data,
    provide_context=True,
    dag=dag,
)

load_data_task = PythonOperator(
    task_id='load_data_to_bigquery',
    python_callable=bigquery_task,
    provide_context=True,
    dag=dag,
)

# Set task dependencies
fetch_data_task >> process_data_task >> load_data_task
