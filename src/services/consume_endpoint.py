import requests
import pandas as pd
from google.cloud import bigquery
from faker import Faker
import random
from datetime import datetime

fake = Faker()

def fetch_data():
    url = "http://v0.ovapi.nl/line/"
    try:
        response = requests.get(url)
        response.raise_for_status()
        return response.json()
    except Exception as err:
        print(f"Error fetching data: {err}")
        return None

def process_data(data):
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
            "load_date": fake.date_between(start_date='-180d', end_date='today').strftime('%Y-%m-%d'),
            "updated_at": datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        }
        records.append(record)
    return records

def load_data_to_bigquery(records, project_id, dataset_id, table_id):
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    df = pd.DataFrame(records)
    
    # Ensure correct data types
    df['load_date'] = pd.to_datetime(df['load_date']).dt.date
    df['updated_at'] = pd.to_datetime(df['updated_at'])  # Ensure updated_at is a datetime
    
    if not df.empty:
        job = client.load_table_from_dataframe(df, table_ref, job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND))
        job.result()
        print(f"Loaded {job.output_rows} rows into {table_ref}.")
    else:
        print("No data to load into BigQuery.")

def main():
    project_id = "develop-431503"
    dataset_id = "transportation_netherlands"
    table_id = "ovapi"
    
    data = fetch_data()
    if data:
        records = process_data(data)
        load_data_to_bigquery(records, project_id, dataset_id, table_id)

if __name__ == "__main__":
    main()
