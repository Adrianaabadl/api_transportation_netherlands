import requests
import pandas as pd
from google.cloud import bigquery

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
            "ID": key,
            "TransportType": value.get("TransportType", "N/A"),
            "LineName": value.get("LineName", "N/A"),
            "LinePublicNumber": value.get("LinePublicNumber", "N/A"),
            "DataOwnerCode": value.get("DataOwnerCode", "N/A"),
            "DestinationName50": value.get("DestinationName50", "N/A"),
            "LinePlanningNumber": value.get("LinePlanningNumber", "N/A"),
            "LineDirection": value.get("LineDirection", "N/A")
        }
        records.append(record)

    return pd.DataFrame(records)

def load_data_to_bigquery(df, project_id, dataset_id, table_id):
    client = bigquery.Client(project=project_id)
    table_ref = f"{project_id}.{dataset_id}.{table_id}"
    if not df.empty:
        job = client.load_table_from_dataframe(df, table_ref, job_config=bigquery.LoadJobConfig(
            write_disposition=bigquery.WriteDisposition.WRITE_APPEND))
        job.result() 
        print(f"Loaded {job.output_rows} rows into {table_ref}.")
    else:
        print("No data to load into BigQuery.")

def main():
    project_id = "your_project_id"
    dataset_id = "your_dataset_id"
    table_id = "your_table_id"
    
    data = fetch_data()
    if data:
        df = process_data(data)
        load_data_to_bigquery(df, project_id, dataset_id, table_id)

if __name__ == "__main__":
    main()
