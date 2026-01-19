import os
import pandas as pd
from google.cloud import bigquery, storage
import io

PROJECT = os.environ["PROJECT_ID"]
DATASET = os.environ["BQ_DATASET"]
TABLE = os.environ["BQ_TABLE"]
BUCKET = os.environ["BUCKET"]
EXECUTION_TIME = os.environ["EXECUTION_TIME"]

def main():
    agg_path = f"data/agg/sales_{EXECUTION_TIME}.csv"

    storage_client = storage.Client()
    data = storage_client.bucket(BUCKET).blob(agg_path).download_as_text()

    df = pd.read_csv(io.StringIO(data))

    client = bigquery.Client()
    table_id = f"{PROJECT}.{DATASET}.{TABLE}"

    job = client.load_table_from_dataframe(df, table_id)
    job.result()

    print(f"Loaded data to {table_id}")

if __name__ == "__main__":
    main()
