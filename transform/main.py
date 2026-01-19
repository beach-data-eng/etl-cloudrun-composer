import os
import pandas as pd
import io
from google.cloud import storage

BUCKET = os.environ["BUCKET"]
EXECUTION_TIME = os.environ["EXECUTION_TIME"]

def main():
    raw_path = f"data/raw/sales_{EXECUTION_TIME}.csv"
    agg_path = f"data/agg/sales_{EXECUTION_TIME}.csv"

    client = storage.Client()
    bucket = client.bucket(BUCKET)

    raw_data = bucket.blob(raw_path).download_as_text()
    df = pd.read_csv(io.StringIO(raw_data))

    agg = df.groupby("product", as_index=False).sum()

    bucket.blob(agg_path).upload_from_string(
        agg.to_csv(index=False),
        content_type="text/csv"
    )

    print(f"Aggregated data: {agg_path}")

if __name__ == "__main__":
    main()
