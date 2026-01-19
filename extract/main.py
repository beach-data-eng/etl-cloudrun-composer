import os
import pandas as pd
import numpy as np
from google.cloud import storage
from datetime import datetime

BUCKET = os.environ["BUCKET"]
EXECUTION_TIME = os.environ["EXECUTION_TIME"]

def main():
    df = pd.DataFrame({
        "product": np.random.choice(["A", "B", "C"], 10),
        "amount": np.random.randint(100, 1000, 10)
    })

    path = f"data/raw/sales_{EXECUTION_TIME}.csv"

    client = storage.Client()
    bucket = client.bucket(BUCKET)
    bucket.blob(path).upload_from_string(
        df.to_csv(index=False),
        content_type="text/csv"
    )

    print(f"Generated raw data: {path}")

if __name__ == "__main__":
    main()
