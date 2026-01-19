from airflow import DAG
from airflow.providers.google.cloud.operators.cloud_run import CloudRunExecuteJobOperator
from datetime import timedelta
from airflow.utils.dates import days_ago

PROJECT_ID = "iconic-balm-484806-h5"
REGION = "us-central1"
BUCKET = "beach-etl-bucket"

default_env = {
    "BUCKET": BUCKET,
    "EXECUTION_TIME": "{{ ts_nodash }}",
    "PROJECT_ID": PROJECT_ID,
    "BQ_DATASET": "beach_demo_etl",
    "BQ_TABLE": "sales_etl"
}

with DAG(
    dag_id="etl_cloudrun_full",
    start_date=days_ago(1),
    schedule_interval="* * * * *",
    catchup=False,
) as dag:

    extract = CloudRunExecuteJobOperator(
        task_id="extract",
        project_id=PROJECT_ID,
        region=REGION,
        job_name="extract-job",
        overrides={"container_overrides": [{"env": [
            {"name": k, "value": v} for k, v in default_env.items()
        ]}]}
    )

    transform = CloudRunExecuteJobOperator(
        task_id="transform",
        project_id=PROJECT_ID,
        region=REGION,
        job_name="transform-job",
        overrides={"container_overrides": [{"env": [
            {"name": k, "value": v} for k, v in default_env.items()
        ]}]}
    )

    load = CloudRunExecuteJobOperator(
        task_id="load",
        project_id=PROJECT_ID,
        region=REGION,
        job_name="load-job",
        overrides={"container_overrides": [{"env": [
            {"name": k, "value": v} for k, v in default_env.items()
        ]}]}
    )

    extract >> transform >> load
