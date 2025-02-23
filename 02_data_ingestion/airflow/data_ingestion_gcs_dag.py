# Original Structure from the 2022 DTC Data Engineering Zoomcamp
# Modern updates were made with reference to:
# https://github.com/ManuelGuerra1987/data-engineering-zoomcamp-notes/blob/main/2_Workflow-Orchestration-AirFlow/airflow/dags/data_ingestion_gcp_yellow.py

import os
import logging
import shutil
import requests
import gzip

from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateExternalTableOperator
from airflow.providers.google.cloud.hooks.gcs import GCSHook

import pyarrow
import pyarrow.csv
import pyarrow.parquet

PROJECT_ID = os.environ.get("GCP_PROJECT_ID")
BUCKET = os.environ.get("GCP_GCS_BUCKET")

path_to_local_home = os.environ.get("AIRFLOW_HOME", "/opt/airflow/")

BIGQUERY_DATASET = os.environ.get("BIGQUERY_DATASET", 'demo_dataset')


def download(file_gz, file_csv, url):
    # Download the CSV.GZ file
    response = requests.get(url)
    if response.status_code == 200:
        with open(file_gz, 'wb') as f_out:
            f_out.write(response.content)
    else:
        print(f"Error downloading file: {response.status_code}")
        return False
    
    # Unzip the CSV file
    with gzip.open(file_gz, 'rb') as f_in:
        with open(file_csv, 'wb') as f_out:
            shutil.copyfileobj(f_in, f_out)

def format_to_parquet(src_file):

    parquet_file = src_file.replace('.csv', '.parquet')
    chunk_size=100000

    csv_reader = pyarrow.csv.open_csv(src_file, read_options=pyarrow.csv.ReadOptions(block_size=chunk_size))

    with pyarrow.parquet.ParquetWriter(parquet_file, csv_reader.schema) as writer:
        for batch in csv_reader:
            writer.write_batch(batch)    
            print("another chunk inserted")



def upload_to_gcs(bucket, object_name, local_file, gcp_conn_id="gcp-airflow"):
    hook = GCSHook(gcp_conn_id)
    hook.upload(
        bucket_name=bucket,
        object_name=object_name,
        filename=local_file,
        timeout=600
    )


default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "depends_on_past": False,
    "retries": 1,
}

# NOTE: DAG declaration - using a Context Manager (an implicit way)
with DAG(
    dag_id="data_ingestion_gcs_dag",
    schedule_interval="@daily",
    default_args=default_args,
    catchup=False,
    max_active_runs=1,
    tags=['dtc-de'],
) as dag:
    
    table_name_template = 'yellow_taxi_{{ execution_date.strftime(\'%Y_%m\') }}'
    file_template_csv_gz = 'output_{{ execution_date.strftime(\'%Y_%m\') }}.csv.gz'
    file_template_csv = 'output_{{ execution_date.strftime(\'%Y_%m\') }}.csv'
    file_template_parquet = 'output_{{ execution_date.strftime(\'%Y_%m\') }}.parquet'
    # consolidated_table_name = "yellow_{{ execution_date.strftime(\'%Y\') }}"
    url_template = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_{{ execution_date.strftime(\'%Y-%m\') }}.csv.gz"


    download_dataset_task = PythonOperator(
        task_id="download_dataset_task",
        python_callable=download,
        op_kwargs={
            'file_gz': file_template_csv_gz,
            'file_csv': file_template_csv,
            'file_parquet': file_template_parquet,
            'url': url_template
        },
    )

    format_to_parquet_task = PythonOperator(
        task_id="format_to_parquet_task",
        python_callable=format_to_parquet,
        op_kwargs={
            "src_file": f"{path_to_local_home}/{file_template_csv}",
        },
        retries=10,
    )

    local_to_gcs_task = PythonOperator(
        task_id="local_to_gcs_task",
        python_callable=upload_to_gcs,
        op_kwargs={
            "bucket": BUCKET,
            "object_name": f"raw/{file_template_parquet}",
            "local_file": f"{path_to_local_home}/{file_template_parquet}",
        },
    )

    bigquery_external_table_task = BigQueryCreateExternalTableOperator(
        task_id="bigquery_external_table_task",
        table_resource={
            "tableReference": {
                "projectId": PROJECT_ID,
                "datasetId": BIGQUERY_DATASET,
                "tableId": "external_table",
            },
            "externalDataConfiguration": {
                "sourceFormat": "PARQUET",
                "sourceUris": [f"gs://{BUCKET}/raw/{file_template_parquet}"],
            },
        },
    )

    download_dataset_task >> format_to_parquet_task >> local_to_gcs_task >> bigquery_external_table_task