import csv
import io
import logging
from datetime import datetime, timedelta
from google.cloud import storage
from google.cloud import bigquery
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.operators.python_operator import PythonOperator

# Pulling Values from the Variables defined in Airflow
raw_project_name = Variable.get("raw_project_name")
raw_bucket_name = Variable.get("raw_bucket_name")
curated_project = Variable.get("curated_project_name")
derived_project = Variable.get("derived_project_name")
composer_bucket_name = Variable.get("composer_bucket_name")
deployment_bucket_name = Variable.get("deployment_bucket_name")
query_path = "ingestion_pipeline/gl/bigquery/dml/pc_to_bc_init_recon"


# GCS client
storage_client = storage.Client(project=raw_project_name)

# Bigquery client
bigquery_client = bigquery.Client(project=curated_project)

execution_time = (
    datetime.now()
    .strftime("%Y-%m-%d%H:%M:%S.%f")[:-5]
    .replace("-", "")
    .replace(":", "")
    .replace(".", ""))
   

def get_execute_query(sql_file_path, **context):
    """
    function to read and execute query in Bigquery
    """
    execution_date = context["execution_date"]
    scheduler_year_scheduler_month = execution_date.strftime("%Y_%m")
    processed_startdate = (execution_date + timedelta(days=1)).strftime(
        "%Y-%m-%d"
    )
    processed_enddate = (execution_date + timedelta(days=1)).strftime(
        "%Y-%m-%d"
    )
    schedule_date = execution_date.strftime("%Y-%m-%d")
    lcv_execution_time = execution_time
    try:
        bucket = storage_client.bucket(deployment_bucket_name)
        blob = bucket.get_blob(sql_file_path)
        query = blob.download_as_string().decode("utf-8")
        query = (
            query.replace("@curated_project", curated_project)
            .replace("@derived_project", derived_project)
            .replace("@schedule_date", schedule_date)
            .replace("@lcv_execution_time", lcv_execution_time)
            .replace(
                "@scheduler_year_@scheduler_month",
                scheduler_year_scheduler_month,
            )
            .replace("@processed_startdate", processed_startdate)
            .replace("@processed_enddate", processed_enddate)
        )
        print("Query submitted for execution in BigQuery")
        query_job = bigquery_client.query(query)
        query_job.result()

    except Exception:
        logging.error("Error in reading/executing query")
        raise AirflowException("Error in reading/executing query")


def error_count_check(sql_file_path, **context):
    execution_date = context["execution_date"]
    scheduler_year_scheduler_month = execution_date.strftime("%Y_%m")
    lcv_execution_time = execution_time
    try:
        bucket = storage_client.bucket(deployment_bucket_name)
        blob = bucket.get_blob(sql_file_path)
        query = blob.download_as_string().decode("utf-8")
        query = (
            query.replace("@curated_project", curated_project)
            .replace("@derived_project", derived_project)
            .replace("@lcv_execution_time", lcv_execution_time)
            .replace(
                "@scheduler_year_@scheduler_month",
                scheduler_year_scheduler_month,
            )
        )
        print("Query submitted for execution in BigQuery")
        query_job = bigquery_client.query(query)
        result = query_job.result()
        rows = result.to_dataframe()
        print("length of dataframe: ", len(rows.index))
        if len(rows.index) != 0:
            error_flag = True
            # raise AirflowException("Error rows present for current run")

    except Exception:
        logging.error("Error in reading/executing query")
        raise AirflowException("Error in reading/executing query")
    else:
        if error_flag:
            raise AirflowException("Error rows present for current run")


with DAG(
    dag_id="tr__product_gwbc_report__pc_to_bc_init_recon",
    schedule_interval=None,
    start_date=datetime(2023, 3, 25),
    catchup=False,
    max_active_runs=1,
) as dag:
    
    pc_to_bc_init_curated = PythonOperator(
        task_id="pc_to_bc_init_curated",
        python_callable=get_execute_query,
        op_kwargs={
            "sql_file_path": (
                f"{query_path}/insert_into_gwbc_pc_to_bc_init_recon.sql"
            ),
        },
        dag=dag,
        )

    pc_to_bc_init_derived = PythonOperator(
        task_id="pc_to_bc_init_derived",
        python_callable=get_execute_query,
        op_kwargs={
            "sql_file_path": (
                f"{query_path}/insert_into_product_gwbc_report_pc_to_bc_init_recon.sql"
            ),
        },
        dag=dag,
        )

    error_count_check = PythonOperator(
        task_id="error_count_check",
        python_callable=error_count_check,
        op_kwargs={"sql_file_path": f"{query_path}/pc_to_bc_init_recon_error_count_check.sql",},
        dag=dag,
        )

(
    pc_to_bc_init_curated
    >> pc_to_bc_init_derived
    >> error_count_check
)