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
query_path = "transformation_pipeline/gl/bigquery/dml"

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


def get_execute_query(pc_summary_sql_file, **context):
    """
    function to read and execute query in Bigquery
    """
    execution_date = context["execution_date"] + timedelta(days=-1)
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
        blob = bucket.get_blob(pc_summary_sql_file)
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
                .replace("@summary_startdate", processed_startdate)
                .replace("@summary_enddate", processed_enddate)
        )
        print("Query submitted for execution in BigQuery")
        query_job = bigquery_client.query(query)
        query_job.result()

    except Exception:
        logging.error("Error in reading/executing query")
        raise AirflowException("Error in reading/executing query")


with DAG(
        'tr__product_gl__pc_summary',
        schedule_interval = None,
        start_date = datetime(2023, 3, 27),
        catchup = False
) as dag:

# transform and insert into finance_gl_summary_staging table
    insert_into_pc_summ_fin_gl_summ_staging_tb_task = PythonOperator(
        task_id = "insert_into_pc_summ_fin_gl_summ_staging_tb_task",
        python_callable = get_execute_query,
        op_kwargs = {
            "pc_summary_sql_file":"transformation_pipeline/gl/bigquery/dml/tb_input_fin_gl_summ_staging_pc_summary.sql"
        },
        dag = dag
    )

# transform data to partitioned parquet file and load into finance_gl_summary table
    insert_into_pc_summ_fin_gl_summ_tb_task = PythonOperator(
        task_id = "insert_into_pc_summ_fin_gl_summ_tb_task",
        python_callable = get_execute_query,
        op_kwargs = {
            "pc_summary_sql_file":"transformation_pipeline/gl/bigquery/dml/transform_data_to_partitioned_parquet_pc_summary.sql"
        },
        dag = dag
    )


insert_into_pc_summ_fin_gl_summ_staging_tb_task \
>> insert_into_pc_summ_fin_gl_summ_tb_task