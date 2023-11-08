from airflow import models
from datetime import datetime, timedelta
from google.cloud import storage
from google.cloud import bigquery
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import PythonOperator
import pytz

america_toronto = pytz.timezone('America/Toronto')

deployment_bucket_name = Variable.get("deployment_bucket_name")
raw_project_name = Variable.get("raw_project_name")
raw_bucket_name = Variable.get("raw_bucket_name")
curated_project = Variable.get("curated_project_name")
derived_project = Variable.get("derived_project_name")
gl_pc_detail_variables = Variable.get("gl_variables", deserialize_json=True)
query_path = gl_pc_detail_variables["query_path"]
bigquery_client = bigquery.Client()
storage_client = storage.Client()
bucket = storage_client.bucket(deployment_bucket_name)


# function to get execution date
def get_execution_date(**context):
    """
    function to get execution date
    """
    execution_date = (
            context["execution_date"].astimezone(america_toronto) + timedelta(days=-1)
    ).strftime("%Y-%m-%d")

    return str(execution_date)


# function to read query from sql file and execute in Bigquery
def read_file_execute_query(ti, sql_file_pc_detail):
    """
    function to read query from sql file and execute in Bigquery
    """
    execution_date = ti.xcom_pull(
        task_ids="gl_pc_detail_get_execution_date_task", key="return_value"
    )
    bucket = storage_client.bucket(deployment_bucket_name)
    blob = bucket.get_blob(f"{sql_file_pc_detail}")
    query = blob.download_as_string().decode("utf-8")
    query = (
        query.replace("@execution_date", execution_date)
        .replace("@curated_project", curated_project)
        .replace("@derived_project", derived_project)
    )
    query_job = bigquery_client.query(query).result


with DAG(
    "tr__product_gl__finance_gl_pc_detail",
    schedule_interval=None,
    tags=["gl", "daily"],
    start_date=datetime(2023, 3, 17),
    catchup=False,
) as dag:
    # Get execution_date variable
    gl_pc_detail_get_execution_date_task = PythonOperator(
        task_id="gl_pc_detail_get_execution_date_task",
        python_callable=get_execution_date,
        dag=dag,
    )

    # external dependencies
    waiting_for_tr__product_gl__finance_gl_financial_activity = (
        ExternalTaskSensor(
            task_id=(
                "waiting_for_tr__product_gl__finance_gl_financial_activity"
            ),
            external_dag_id="tr__product_gl__finance_gl_financial_activity",
            external_task_id=None,
        )
    )

    # Merge future dated transactions
    Merge_future_dated_transactions_task = PythonOperator(
        task_id="Merge_future_dated_transactions_task",
        python_callable=read_file_execute_query,
        op_kwargs={
            "sql_file_pc_detail": (
                f"{query_path}merge_future_dated_transactions_pc_detail.sql"
            )
        },
        dag=dag,
    )

    # Load current trans into GL financial detail table
    ld_current_trans_into_gl_fin_dt_tb_task = PythonOperator(
        task_id="ld_current_trans_into_gl_fin_dt_tb_task",
        python_callable=read_file_execute_query,
        op_kwargs={
            "sql_file_pc_detail": f"{query_path}ld_current_trans_into_gl_financial_detail_tb_pc_detail.sql"
        },
        dag=dag,
    )

    # Load future dated trans into GL financial detail table
    ld_future_dated_trans_into_gl_fin_dt_tb_task = PythonOperator(
        task_id="ld_future_dated_trans_into_gl_fin_dt_tb_task",
        python_callable=read_file_execute_query,
        op_kwargs={
            "sql_file_pc_detail": f"{query_path}ld_future_dated_trans_gl_financial_detail_tb_pc_detail.sql"
        },
        dag=dag,
    )

    # Load new future dated trans table
    ld_new_future_dated_trans_tb_task = PythonOperator(
        task_id="ld_new_future_dated_trans_tb_task",
        python_callable=read_file_execute_query,
        op_kwargs={
            "sql_file_pc_detail": (
                f"{query_path}ld_new_future_dated_tran_tb_pc_detail.sql"
            )
        },
        dag=dag,
    )


(
    gl_pc_detail_get_execution_date_task
    >> waiting_for_tr__product_gl__finance_gl_financial_activity
    >> Merge_future_dated_transactions_task
    >> ld_current_trans_into_gl_fin_dt_tb_task
    >> ld_future_dated_trans_into_gl_fin_dt_tb_task
    >> ld_new_future_dated_trans_tb_task
)
