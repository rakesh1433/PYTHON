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
gl_backdated_earned_variables = Variable.get(
    "gl_variables", deserialize_json=True
)
query_path = gl_backdated_earned_variables["query_path"]
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
def read_file_execute_query(ti, pc_dt_backdated_earn_sql_file):
    """
    function to read query from sql file and execute in Bigquery
    """
    execution_date = ti.xcom_pull(
        task_ids="pc_dt_backdated_earn_get_execution_date_task",
        key="return_value",
    )
    bucket = storage_client.bucket(deployment_bucket_name)
    blob = bucket.get_blob(f"{pc_dt_backdated_earn_sql_file}")
    query = blob.download_as_string().decode("utf-8")
    query = (
        query.replace("@execution_date", execution_date)
        .replace("@curated_project", curated_project)
        .replace("@derived_project", derived_project)
    )
    query_job = bigquery_client.query(query).result


with DAG(
    "tr__product_gl__backdated_earned",
    schedule_interval=None,
    tags=["gl", "daily"],
    start_date=datetime(2023, 3, 17),
    catchup=False,
) as dag:
    # Get execution_date variable
    pc_dt_backdated_earn_get_execution_date_task = PythonOperator(
        task_id="pc_dt_backdated_earn_get_execution_date_task",
        python_callable=get_execution_date,
        dag=dag,
    )

    # external dependencies
    waiting_for_tr__product_gl__pc_detail_earned = ExternalTaskSensor(
        task_id="waiting_for_tr__product_gl__pc_detail_earned",
        external_dag_id="tr__product_gl__pc_detail_earned",
        external_task_id=None,
    )

    waiting_for_tr__product_gl__finance_gl_financial_activity = (
        ExternalTaskSensor(
            task_id=(
                "waiting_for_tr__product_gl__finance_gl_financial_activity"
            ),
            external_dag_id="tr__product_gl__finance_gl_financial_activity",
            external_task_id=None,
        )
    )

    # Insert into pc daily transaction table
    insert_into_backdated_earn_pc_daily_tran_tb_task = PythonOperator(
        task_id="insert_into_backdated_earn_pc_daily_tran_tb_task",
        python_callable=read_file_execute_query,
        op_kwargs={
            "pc_dt_backdated_earn_sql_file": (
                f"{query_path}insert_into_daily_tran_tb_bckdtd_earn.sql"
            )
        },
        dag=dag,
    )

    # Reverse earned premiums for prior trans eff after current
    reverse_earned_prem_for_prior_trans_eff_task = PythonOperator(
        task_id="reverse_earned_prem_for_prior_trans_eff_task",
        python_callable=read_file_execute_query,
        op_kwargs={
            "pc_dt_backdated_earn_sql_file": (
                f"{query_path}rvrs_earnd_prm_prior_tran_eff_bckdtd_earn.sql"
            )
        },
        dag=dag,
    )

    # Reverse written premiums for prior trans eff after current
    reverse_written_prem_for_prior_trans_eff_task = PythonOperator(
        task_id="reverse_written_prem_for_prior_trans_eff_task",
        python_callable=read_file_execute_query,
        op_kwargs={
            "pc_dt_backdated_earn_sql_file": (
                f"{query_path}rvrs_wrtn_prem_prior_tran_bckdtd_earn.sql"
            )
        },
        dag=dag,
    )

    # Truncate daily transaction table
    truncate_gl_pc_bckdated_earn_daily_tran_tb_task = PythonOperator(
        task_id="truncate_gl_pc_bckdated_earn_daily_tran_tb_task",
        python_callable=read_file_execute_query,
        op_kwargs={
            "pc_dt_backdated_earn_sql_file": (
                f"{query_path}truncate_daily_tran_tb_bckdtd_earn.sql"
            )
        },
        dag=dag,
    )


(
    pc_dt_backdated_earn_get_execution_date_task
    >> [
        waiting_for_tr__product_gl__pc_detail_earned,
        waiting_for_tr__product_gl__finance_gl_financial_activity,
    ]
    >> insert_into_backdated_earn_pc_daily_tran_tb_task
    >> reverse_earned_prem_for_prior_trans_eff_task
    >> reverse_written_prem_for_prior_trans_eff_task
    >> truncate_gl_pc_bckdated_earn_daily_tran_tb_task
)
