from airflow import models
from datetime import datetime, timedelta
from google.cloud import storage
from google.cloud import bigquery
from airflow import DAG
from airflow.exceptions import AirflowException
from airflow.models import Variable
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.operators.python import PythonOperator
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.models import Variable
import pytz

america_toronto = pytz.timezone('America/Toronto')

deployment_bucket_name = Variable.get("deployment_bucket_name")
raw_project_name = Variable.get("raw_project_name")
raw_bucket_name = Variable.get("raw_bucket_name")
curated_project = Variable.get("curated_project_name")
derived_project = Variable.get("derived_project_name")
gl_short_rate_earn_variables = Variable.get(
    "gl_variables", deserialize_json=True
)
query_path = gl_short_rate_earn_variables["query_path"]
databricks_conn_id = gl_short_rate_earn_variables["databricks_conn_id"]
google_service_account = gl_short_rate_earn_variables["google_service_account"]
bigquery_client = bigquery.Client()
storage_client = storage.Client()
bucket = storage_client.bucket(deployment_bucket_name)

policy_id = Variable.get("cdpm_policy_id")
node_type_id = "n2-standard-4"
driver_node_type_id = "n2-standard-4"
num_workers = 0
min_workers = 1
max_workers = 2
schedule_interval = "@once"
start_date = datetime(2023, 3, 18, 0, 0, 0)
max_active_runs = 1


new_cluster = {
        "spark_version": "10.4.x-scala2.12",
        "node_type_id": node_type_id,
        "driver_node_type_id": driver_node_type_id,
        "policy_id": policy_id,
        "max_active_runs": max_active_runs,
        "autoscale": {"min_workers": 1, "max_workers": 2},
        "custom_tags": {"TeamName": "CDPM"},
        "gcp_attributes": {
            "google_service_account": google_service_account,
            "zone_id": "HA",
            "use_preemptible_executors": False,
        },

}

# function to get execution date
def get_execution_date(**context):
    """
    function to get execution date
    """
    execution_date = (
            context["execution_date"].astimezone(america_toronto) + timedelta(days=-1)
    ).strftime("%Y-%m-%d")
    return execution_date


# function to read query from sql file and execute in Bigquery
def read_file_execute_query(ti, gl_short_rate_earn_sql_file):
    """
    function to read query from sql file and execute in Bigquery
    """
    execution_date = ti.xcom_pull(
        task_ids="gl_short_earn_get_execution_date_task", key="return_value"
    )
    bucket = storage_client.bucket(deployment_bucket_name)
    blob = bucket.get_blob(f"{gl_short_rate_earn_sql_file}")
    query = blob.download_as_string().decode("utf-8")
    query = (
        query.replace("@execution_date", execution_date)
            .replace("@curated_project", curated_project)
            .replace("@derived_project", derived_project)
    )
    query_job = bigquery_client.query(query).result


with DAG(
        'short_rate_earn',
        schedule_interval=None,
        tags=["gl", "daily"],
        start_date=datetime(2023, 3, 17),
        catchup=False
) as dag:


# Get execution_date variable
    gl_short_earn_get_execution_date_task = PythonOperator(
        task_id="gl_short_earn_get_execution_date_task",
        python_callable=get_execution_date,
        dag=dag,
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

# Insert into daily transaction table
    insert_into_short_rate_earn_daily_tran_tb_task = PythonOperator(
        task_id="insert_into_short_rate_earn_daily_tran_tb_task",
        python_callable=read_file_execute_query,
        op_kwargs={
            "gl_short_rate_earn_sql_file": f"{query_path}insert_into_daily_tran_tb_short_rate.sql"
        },
        dag=dag
    )

    spark_notebook_task = DatabricksSubmitRunOperator(
        databricks_conn_id="databricks_cdpm",
        task_id='create_fin_dt_from_fin_act_tran_task',
        new_cluster=new_cluster,
        notebook_task={
            'notebook_path': '/Shared/projects/gl/short_rate'
        }

    )

# Load GL financial detail table
    load_gl_financial_detail_tb_task = PythonOperator(
        task_id="load_gl_financial_detail_tb_task",
        python_callable=read_file_execute_query,
        op_kwargs={
            "gl_short_rate_earn_sql_file": f"{query_path}ld_gl_financial_detail_tb_short_rate.sql"
        },
        dag=dag
    )


# Truncate daily transaction table
    truncate_gl_short_rate_earn_daily_tran_tb_task = PythonOperator(
        task_id="truncate_gl_short_rate_earn_daily_tran_tb_task",
        python_callable=read_file_execute_query,
        op_kwargs={
            "gl_short_rate_earn_sql_file": f"{query_path}truncate_daily_tran_tb_short_rate.sql"
        },
        dag=dag
    )


(gl_short_earn_get_execution_date_task
>> waiting_for_tr__product_gl__finance_gl_financial_activity
>> insert_into_short_rate_earn_daily_tran_tb_task
>> spark_notebook_task
>> load_gl_financial_detail_tb_task
>> truncate_gl_short_rate_earn_daily_tran_tb_task)

