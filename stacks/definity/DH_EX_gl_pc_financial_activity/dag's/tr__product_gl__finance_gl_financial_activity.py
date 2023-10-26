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
gl_financial_activity_variables = Variable.get(
    "gl_variables", deserialize_json=True
)
query_path = gl_financial_activity_variables["query_path"]
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
def read_file_execute_query(ti, sql_file_fin_act):
    """
    function to read query from sql file and execute in Bigquery
    """
    execution_date = ti.xcom_pull(
        task_ids="gl_fin_act_get_execution_date_task", key="return_value"
    )
    bucket = storage_client.bucket(deployment_bucket_name)
    blob = bucket.get_blob(f"{sql_file_fin_act}")
    query = blob.download_as_string().decode("utf-8")
    query = (
        query.replace("@execution_date", execution_date)
        .replace("@curated_project", curated_project)
        .replace("@derived_project", derived_project)
    )
    query_job = bigquery_client.query(query).result


with DAG(
    "tr__product_gl__finance_gl_financial_activity",
    schedule_interval=None,
    tags=["gl", "daily"],
    start_date=datetime(2023, 3, 17),
    catchup=False,
) as dag:
    # Get execution_date variable
    gl_fin_act_get_execution_date_task = PythonOperator(
        task_id="gl_fin_act_get_execution_date_task",
        python_callable=get_execution_date,
        dag=dag,
    )
    # external dependencies
    waiting_for_tr__product_gwbc_report__fin_ctrl_rpt = ExternalTaskSensor(
        task_id="waiting_for_tr__product_gwbc_report__fin_ctrl_rpt",
        external_dag_id="tr__product_gwbc_report__fin_ctrl_rpt",
        external_task_id=None,
    )

    waiting_for_tr__product_gwpc_report__fin_ctrl_rpt = ExternalTaskSensor(
        task_id="waiting_for_tr__product_gwpc_report__fin_ctrl_rpt",
        external_dag_id="tr__product_gwpc_report__fin_ctrl_rpt",
        external_task_id=None,
    )

    waiting_for_in__gwbc__init_load = ExternalTaskSensor(
        task_id="waiting_for_in__gwbc__init_load",
        external_dag_id="in__gwbc__init_load",
        external_task_id=None,
    )

    # Truncate daily transaction table
    truncate_fin_act_daily_tran_tb_task = PythonOperator(
        task_id="truncate_fin_act_daily_tran_tb_task",
        python_callable=read_file_execute_query,
        op_kwargs={
            "sql_file_fin_act": (
                f"{query_path}truncate_daily_tran_tb_fin_act.sql"
            )
        },
        dag=dag,
    )

    # Insert into pc_daily transaction_table
    insert_into_fin_act_pc_daily_tran_tb_task = PythonOperator(
        task_id="insert_into_fin_act_pc_daily_tran_tb_task",
        python_callable=read_file_execute_query,
        op_kwargs={
            "sql_file_fin_act": (
                f"{query_path}insert_into_pc_daily_tran_tb_fin_act.sql"
            )
        },
        dag=dag,
    )

    # Load financial activity table with transaction type
    ld_fin_act_tb_with_tran_type_task = PythonOperator(
        task_id="ld_fin_act_tb_with_tran_type_task",
        python_callable=read_file_execute_query,
        op_kwargs={
            "sql_file_fin_act": (
                f"{query_path}ld_fin_activity_tb_tran_type.sql"
            )
        },
        dag=dag,
    )

    # Load financial activity table with cost type
    ld_fin_act_tb_with_cost_type_task = PythonOperator(
        task_id="ld_fin_act_tb_with_cost_type_task",
        python_callable=read_file_execute_query,
        op_kwargs={
            "sql_file_fin_act": (
                f"{query_path}ld_fin_activity_tb_cost_type.sql"
            )
        },
        dag=dag,
    )

(
    gl_fin_act_get_execution_date_task
    >> [
        waiting_for_tr__product_gwbc_report__fin_ctrl_rpt,
        waiting_for_tr__product_gwpc_report__fin_ctrl_rpt,
        waiting_for_in__gwbc__init_load,
    ]
    >> truncate_fin_act_daily_tran_tb_task
    >> insert_into_fin_act_pc_daily_tran_tb_task
    >> ld_fin_act_tb_with_tran_type_task
    >> ld_fin_act_tb_with_cost_type_task
)
