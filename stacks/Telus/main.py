from pyspark.sql import SparkSession
from datetime import datetime, timedelta
import sys
import calendar

# Initialize Spark session
spark = SparkSession.builder \
    .appName("Download BigQuery Data and Export to GCS") \
    .config("spark.speculation", "false") \
    .getOrCreate()

# Set your BigQuery project, dataset, and table names
#project_id = "bi-stg-tps-telport-pr-070f9e"
dataset_name = "tps_teleport_bq_nne1_audit_dataset"
tenant_table_name = "bq_tenant_table"

# Calculate the date range for the previous month
today = datetime.now()
last_month = today.replace(day=1) - timedelta(days=1)
first_day_of_last_month = last_month.replace(day=1)
last_day_of_last_month = last_month

start_date_str = first_day_of_last_month.strftime("%Y-%m-%d")
end_date_str = last_day_of_last_month.strftime("%Y-%m-%d")

# Set your Google Cloud Storage (GCS) bucket name and folder path name
gcs_bucket = sys.argv[1]  # Replace with your GCS bucket name
month_name = calendar.month_abbr[last_month.month]
gcs_folder = f"{month_name} {last_month.year}"

# Retrieve the distinct tenant names from BigQuery table
distinct_tenants_query = f"""
    SELECT DISTINCT(tenant) FROM df_view
"""

# Read the data from BigQuery
distinct_tenants_df = spark.read.format("bigquery") \
    .option("dataset", dataset_name) \
    .option("table", tenant_table_name) \
    .load()

distinct_tenants_df.createOrReplaceTempView("df_view")
# Execute the query to get distinct tenants
distinct_tenants_df = spark.sql(distinct_tenants_query)
distinct_tenants_df.show()

# Convert DataFrame to a list of tenant name
tenants = [row.tenant for row in distinct_tenants_df.collect()]
tables = [
        {
            "dataset_name": "tps_teleport_bq_nne1_ccp_dataset",
            "table_name": "bq_prepaid_dashboard_view",
            "environment": "prepaid",
            "stage": "production",
            "sql_query": f"""
                
                select * from df_table_view
                    WHERE eventStartDate >= DATE('{start_date_str}') AND eventStartDate <= DATE('{end_date_str}')
                    AND Partner = """
        },
        {
            "dataset_name": "tps_teleport_bq_nne1_ccp_dataset",
            "table_name": "bq_postpaid_dashboard_view",
            "environment": "postpaid",
            "stage": "production",
            "sql_query": f"""
                SELECT * FROM df_table_view
                    WHERE eventStartDate >= DATE('{start_date_str}') AND eventStartDate <= DATE('{end_date_str}')
                    AND Partner = """
        },
        {
            "dataset_name": "tps_stg_teleport_bq_nne1_ccp_dataset_stg",
            "table_name": "bq_prepaid_dashboard_stg_view",
            "environment": "prepaid",
            "stage": "staging",
            "sql_query": f"""
                select * FROM df_table_view
                WHERE eventStartDate >= DATE('{start_date_str}') AND eventStartDate <= DATE('{end_date_str}')
                AND Partner = """
        },
        {
            "dataset_name": "tps_stg_teleport_bq_nne1_ccp_dataset_stg",
            "table_name": "bq_postpaid_dashboard_stg_view",
            "environment": "postpaid",
            "stage": "staging",
            "sql_query": f"""
                select * FROM df_table_view
                    WHERE eventStartDate >= DATE('{start_date_str}') AND eventStartDate <= DATE('{end_date_str}')
                    AND Partner = """
        }
    ]

for table_info in tables:
    # Loop through the list of tables for the current tenant
    dataset_name = table_info["dataset_name"]
    table_name = table_info["table_name"]
    environment = table_info["environment"]
    stage = table_info["stage"]
    sql_query = table_info["sql_query"] 
    # Execute the SQL query using Spark SQL
    df_table = spark.read \
        .format("bigquery") \
        .option("dataset", dataset_name) \
        .option("table", table_name) \
        .option("viewsEnabled", "true") \
        .load()
    df_table.createOrReplaceTempView("df_table_view")
    for tenant in tenants: 
    # Check if there is any data for the table within the date range
        sql_query_new = sql_query + f"'{tenant}'"
        df_table = spark.sql(sql_query_new)
        if not df_table.isEmpty():
            # Create a unique subfolder for each table's data
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            csv_file_name = f"{environment}_{stage}_Settlement_Report_{month_name}_{last_month.year}_{tenant}.csv"
            csv_output_path = f"gs://{gcs_bucket}/{gcs_folder}/{table_name}/{csv_file_name}"

            # Export DataFrame to GCS as a CSV files
            print(f"Writing to GCS: {csv_output_path}")
            df_table.write.option("header", True).csv(csv_output_path)

# Stop the Spark session
spark.stop()
