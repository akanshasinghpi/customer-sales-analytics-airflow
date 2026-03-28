from airflow import DAG
#from airflow.operators.python import PythonOperator
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime
import sys
sys.path.append("/opt/airflow/customer-sales-analytics-pipeline")
import os
from etl.extract import extract_data
from etl.transform import transform_data
from etl.load import load_data

def run_pipeline():
    df = extract_data("/opt/airflow/customer-sales-analytics-pipeline/data/raw/sales_data.csv")
    clean_df, customer_metrics = transform_data(df)
    load_data(clean_df, customer_metrics)

def run_report():
    import subprocess
    subprocess.run(["python3", "/opt/airflow/customer-sales-analytics-pipeline/reporting/generate_report.py"])

def run_charts():
    import subprocess
    subprocess.run(["python3", "/opt/airflow/customer-sales-analytics-pipeline/visualization/create_charts.py"])

default_args = {
    'owner': 'user',
    'start_date': datetime(2024, 1, 1),
}

dag = DAG(
    'customer_sales_pipeline',
    default_args=default_args,
    schedule='@daily',
    catchup=False
)

report_task = PythonOperator(
    task_id='report',
    python_callable=run_report,
    dag=dag
)

run_pipeline_task = PythonOperator(
    task_id='run_etl_pipeline',
    python_callable=run_pipeline,
    dag=dag
)

charts_task = PythonOperator(
    task_id='charts',
    python_callable=run_charts,
    dag=dag
)
