"""
Production-Ready Airflow DAG for Daily Sales Pipeline
Course: Automate Data Workflows with Airflow Excellence
Module 2: Production Implementation - Core Application & Assessment

This DAG demonstrates enterprise-grade workflow design principles
used by companies like Airbnb, Uber, and Netflix for production data pipelines.

NOTE FOR LEARNERS:
This lab runs in a simulated Apache Airflow environment.
Some APIs (such as schedule_interval, days_ago, and task-level SLA callbacks)
have been updated in newer Airflow versions.
"""

# ---------------------------------------------------
# UPDATED IMPORTS (Airflow 2.9+ compatible)
# ---------------------------------------------------
from airflow.sdk import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime, timedelta


# ===================================================
# PROVIDED CODE - DO NOT MODIFY
# ===================================================
def extract_sales_data(**context):
    """Simulate daily sales data extraction from multiple sources"""
    print("Extracting sales data from CRM, POS, and web analytics...")
    print("Data extraction completed successfully")
    return "sales_data_extracted"


def transform_customer_metrics(**context):
    """Transform and aggregate customer sales metrics"""
    print("Transforming sales data into customer metrics...")
    print("Calculating daily revenue, customer counts, and conversion rates...")
    return "metrics_transformed"


def load_to_warehouse(**context):
    """Load processed data to data warehouse"""
    print("Loading transformed metrics to data warehouse...")
    print("Data loaded successfully to production tables")
    return "data_loaded"


def validate_data_quality(**context):
    """Validate loaded data meets quality thresholds"""
    print("Running data quality checks...")
    print("All quality checks passed")
    return "validation_complete"


def send_slack_alert(context):
    """
    Simulated Slack notification for SLA misses.
    In a real production system, this would send a message using a Slack webhook.
    """
    print(
        f"SLA MISSED: DAG={context['dag'].dag_id}, "
        f"Tasks={context.get('task_list')}"
    )
    print("Alert sent to #data-engineering Slack channel")


# ===================================================
# PRACTICE CHALLENGE 1
# Configure default_args
# ===================================================
# TASK:
# Configure production-ready retry behavior:
# - 3 retries
# - 5-minute retry delay
# - Email alerts on failure
#
# NOTE:
# days_ago() is deprecated in newer Airflow versions,
# so we use explicit datetime arithmetic instead.

default_args = {
    'owner': 'data-engineering-team',
    'depends_on_past': False,
    'start_date': datetime.now() - timedelta(days=1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email_on_retry': False,
    'email': ['data-team@company.com']
}


# ===================================================
# PRACTICE CHALLENGE 2
# Define the DAG and scheduling
# ===================================================
# TASK:
# Create the DAG scheduled to run daily at 2 AM
#
# NOTE:
# In Airflow 2.9+, `schedule_interval` is replaced by `schedule`.

dag = DAG(
    dag_id='daily_sales_pipeline',
    default_args=default_args,
    description='Production-ready daily sales data pipeline with robust error handling',
    schedule='0 2 * * *',  # Daily at 2 AM
    start_date=datetime.now() - timedelta(days=1),
    catchup=False,
    tags=['production', 'sales', 'etl'],

    # NOTE:
    # SLA callbacks are configured at the DAG level in this simulated environment
    sla_miss_callback=send_slack_alert
)


# ===================================================
# PRACTICE CHALLENGE 3
# Configure SLA monitoring
# ===================================================
# TASK:
# - Extract task SLA: 30 minutes
# - Transform task SLA: 45 minutes
#
# NOTE:
# SLA durations are set at the task level.
# The SLA callback is handled by the DAG.

extract_task = PythonOperator(
    task_id='extract_sales_data',
    python_callable=extract_sales_data,
    dag=dag,
    sla=timedelta(minutes=30)
)

transform_task = PythonOperator(
    task_id='transform_customer_metrics',
    python_callable=transform_customer_metrics,
    dag=dag,
    sla=timedelta(minutes=45)
)

load_task = PythonOperator(
    task_id='load_to_warehouse',
    python_callable=load_to_warehouse,
    dag=dag
)

validate_task = PythonOperator(
    task_id='validate_data_quality',
    python_callable=validate_data_quality,
    dag=dag
)


# ===================================================
# Task dependencies
# ===================================================
# extract → transform → [load, validate]

extract_task >> transform_task >> [load_task, validate_task]