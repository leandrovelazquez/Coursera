from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from airflow.models import Variable

default_args = {

    'owner': 'data-engineering',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'sla': timedelta(hours=2),
}

dag = DAG(
    'sales_analytics_pipeline',
    default_args=default_args,
    description='Daily sales analytics pipeline with robust error handling and monitoring',
    schedule_interval='0 6 * * *', # Daily at 6 AM UTC
    catchup=False,
    max_active_runs=1,
    tags = ['sales', 'analytics', 'production']
)

#Simulate data extraction logic

if environment == 'production':
    data_source = Variable.get(""prod_database_connection"")
else:
    data_source = Variable.get(""dev_database_connection"")

print(f""Using data source: {data_source}"")

return f""sales_data_(excecution_date).json""

#This function demonstrates parameterization in action. It uses airflow variables to adapt behaviour based on the deployed environment, implementing our enviromnet -agnos

#Creating tasks with dependencies

# Task 1: Data Extraction

extract_task = PythonOperator(
    task_id='extract_sales_data',
    python_callable=extract_sales_data,
    dag = dag
)

# Task 2: Data Validation

validate_task = BashOperator(
    task_id='validate_data_quality',
    bash_command= """"""
    echo ""Validating data quality for {{ds}}""
    
    #Add data validation commands here 
    """""",

    dag = dag
)

# Task 3: Analytics Processing

process_task = PythonOperator(
    task_id = 'process_analytics',
    python_callable = lambda: print(""Processing sales analytics with robust error handling""),
    dag = dag
)

extract_task >> validate_task >> process_task

def sla_miss_callback(dag, task_list, blocking_task_list, slas, blocking_tis):
    
    """""Handle SLA violations with appropriate alerting."""""
    print(f""SLA missed for tasks: {[t.task_id for t in task_list]}"")

    #Add alerting logic here (Slack, email, PagerDuty)
    # Update DAG with SLA callback

    dag.sla_miss_callback = sla_miss_callback
    
