"""Advanced Retry Strategies for Production Resilience

Production environments require sophisticated retry strategies that go beyond basic attempt counts. Effective retry implementations consider failure types,
resource availability, and business impact to optimize recovery success while minimizing system load.

Exponential Backoff and Jitter Implementation

Simple retry intervals can create thundering herd problems when multiple failed tasks retry simultaneously, overwhelming already-struggling services. 
Exponential backoff with jitter solves this by spacing retry attempts over increasing, randomized intervals."""

from datetime import timedelta

from multiprocessing import context
import random

def custom_retry_delay(attempt_number):

    base_delay = 60 # 1 minute base

    max_delay = 3600 # 1 hour maximum

    exponential_delay = base_delay * (2**attempt_number)

    jittered_delay = exponential_delay + random.uniform(0, 30)

    return min(jittered_delay, max_delay)

default_args = {

'retries': 5,

'retry_exponential_backoff': True,

'retry_delay': timedelta(seconds=60),

'max_retry_delay': timedelta(hours=1)

}
"""
Netflix employs this pattern in their content analytics workflows, preventing failed recommendation model training tasks from overwhelming GPU clusters
 during recovery periods. The randomized jitter ensures that when external services recover, task retries don't immediately overload them again.

Task-Specific Retry Configuration

Different task types require different retry strategies based on their failure characteristics and business criticality. Data extraction tasks might retry aggressively for transient network issues, while machine learning model training tasks might use conservative retry policies due to expensive computational costs.

Task-level retry configuration overrides DAG defaults to provide appropriate recovery behavior:"""

critical_extract_task = PythonOperator(

task_id='extract_critical_data',

python_callable=extract_function,

retries=10, # More aggressive for critical paths

retry_delay=timedelta(minutes=2),

dag=dag

)

expensive_ml_task = PythonOperator(

task_id='train_recommendation_model',

python_callable=training_function,

retries=1, # Conservative for expensive operations

retry_delay=timedelta(hours=1),

dag=dag

)
"""
Uber differentiates retry strategies across their driver dispatch workflows, with location-based tasks retrying frequently due to GPS accuracy variations,
 while payment processing tasks use conservative retry policies to prevent duplicate charges.

SLA Monitoring and Proactive Alerting

Service Level Agreement monitoring transforms reactive troubleshooting into proactive operations management. Effective SLA implementation considers
both task-level and workflow-level performance expectations to provide appropriate alerting granularity.

Multi-Tier SLA Configuration

Production workflows benefit from tiered SLA monitoring that escalates alerts based on severity and business impact. 
Warning-level SLAs provide early indicators of performance degradation, while critical-level SLAs trigger immediate incident response.
"""

from datetime import datetime, timedelta

def sla_warning_callback(dag, task_list, blocking_task_list, slas, blocking_tis):

    """Handle SLA warnings with monitoring system integration"""

    for task in task_list:

        send_monitoring_alert(

        level='WARNING',

        message=f'Task {task.task_id} approaching SLA threshold',

        channel='#data-engineering-alerts'

)

def sla_critical_callback(dag, task_list, blocking_task_list, slas, blocking_tis):

    """Handle SLA violations with immediate escalation"""

    for task in task_list:

        page_oncall_engineer(

        task_id=task.task_id,

        execution_date=task.execution_date,

        severity='CRITICAL'

)

# Task with multi-tier SLA monitoring

data_processing_task = PythonOperator(

task_id='process_customer_analytics',

python_callable=process_data,

sla=timedelta(hours=2), # Critical threshold

on_success_callback=lambda context: log_performance_metrics(context),

dag=dag

)

"""
Airbnb implements sophisticated SLA monitoring across their data platform, with guest booking analytics workflows having 4-hour warning
SLAs and 6-hour critical SLAs to ensure business reporting availability for global operations teams.

Dynamic Parameterization for Environment Agnostic Deployment

Production workflows must adapt seamlessly across development, staging, and production environments without code modifications. 
Advanced parameterization strategies enable single DAG definitions that configure themselves based on deployment context and runtime conditions.

Environment-Aware Configuration Management

Airflow Variables provide centralized configuration management that supports environment-specific settings while maintaining code portability:
"""

from airflow.models import Variable

import json

def get_environment_config():

    """Load environment-specific configuration from Airflow Variables"""

    env = Variable.get("deployment_environment", default_var="development")

    config_mapping = {

    'development': {

    'database_connection': Variable.get("dev_database_url"),

    'batch_size': 1000,

    'enable_monitoring': False

    },

    'staging': {

    'database_connection': Variable.get("staging_database_url"),

    'batch_size': 5000,

    'enable_monitoring': True

    },

    'production': {

    'database_connection': Variable.get("prod_database_url"),

    'batch_size': 10000,

    'enable_monitoring': True

    }

    }

    return config_mapping.get(env, config_mapping['development'])

def parameterized_data_processing(context):

    """Data processing function with environment-aware configuration"""

    config = get_environment_config()

    # Use environment-specific settings

    connection = create_database_connection(config['database_connection'])

    process_data_batch(connection, batch_size=config['batch_size'])

    if config['enable_monitoring']:

        send_performance_metrics(context)

"""
PayPal's payment processing workflows demonstrate this pattern, using identical DAG logic across their global data centers with only configuration 
parameters changing between regions and regulatory environments.

Runtime Parameter Injection

Template variables and macro functions enable dynamic parameter injection at execution time, supporting date-based partitioning, 
conditional logic, and external system integration:
"""

parameterized_task = PythonOperator(

task_id='process_daily_transactions',

python_callable=process_transactions,

op_kwargs={

'date_partition': '{{ ds }}',

'environment': '{{ var.value.deployment_environment }}',

'retry_count': '{{ task_instance.try_number }}',

'upstream_success': '{{ task_instance.get_dagrun().get_task_instance("extract_data").state }}'

},

dag=dag

)
"""
Comprehensive Alerting and Incident Response Integration

Production workflows require integration with enterprise alerting systems to ensure appropriate incident response during failures and 
performance degradations. Effective alerting strategies balance notification frequency with actionable information to prevent alert 
fatigue while ensuring critical issues receive immediate attention.

Multi-Channel Alert Routing

Different failure types and business impacts require different notification channels and response urgency:
"""

def route_alert_by_severity(context, severity_level):

    """Route alerts to appropriate channels based on severity"""

    if severity_level == 'CRITICAL':

        # Page on-call engineer immediately

        send_pagerduty_alert(context)

        post_slack_message('#incident-response', context, urgent=True)

        send_email_alert(context, recipients=['data-engineering-leads@company.com'])

    elif severity_level == 'WARNING':

        # Notify team channels during business hours

        post_slack_message('#data-engineering-alerts', context)

        create_jira_ticket(context, priority='High')

    elif severity_level == 'INFO':

        # Log for monitoring and trend analysis

        send_datadog_metrics(context)

        log_to_elasticsearch(context)

def failure_callback(context):

    """Handle task failures with appropriate alerting"""

    task_id = context['task_instance'].task_id

    # Determine severity based on task criticality

    critical_tasks = ['process_payments', 'update_user_balances', 'generate_regulatory_reports']

    if task_id in critical_tasks:

        route_alert_by_severity(context, 'CRITICAL')

    else:

        route_alert_by_severity(context, 'WARNING')

"""
Spotify uses sophisticated alert routing in their music recommendation workflows, with user-facing features triggering immediate escalation while 
analytics and reporting workflows use business-hours notification patterns.

Key Takeaways

Production implementation success depends on layered resilience strategies that combine intelligent retry mechanisms, proactive SLA monitoring, 
comprehensive parameterization, and integrated alerting systems. These patterns work together to create workflows that handle enterprise-scale 
challenges while providing the observability and control needed for effective operations management.

When you implement these production patterns consistently, your workflows transition from basic automation scripts to enterprise-grade systems 
that business stakeholders depend on for critical operations and strategic decision-making.
"""