#!/usr/bin/env python3
"""
ChainAnalytics Table Creation DAG
Creates Bronze, Silver, and Gold tables for the ChainAnalytics platform.
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'chainalytics-data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 15),
    'email': ['data-team@chainalytics.com', 'devops@chainalytics.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
    'execution_timeout': timedelta(hours=4),
}

dag = DAG(
    dag_id='chainalytics_table_setup',
    default_args=default_args,
    description='Creates Bronze, Silver, and Gold tables for ChainAnalytics data platform',
    schedule=None,
    catchup=False,
    max_active_runs=1,
    max_active_tasks=6,
    tags=['chainalytics', 'table-creation', 'setup', 'data-platform'],
)

start_setup = EmptyOperator(
    task_id='start_setup',
    dag=dag
)

create_bronze_tables = BashOperator(
    task_id='create_bronze_tables',
    bash_command='docker exec chainalytics-spark-master /opt/spark/bin/spark-submit /opt/spark/jobs/setup/create_bronze_tables.py',
    dag=dag,
)

create_silver_tables = BashOperator(
    task_id='create_silver_tables',
    bash_command='docker exec chainalytics-spark-master /opt/spark/bin/spark-submit /opt/spark/jobs/setup/create_silver_tables.py',
    dag=dag,
)

create_gold_tables = BashOperator(
    task_id='create_gold_tables',
    bash_command='docker exec chainalytics-spark-master /opt/spark/bin/spark-submit /opt/spark/jobs/setup/create_gold_tables.py',
    dag=dag,
)

end_setup = EmptyOperator(
    task_id='end_setup',
    dag=dag
)

# DAG execution flow
start_setup >> create_bronze_tables >> create_silver_tables >> create_gold_tables >> end_setup
