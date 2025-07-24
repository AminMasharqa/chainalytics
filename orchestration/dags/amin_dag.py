#!/usr/bin/env python3
"""
Simple DAG for ChainAnalytics Complete Data Platform Setup
Creates Bronze, Silver, and Gold tables in warehouse catalog
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'chainalytics-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 7, 20),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=2),
    'execution_timeout': timedelta(minutes=30),
}

dag = DAG(
    dag_id='amin_dag',
    default_args=default_args,
    description='Complete warehouse setup: Bronze, Silver, Gold tables',
    schedule=None,  # Manual trigger
    catchup=False,
    max_active_runs=1,
    tags=['warehouse', 'bronze', 'silver', 'gold', 'setup'],
)

start_task = EmptyOperator(
    task_id='start_setup',
    dag=dag
)

# Create bronze tables
create_bronze_tables = BashOperator(
    task_id='create_bronze_tables',
    bash_command=(
        "docker exec -i chainalytics-spark-master "
        "/opt/spark/bin/spark-submit "
        "--jars /opt/spark/jars/iceberg/* "
        "/opt/spark/jobs/setup/create_bronze_tables.py"
    ),
    dag=dag,
)

# Create silver tables
create_silver_tables = BashOperator(
    task_id='create_silver_tables',
    bash_command=(
        "docker exec -i chainalytics-spark-master "
        "/opt/spark/bin/spark-submit "
        "--jars /opt/spark/jars/iceberg/* "
        "/opt/spark/jobs/setup/create_silver_tables.py"
    ),
    dag=dag,
)

# Create gold tables
create_gold_tables = BashOperator(
    task_id='create_gold_tables',
    bash_command=(
        "docker exec -i chainalytics-spark-master "
        "/opt/spark/bin/spark-submit "
        "--jars /opt/spark/jars/iceberg/* "
        "/opt/spark/jobs/setup/create_gold_tables.py"
    ),
    dag=dag,
)

end_task = EmptyOperator(
    task_id='end_setup',
    dag=dag
)

# Task dependencies - Sequential execution
start_task >> create_bronze_tables >> create_silver_tables >> create_gold_tables >> end_task