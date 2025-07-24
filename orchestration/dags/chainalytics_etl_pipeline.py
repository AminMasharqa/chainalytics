#!/usr/bin/env python3
"""
ChainAnalytics ETL Pipeline - Simplified Version
Executes Bronze → Silver → Gold transformations
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule
from datetime import datetime, timedelta

# Default arguments
default_args = {
    'owner': 'chainalytics-data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 15),
    'email_on_failure': True,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}

# Create DAG
dag = DAG(
    dag_id='chainalytics_etl_pipeline_simple',
    default_args=default_args,
    description='Simplified ETL Pipeline for ChainAnalytics',
    schedule=None,  # Manual trigger
    catchup=False,
    max_active_runs=1,
    tags=['chainalytics', 'etl', 'simple'],
)

# Define Spark submit command template
SPARK_SUBMIT = """
docker exec chainalytics-spark-master /opt/spark/bin/spark-submit \
    --master spark://chainalytics-spark-master:7077 \
    --conf spark.sql.adaptive.enabled=true \
    --conf spark.sql.adaptive.coalescePartitions.enabled=true \
    {}
"""

# Tasks
start = EmptyOperator(task_id='start', dag=dag)

# Bronze to Silver transformation
bronze_to_silver = BashOperator(
    task_id='bronze_to_silver',
    bash_command=SPARK_SUBMIT.format('/opt/spark/jobs/batch/etl_bronze_to_silver.py'),
    dag=dag,
)

# Silver to Gold transformation
silver_to_gold = BashOperator(
    task_id='silver_to_gold',
    bash_command=SPARK_SUBMIT.format('/opt/spark/jobs/batch/etl_silver_to_gold.py'),
    dag=dag,
)

# End task
end = EmptyOperator(
    task_id='end',
    trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
    dag=dag
)

# Define dependencies - simple linear flow
start >> bronze_to_silver >> silver_to_gold >> end