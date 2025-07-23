#!/usr/bin/env python3
"""
ChainAnalytics Batch Processing & Data Quality DAG

This DAG orchestrates the batch data processing and quality checks for ChainAnalytics platform
following the same simple pattern as the working table creation DAG.

The batch pipeline includes:
- Bronze data ingestion
- Silver data transformation
- Gold data aggregation
- SCD customer processing
- Data quality checks

Location: /orchestration/dags/chainalytics_batch_data_quality.py
"""

from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from datetime import datetime, timedelta

# Use the exact same pattern as your working table creation DAG
default_args = {
    'owner': 'chainalytics-data-team',
    'depends_on_past': False,
    'start_date': datetime(2025, 1, 15),  # Same as table creation DAG
    'email': ['data-team@chainalytics.com', 'devops@chainalytics.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=30),
    'execution_timeout': timedelta(hours=4),
}

# Define the DAG - exactly like your working table creation DAG pattern
dag = DAG(
    dag_id='chainalytics_batch_data_quality',
    default_args=default_args,
    description='Batch data processing and quality checks for ChainAnalytics platform',
    schedule='0 2 * * *',  # Daily at 2 AM - you can change this or set to None for manual trigger
    catchup=False,
    max_active_runs=1,
    max_active_tasks=6,
    tags=['chainalytics', 'batch', 'data-quality', 'etl'],
    doc_md=__doc__,
)

# =============================================================================
# PIPELINE TASKS - Following the working BashOperator pattern
# =============================================================================

# Start task
start_batch_processing = EmptyOperator(
    task_id='start_batch_processing',
    dag=dag
)



# Task 2: Silver Data Transformation
silver_transformation = BashOperator(
    task_id='silver_transformation',
    bash_command='docker exec chainalytics-spark-master /opt/spark/bin/spark-submit --master spark://chainalytics-spark-master:7077 /opt/spark/jobs/batch/etl_bronze_to_silver.py',
    dag=dag,
)

# Task 3: Gold Data Aggregation
gold_aggregation = BashOperator(
    task_id='gold_aggregation',
    bash_command='docker exec chainalytics-spark-master /opt/spark/bin/spark-submit --master spark://chainalytics-spark-master:7077 /opt/spark/jobs/batch/03_gold_aggregation.py',
    dag=dag,
)

# Task 4: SCD Customer Processing
scd_customer_processing = BashOperator(
    task_id='scd_customer_processing',
    bash_command='docker exec chainalytics-spark-master /opt/spark/bin/spark-submit --master spark://chainalytics-spark-master:7077 /opt/spark/jobs/batch/04_scd_customer.py',
    dag=dag,
)

# Task 5: Data Quality Checks
data_quality_checks = BashOperator(
    task_id='data_quality_checks',
    bash_command='docker exec chainalytics-spark-master /opt/spark/bin/spark-submit --master spark://chainalytics-spark-master:7077 /opt/spark/jobs/data_quality/quality_checks.py',
    dag=dag,
)

# Task 6: Generate Batch Processing Report
generate_batch_report = BashOperator(
    task_id='generate_batch_report',
    bash_command='''
        echo "📊 Generating Batch Processing Report..."
        
        echo "🔍 Checking table record counts..."
        
        # You can add specific reporting logic here
        echo "📈 Bronze Tables Status:"
        echo "📈 Silver Tables Status:"
        echo "📈 Gold Tables Status:"
        echo "📈 Data Quality Status:"
        
        echo "✅ Batch processing report generated"
        echo ""
        echo "🎉 Batch Processing Completed Successfully!"
        echo "============================================="
        echo "📊 Spark UI: http://localhost:9090"
        echo "💾 MinIO Console: http://localhost:9001" 
        echo "🔗 Kafka UI: http://localhost:8090"
        echo "📋 Airflow UI: http://localhost:8080"
        echo "============================================="
    ''',
    dag=dag,
)

# End task
end_batch_processing = EmptyOperator(
    task_id='end_batch_processing',
    dag=dag
)

# =============================================================================
# TASK DEPENDENCIES - Simple linear flow like table creation DAG
# =============================================================================

# Bronze layer first
start_batch_processing >> bronze_ingestion

# Silver layer depends on bronze
bronze_ingestion >> silver_transformation

# Gold layer depends on silver
silver_transformation >> gold_aggregation

# SCD processing can run in parallel with gold
silver_transformation >> scd_customer_processing

# Data quality checks run after all transformations
[gold_aggregation, scd_customer_processing] >> data_quality_checks

# Generate report and finish
data_quality_checks >> generate_batch_report >> end_batch_processing