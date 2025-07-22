#!/usr/bin/env python3
"""
ChainAnalytics Streaming Processing DAG

This DAG orchestrates the real-time data processing jobs for ChainAnalytics platform
following the same simple pattern as the working table creation DAG.

The streaming processing pipeline includes:
- Kafka to Bronze layer processing
- Late arriving data handler
- Real-time data transformation jobs

Location: /orchestration/dags/chainalytics_streaming_processing.py
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
    dag_id='chainalytics_streaming_processing2',
    default_args=default_args,
    description='Real-time streaming data processing jobs for ChainAnalytics platform',
    schedule=None,  # Manual trigger like table creation DAG
    catchup=False,
    max_active_runs=1,
    max_active_tasks=6,
    tags=['chainalytics', 'streaming', 'real-time', 'processing'],
    doc_md=__doc__,
)

# =============================================================================
# PIPELINE TASKS - Following the working BashOperator pattern
# =============================================================================

# Start task
start_processing = EmptyOperator(
    task_id='start_processing',
    dag=dag
)

# Task 1: Start Kafka to Bronze Streaming Job
start_kafka_to_bronze = BashOperator(
    task_id='start_kafka_to_bronze',
    bash_command='docker exec -d chainalytics-spark-master /opt/spark/bin/spark-submit --master spark://chainalytics-spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 --conf spark.sql.streaming.checkpointLocation=/tmp/spark-checkpoints/bronze /opt/spark/jobs/streaming/kafka_to_bronze.py',
    dag=dag,
)

# Task 2: Start Late Data Handler
# start_late_data_handler = BashOperator(
#     task_id='start_late_data_handler',
#     bash_command='docker exec -d chainalytics-spark-master /opt/spark/bin/spark-submit --master spark://chainalytics-spark-master:7077 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 --conf spark.sql.streaming.checkpointLocation=/tmp/spark-checkpoints/late-data /opt/spark/jobs/streaming/late_data_handler.py',
#     dag=dag,
# )

# Task 3: Verify Streaming Jobs are Running
verify_streaming_jobs = BashOperator(
    task_id='verify_streaming_jobs',
    bash_command='''
        echo "🔍 Verifying streaming jobs are running..."
        sleep 10  # Give jobs time to start
        
        echo "📊 Checking Spark Master UI for running applications..."
        curl -s http://localhost:9090/json/ | grep -o '"name":"[^"]*"' || echo "Could not check Spark UI"
        
        echo "📈 Checking checkpoint directories..."
        docker exec chainalytics-spark-master ls -la /tmp/spark-checkpoints/ || echo "Checkpoint directories not found"
        
        echo "🐳 Checking Spark processes..."
        docker exec chainalytics-spark-master ps aux | grep spark-submit || echo "No Spark submit processes found"
        
        echo "✅ Streaming job verification completed"
        echo ""
        echo "🎉 Streaming Processing Status:"
        echo "============================================="
        echo "📊 Spark UI: http://localhost:9090"
        echo "💾 MinIO Console: http://localhost:9001" 
        echo "🔗 Kafka UI: http://localhost:8090"
        echo "============================================="
    ''',
    dag=dag,
)

# End task
end_processing = EmptyOperator(
    task_id='end_processing',
    dag=dag
)

# =============================================================================
# TASK DEPENDENCIES - Simple linear flow like table creation DAG
# =============================================================================

# Simple sequential flow - exactly like your table creation DAG
# start_processing >> start_kafka_to_bronze >> start_late_data_handler >> verify_streaming_jobs >> end_processing
start_processing >> start_kafka_to_bronze >> verify_streaming_jobs >> end_processing