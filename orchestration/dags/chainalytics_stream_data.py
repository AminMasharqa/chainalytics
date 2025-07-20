#!/usr/bin/env python3
"""
ChainAnalytics Streaming Pipeline DAG

This DAG orchestrates the real-time streaming pipeline for ChainAnalytics platform
following the same simple pattern as the working table creation DAG.

The streaming pipeline includes:
- Kafka health checks
- Data producers for multiple streams (using chainalytics-producers container)
- Streaming data processing jobs
- Health monitoring

Location: /orchestration/dags/chainalytics_streaming_dag.py
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
    'retries': 2,  # Reduced retries for faster debugging
    'retry_delay': timedelta(minutes=2),  # Faster retry for debugging
    'retry_exponential_backoff': True,
    'max_retry_delay': timedelta(minutes=10),  # Shorter max delay
    'execution_timeout': timedelta(minutes=30),  # Shorter timeout for debugging
}

# Define the DAG - exactly like your working table creation DAG pattern
dag = DAG(
    dag_id='chainalytics_streaming_pipeline13',
    default_args=default_args,
    description='Real-time ChainAnalytics Streaming Pipeline with Kafka integration',
    schedule=None,  # Manual trigger like table creation DAG
    catchup=False,
    max_active_runs=1,
    max_active_tasks=6,
    tags=['chainalytics', 'streaming', 'kafka', 'real-time'],
    doc_md=__doc__,
)

# =============================================================================
# PIPELINE TASKS - Following the working BashOperator pattern
# =============================================================================

# Start task
start_streaming = EmptyOperator(
    task_id='start_streaming',
    dag=dag
)

# Task 1: Simple startup task
simple_startup = EmptyOperator(
    task_id='simple_startup',
    dag=dag
)

# Task 2: Stop any existing producers first
stop_existing_producers = BashOperator(
    task_id='stop_existing_producers',
    bash_command='''
        echo "🧹 Stopping any existing producer processes..."
        docker exec chainalytics-producers pkill -f "python.*produce" || echo "No existing producers found"
        sleep 3
        echo "✅ Cleanup completed"
    ''',
    dag=dag,
)

# Task 3: Start User Events Producer
start_user_events_producer = BashOperator(
    task_id='start_user_events_producer',
    bash_command='docker exec -d chainalytics-producers bash -c "cd /app/producers && python produce_user_events.py"',
    dag=dag,
)

# Task 4: Start User Posts Producer
start_user_posts_producer = BashOperator(
    task_id='start_user_posts_producer',
    bash_command='docker exec -d chainalytics-producers bash -c "cd /app/producers && python produce_user_posts.py"',
    dag=dag,
)

# Task 5: Start Products Producer
start_products_producer = BashOperator(
    task_id='start_products_producer',
    bash_command='docker exec -d chainalytics-producers bash -c "cd /app/producers && python produce_products.py"',
    dag=dag,
)

# Task 6: Start API Logs Producer
start_api_logs_producer = BashOperator(
    task_id='start_api_logs_producer',
    bash_command='docker exec -d chainalytics-producers bash -c "cd /app/producers && python produce_api_logs.py"',
    dag=dag,
)

# Task 7: Start Weather Data Producer
start_weather_producer = BashOperator(
    task_id='start_weather_producer',
    bash_command='docker exec -d chainalytics-producers bash -c "cd /app/producers && python produce_weather_data.py"',
    dag=dag,
)

# Task 8: Verify Producers are Running
verify_producers = BashOperator(
    task_id='verify_producers',
    bash_command='''
        echo "🔍 Verifying all producers are running..."
        sleep 10  # Give producers more time to start
        
        echo "📊 Checking running Python processes in producers container..."
        producer_processes=$(docker exec chainalytics-producers ps aux | grep python | grep produce || echo "No producer processes found")
        echo "$producer_processes"
        
        echo "📜 Checking producer logs for errors..."
        for log in user_events user_posts products api_logs weather; do
            echo "=== $log.log ==="
            docker exec chainalytics-producers cat /tmp/$log.log 2>/dev/null | tail -5 || echo "No log file found"
        done
        
        echo "📈 Checking Kafka topics for data..."
        for topic in user-events user-posts products api-logs weather-data; do
            echo "Checking topic: $topic"
            message_count=$(docker exec chainalytics-kafka kafka-run-class kafka.tools.GetOffsetShell --broker-list localhost:9092 --topic $topic --time -1 2>/dev/null | awk -F: '{sum += $3} END {print sum}' || echo "0")
            if [ "$message_count" -gt 0 ]; then
                echo "✅ $topic has $message_count messages"
            else
                echo "⚠️ $topic has no messages yet"
            fi
        done
        
        echo "✅ Producer verification completed"
    ''',
    dag=dag,
)

# End task (simplified for debugging)
end_streaming = EmptyOperator(
    task_id='end_streaming',
    dag=dag
)

# =============================================================================
# TASK DEPENDENCIES - Simplified for debugging
# =============================================================================

# Start with simple startup
start_streaming >> simple_startup >> stop_existing_producers

# Start all producers in sequence for better debugging
stop_existing_producers >> start_user_events_producer >> start_user_posts_producer >> start_products_producer >> start_api_logs_producer >> start_weather_producer

# Verify and end
start_weather_producer >> verify_producers >> end_streaming