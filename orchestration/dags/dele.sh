#!/bin/bash
# DAG Cleanup and Refresh Script

echo "🧹 Starting DAG cleanup..."

# Stop Airflow
echo "Stopping Airflow services..."
docker-compose down

# Start only database
echo "Starting database..."
docker-compose up -d postgres
sleep 15

# Clean database
echo "Cleaning DAG records from database..."
docker exec orchestration-postgres-1 psql -U airflow -d airflow -c "
DELETE FROM dag_run WHERE dag_id LIKE 'chainalytics_%';
DELETE FROM task_instance WHERE dag_id LIKE 'chainalytics_%';
DELETE FROM dag WHERE dag_id LIKE 'chainalytics_%';
DELETE FROM serialized_dag WHERE dag_id LIKE 'chainalytics_%';
DELETE FROM dag_tag WHERE dag_id LIKE 'chainalytics_%';
"

# Restart all services
echo "Restarting all Airflow services..."
docker-compose up -d
sleep 30

# Refresh DAGs
echo "Refreshing DAG serialization..."
docker exec orchestration-airflow-scheduler-1 airflow dags reserialize

# Check results
echo "✅ Cleanup complete! Current DAGs:"
docker exec orchestration-airflow-scheduler-1 airflow dags list | grep "^chain" || echo "No chain DAGs found"

echo "🔍 Checking for import errors:"
docker exec orchestration-airflow-scheduler-1 airflow dags list-import-errors