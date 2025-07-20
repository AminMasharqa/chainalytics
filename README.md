# ChainAnalytics Data Platform 🚀

A comprehensive real-time data analytics platform with streaming, batch processing, and orchestration capabilities.

## Architecture Overview

- **Kafka**: Real-time data streaming
- **Apache Spark**: Distributed data processing (Master + 3 Workers)
- **Apache Airflow**: Workflow orchestration
- **MinIO**: Object storage (S3-compatible)
- **Apache Iceberg**: Data lakehouse format

## 🛠️ Setup Instructions

### Prerequisites
- Docker and Docker Compose installed
- Git installed
- At least 8GB RAM available
- Ports available: 2181, 8080, 8085, 8090, 9000, 9001, 9090, 9092

### Step 1: Clone the Repository

```bash
git clone https://github.com/AminMasharqa/chainalytics.git
cd chainalytics
```

### Step 2: Activate Kafka Containers

```bash
# Navigate to streaming directory
cd streaming

# Start Kafka ecosystem (Zookeeper, Kafka, Schema Registry, Kafka UI, Producers)
docker compose up -d

# Verify Kafka is running
docker ps | grep kafka
```

**Services Started:**
- Zookeeper: `localhost:2181`
- Kafka: `localhost:9092`
- Kafka UI: `http://localhost:8090`
- Schema Registry: `localhost:8085`
- Kafka Producers: `chainalytics-producers`

### Step 3: Activate Spark Processing Cluster

Open a **new terminal** and navigate to the processing directory:

```bash
# From root directory, navigate to processing
cd processing

# Rename Dockerfile (case-sensitive fix)
mv DokerFile dockerfile

# Create Spark environment configuration
cat > .env.spark << EOF
SPARK_NO_DAEMONIZE=true
SPARK_MASTER=spark://chainalytics-spark-master:7077
MINIO_ENDPOINT=http://chainalytics-minio:9000
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin
EOF

# Build the Spark image
make build

# Run Spark cluster (1 Master + 3 Workers + MinIO + History Server)
make run-scaled
```

**Services Started:**
- Spark Master: `http://localhost:9090`
- Spark Workers: 3 worker nodes
- MinIO Console: `http://localhost:9001` (minioadmin/minioadmin)
- Spark History Server: `http://localhost:18080`

### Step 4: Activate Airflow Orchestration

Open a **new terminal** and navigate to the orchestration directory:

```bash
# From root directory, navigate to orchestration
cd orchestration

# Start Airflow ecosystem
docker compose up

# Wait for all services to be healthy (this may take 2-3 minutes)
```

**Services Started:**
- Airflow Webserver: `http://localhost:8080` (admin/admin)
- Airflow Scheduler
- Airflow Worker
- PostgreSQL Database
- Redis Cache

## 🎯 Platform Access Points

| Service | URL | Credentials |
|---------|-----|-------------|
| **Airflow UI** | http://localhost:8080 | admin/admin |
| **Spark Master UI** | http://localhost:9090 | - |
| **Kafka UI** | http://localhost:8090 | - |
| **MinIO Console** | http://localhost:9001 | minioadmin/minioadmin |
| **Spark History** | http://localhost:18080 | - |

## 🚀 Quick Start - Running the Pipeline

After completing Step 4, navigate to the Airflow UI to execute the data pipeline:

### Access Airflow DAGs
- Go to: `http://localhost:8080/dags/`
- Login with: admin/admin

### Pipeline Execution Order

#### 1. Create Data Tables
- **Search for**: `chainalytics_table_setup` in the search box
- **Trigger**: This DAG to create Bronze, Silver, and Gold tables
- **Wait**: Until completion (creates table structure in MinIO)

#### 2. Start Data Producers  
- **Trigger**: `chainalytics_streaming_pipeline13`
- **Purpose**: Feeds Kafka topics with streaming data
- **Verify**: Check Kafka UI at `http://localhost:8090` for incoming messages

#### 3. Start Stream Processing
- **Trigger**: `chainalytics_streaming_processing2`
- **Purpose**: Activates Kafka-to-Bronze ingestion and late arrivals handler
- **Note**: This processes streaming data from Kafka into Bronze tables

#### 4. Run Batch ETL Processing
- **Trigger**: `chainalytics_batch_data_quality` 
- **Purpose**: Transforms data from Bronze → Silver → Gold layers
- **Includes**: Data quality checks and aggregations

## ⚠️ Current Platform Status

**What's Working:**
- ✅ All DAGs execute successfully
- ✅ Kafka messages are visible in Kafka UI (`http://localhost:8090`)
- ✅ Tables and folder structures are created in MinIO (`http://localhost:9001`)
- ✅ Spark jobs run without errors
- ✅ Complete pipeline orchestration works

**Known Limitation:**
- ⚠️ **Data Population**: While the infrastructure works perfectly and data flows through Kafka, the final step of populating data into tables needs refinement
- The pipeline creates all necessary structures but data visibility in tables requires additional debugging
