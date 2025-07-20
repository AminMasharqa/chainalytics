# ChainAnalytics Platform Components

## Architecture Overview

ChainAnalytics is a modern data platform built on cloud-native technologies, implementing a **Lambda Architecture** pattern for both real-time and batch processing. The platform integrates multiple components to provide end-to-end data analytics capabilities.

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   DATA SOURCES  │───▶│   STREAMING     │───▶│   PROCESSING    │
│                 │    │   & INGESTION   │    │   & STORAGE     │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                                                       │
┌─────────────────┐    ┌─────────────────┐           │
│   ANALYTICS &   │◀───│  ORCHESTRATION  │◀──────────┘
│   MONITORING    │    │   & WORKFLOW    │
└─────────────────┘    └─────────────────┘
```

## 1. Streaming & Data Ingestion Layer

### Apache Kafka Ecosystem

#### **Apache Zookeeper**
- **Purpose**: Coordination service for Kafka cluster
- **Image**: `confluentinc/cp-zookeeper:7.5.0`
- **Port**: 2181
- **Function**: 
  - Manages Kafka broker metadata
  - Handles leader election for partitions
  - Maintains cluster configuration
- **Configuration**:
  - Client port: 2181
  - Tick time: 2000ms
  - Sync limit: 2

#### **Apache Kafka**
- **Purpose**: Distributed streaming platform for real-time data ingestion
- **Image**: `confluentinc/cp-kafka:7.5.0`
- **Ports**: 9092 (external), 29092 (internal)
- **Function**:
  - Ingests real-time events from multiple sources
  - Provides fault-tolerant message durability
  - Supports horizontal scaling
- **Topics**:
  - `user-events`: User interaction events
  - `products`: Product-related events
  - `api-logs`: API usage logging
  - `user-posts`: User-generated content
  - `weather-data`: External weather data
- **Configuration**:
  - Replication factor: 1 (development)
  - Partitions: 3 per topic
  - Retention: 168 hours (7 days)

#### **Confluent Schema Registry**
- **Purpose**: Schema management and evolution for Kafka messages
- **Image**: `confluentinc/cp-schema-registry:7.5.0`
- **Port**: 8085
- **Function**:
  - Manages Avro/JSON schema versions
  - Ensures backward/forward compatibility
  - Validates message schemas

#### **Kafka UI**
- **Purpose**: Web interface for Kafka cluster management
- **Image**: `provectuslabs/kafka-ui:latest`
- **Port**: 8090
- **Function**:
  - Monitor Kafka topics and messages
  - View consumer groups and lag
  - Browse message content
  - Monitor cluster health

### Data Producers

#### **Python Data Producers Container**
- **Purpose**: Generates synthetic streaming data for the platform
- **Image**: `python:3.9-slim`
- **Container**: `chainalytics-producers`
- **Function**:
  - Simulates real-time user events
  - Generates product performance metrics
  - Creates API usage logs
  - Produces weather data feeds
- **Producers**:
  - `produce_user_events.py`: User interaction events
  - `produce_products.py`: Product catalog events
  - `produce_api_logs.py`: API access logs
  - `produce_user_posts.py`: User content events
  - `produce_weather_data.py`: Weather API data

## 2. Processing & Compute Layer

### Apache Spark Cluster

#### **Spark Master**
- **Purpose**: Cluster manager and resource coordinator
- **Image**: Custom `chainalytics-spark-image`
- **Container**: `chainalytics-spark-master`
- **Port**: 9090 (Web UI), 7077 (Master)
- **Function**:
  - Manages cluster resources
  - Schedules jobs across workers
  - Provides web interface for monitoring
  - Handles job orchestration

#### **Spark Workers (3 instances)**
- **Purpose**: Execute distributed computing tasks
- **Image**: Custom `chainalytics-spark-image`
- **Containers**: `processing-spark-worker-1/2/3`
- **Function**:
  - Execute Spark tasks and stages
  - Process data in parallel
  - Provide fault tolerance through replication
  - Scale horizontally based on workload

#### **Spark History Server**
- **Purpose**: Historical job monitoring and debugging
- **Image**: Custom `chainalytics-spark-image`
- **Container**: `chainalytics-spark-history`
- **Port**: 18080
- **Function**:
  - Stores completed job metrics
  - Provides historical job analysis
  - Enables debugging of past executions

### Data Processing Jobs

#### **Streaming Jobs**
Located in `/opt/spark/jobs/streaming/`

**`kafka_to_bronze.py`**
- **Purpose**: Real-time ingestion from Kafka to Bronze layer
- **Function**:
  - Reads streaming data from Kafka topics
  - Applies basic schema validation
  - Writes to Iceberg bronze tables
  - Handles schema evolution
- **Trigger**: 30-second micro-batches
- **Output**: Bronze streaming events table

**`late_data_handler.py`**
- **Purpose**: Processes late-arriving data
- **Function**:
  - Detects events arriving beyond SLA
  - Handles out-of-order events
  - Maintains data completeness
  - Updates bronze late events table

#### **Batch Jobs**
Located in `/opt/spark/jobs/batch/`

**`01_bronze_ingestion.py`**
- **Purpose**: Batch ingestion from external sources
- **Function**:
  - Ingests data from file systems
  - Handles bulk data loads
  - Maintains data lineage
  - Implements change data capture

**`02_silver_transformation.py`**
- **Purpose**: Data cleaning and standardization
- **Function**:
  - Cleanses and validates bronze data
  - Applies business rules
  - Removes duplicates
  - Standardizes data formats
  - Creates silver layer tables

**`03_gold_aggregation.py`**
- **Purpose**: Business-ready data aggregation
- **Function**:
  - Creates dimensional models
  - Calculates KPIs and metrics
  - Builds aggregated views
  - Optimizes for analytics queries

**`04_scd_customer.py`**
- **Purpose**: Slowly Changing Dimension management
- **Function**:
  - Implements SCD Type 2 for customers
  - Maintains historical records
  - Tracks dimension changes over time
  - Preserves data lineage

#### **Setup Jobs**
Located in `/opt/spark/jobs/setup/`

**`create_bronze_tables.py`**
- **Purpose**: Initialize bronze layer tables
- **Function**: Creates Iceberg table structures for raw data

**`create_silver_tables.py`**
- **Purpose**: Initialize silver layer tables  
- **Function**: Creates cleaned data table structures

**`create_gold_tables.py`**
- **Purpose**: Initialize gold layer tables
- **Function**: Creates business-ready table structures

**`verify_table_creation.py`**
- **Purpose**: Validates table creation success
- **Function**: Ensures all tables are properly created

## 3. Storage Layer

### MinIO Object Storage
- **Purpose**: S3-compatible object storage for data lake
- **Image**: `minio/minio:latest`
- **Container**: `chainalytics-minio`
- **Ports**: 9000 (API), 9001 (Console)
- **Function**:
  - Stores all data in Iceberg format
  - Provides S3-compatible API
  - Handles metadata and data files
  - Supports versioning and lifecycle management
- **Buckets**:
  - `chainalytics`: Main data warehouse
  - `iceberg-warehouse`: Iceberg metadata
  - `checkpoints`: Spark streaming checkpoints

### Apache Iceberg
- **Purpose**: Open table format for large analytic datasets
- **Function**:
  - Provides ACID transactions
  - Supports schema evolution
  - Enables time travel queries
  - Optimizes query performance
  - Handles concurrent reads/writes

## 4. Orchestration & Workflow Management

### Apache Airflow
- **Purpose**: Workflow orchestration and job scheduling
- **Image**: `apache/airflow:3.0.3`
- **Port**: 8080
- **Components**:

#### **Airflow Webserver**
- **Container**: `orchestration-airflow-apiserver-1`
- **Function**: Provides web UI for DAG management

#### **Airflow Scheduler**
- **Container**: `orchestration-airflow-scheduler-1`
- **Function**: Schedules and executes DAG tasks

#### **Airflow Worker**
- **Container**: `orchestration-airflow-worker-1`
- **Function**: Executes task instances

#### **Airflow Triggerer**
- **Container**: `orchestration-airflow-triggerer-1`
- **Function**: Handles deferred and sensor tasks

#### **DAG Processor**
- **Container**: `orchestration-airflow-dag-processor-1`
- **Function**: Parses and processes DAG definitions

### Data Pipeline DAGs

#### **`chainalytics_table_setup`**
- **Purpose**: Initial platform setup
- **Schedule**: Manual trigger
- **Function**: Creates all database tables and schemas

#### **`chainalytics_streaming_pipeline13`**
- **Purpose**: Manages data producers
- **Schedule**: Manual trigger
- **Function**: Starts/stops Kafka data producers

#### **`chainalytics_streaming_processing2`**
- **Purpose**: Real-time data processing
- **Schedule**: Manual trigger
- **Function**: Activates streaming jobs (Kafka→Bronze)

#### **`chainalytics_batch_data_quality`**
- **Purpose**: Batch ETL and quality checks
- **Schedule**: Daily at 2 AM
- **Function**: Bronze→Silver→Gold transformations

## 5. Supporting Infrastructure

### PostgreSQL Database
- **Purpose**: Airflow metadata storage
- **Image**: `postgres:13`
- **Container**: `orchestration-postgres-1`
- **Function**:
  - Stores Airflow DAG metadata
  - Maintains job execution history
  - Handles user authentication

### Redis Cache
- **Purpose**: Message broker for Airflow
- **Image**: `redis:7.2-bookworm`
- **Container**: `orchestration-redis-1`
- **Function**:
  - Celery message broker
  - Task queue management
  - Caching layer for Airflow

## 6. Data Quality & Monitoring

### Data Quality Framework
- **Location**: `/opt/spark/jobs/data_quality/`
- **Components**:

#### **`quality_checks.py`**
- **Purpose**: Comprehensive data validation
- **Function**:
  - Schema validation checks
  - Data completeness verification
  - Referential integrity validation
  - Business rule enforcement
  - Anomaly detection
  - Data profiling and statistics

### Monitoring & Observability

#### **Spark UI (Port 9090)**
- Real-time job monitoring
- Resource utilization tracking
- Performance optimization insights

#### **Kafka UI (Port 8090)**
- Topic and partition monitoring
- Consumer lag tracking
- Message throughput analysis

#### **MinIO Console (Port 9001)**
- Storage utilization monitoring
- Bucket and object management
- Access policy configuration

#### **Airflow UI (Port 8080)**
- DAG execution monitoring
- Task failure analysis
- Scheduling and dependency tracking

## 7. Network & Security

### Docker Networks
- **`chainalytics-network`**: Isolated network for all components
- **Function**: Secure inter-service communication

### Security Features
- **Service Isolation**: Each component runs in isolated containers
- **Network Segmentation**: Internal communication only
- **Access Control**: MinIO and Airflow authentication
- **Data Encryption**: In-transit encryption between services

## Component Integration Flow

```
1. Producers → Kafka Topics
2. Kafka → Spark Streaming → Bronze Tables (MinIO/Iceberg)
3. Bronze → Spark Batch → Silver Tables
4. Silver → Spark Batch → Gold Tables
5. Airflow orchestrates all processes
6. UIs provide monitoring and management
```

## Scalability & Performance

### Horizontal Scaling
- **Kafka**: Add brokers for higher throughput
- **Spark**: Add workers for more processing power
- **Storage**: MinIO supports distributed deployment

### Performance Optimization
- **Partitioning**: Time-based partitioning for all layers
- **Compression**: Snappy/ZSTD compression
- **Indexing**: Iceberg metadata optimization
- **Caching**: Redis for Airflow performance

This component architecture provides a robust, scalable foundation for real-time analytics while maintaining operational simplicity and monitoring capabilities.