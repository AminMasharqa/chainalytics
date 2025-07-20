# ChainAnalytics Data Model Documentation

## Overview

The ChainAnalytics platform implements a **medallion architecture** (Bronze → Silver → Gold) using Apache Iceberg tables stored in MinIO object storage. This design ensures data quality, scalability, and performance optimization for real-time analytics.

## Architecture Layers

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   BRONZE LAYER  │───▶│  SILVER LAYER   │───▶│   GOLD LAYER    │
│   Raw Data      │    │  Cleaned Data   │    │ Business Ready  │
└─────────────────┘    └─────────────────┘    └─────────────────┘
```

## Bronze Layer (Raw Data Ingestion)

### Purpose
- **Raw data storage** from various source systems
- **Minimal transformation** - preserving original data structure
- **Schema evolution** support through Iceberg format
- **Time-based partitioning** for efficient queries

### Tables

#### `bronze_streaming_events`
**Description**: Real-time events from Kafka streaming sources

| Column | Type | Description |
|--------|------|-------------|
| `event_id` | String | Unique event identifier |
| `user_id` | String | User identifier |
| `event_type` | String | Type of event (click, purchase, view) |
| `product_id` | String | Product identifier |
| `event_timestamp` | String | When the event occurred |
| `ingestion_timestamp` | String | When event was ingested from source |
| `kafka_timestamp` | Timestamp | Kafka message timestamp |
| `processing_timestamp` | Timestamp | When event was processed by Spark |
| `source_system` | String | Source system identifier |

**Partitioning**: By date extracted from `event_timestamp`  
**Source**: Kafka topics (`user-events`, `products`, `api-logs`, etc.)

#### `bronze_streaming_events_late`
**Description**: Late-arriving events processed by late data handler

| Column | Type | Description |
|--------|------|-------------|
| `event_id` | String | Unique event identifier |
| `user_id` | String | User identifier |
| `event_type` | String | Type of event |
| `product_id` | String | Product identifier |
| `event_timestamp` | String | Original event timestamp |
| `ingestion_timestamp` | String | Original ingestion time |
| `late_arrival_timestamp` | Timestamp | When late event was detected |
| `delay_minutes` | Integer | Minutes between event and processing |

#### `bronze_user_sessions`
**Description**: Raw user session data

| Column | Type | Description |
|--------|------|-------------|
| `session_id` | String | Unique session identifier |
| `user_id` | String | User identifier |
| `start_time` | Timestamp | Session start time |
| `end_time` | Timestamp | Session end time |
| `page_views` | Integer | Number of page views |
| `source_system` | String | Data source |

#### `bronze_product_performance`
**Description**: Raw product performance metrics

| Column | Type | Description |
|--------|------|-------------|
| `product_id` | String | Product identifier |
| `metric_name` | String | Performance metric name |
| `metric_value` | Double | Metric value |
| `recorded_at` | Timestamp | When metric was recorded |
| `source_system` | String | Data source |

#### `bronze_customer_behavior`
**Description**: Raw customer behavior data

| Column | Type | Description |
|--------|------|-------------|
| `customer_id` | String | Customer identifier |
| `behavior_type` | String | Type of behavior |
| `behavior_data` | String | JSON behavior details |
| `recorded_at` | Timestamp | Recording timestamp |
| `source_system` | String | Data source |

## Silver Layer (Cleaned & Validated Data)

### Purpose
- **Data cleaning** and validation
- **Schema standardization** across sources
- **Data type conversions** and normalization
- **Duplicate removal** and data quality enforcement

### Tables

#### `silver_user_sessions`
**Description**: Cleaned and validated user session data

| Column | Type | Description |
|--------|------|-------------|
| `session_id` | String | Unique session identifier |
| `user_id` | String | Validated user identifier |
| `start_time` | Timestamp | Standardized session start |
| `end_time` | Timestamp | Standardized session end |
| `duration_minutes` | Integer | Calculated session duration |
| `page_views` | Integer | Validated page view count |
| `is_valid_session` | Boolean | Session validity flag |
| `processed_at` | Timestamp | Silver processing timestamp |

#### `silver_product_performance`
**Description**: Cleaned product performance metrics

| Column | Type | Description |
|--------|------|-------------|
| `product_id` | String | Validated product identifier |
| `metric_name` | String | Standardized metric name |
| `metric_value` | Double | Validated metric value |
| `metric_category` | String | Categorized metric type |
| `recorded_date` | Date | Date of recording |
| `is_outlier` | Boolean | Outlier detection flag |
| `processed_at` | Timestamp | Silver processing timestamp |

#### `silver_customer_behavior`
**Description**: Cleaned customer behavior analytics

| Column | Type | Description |
|--------|------|-------------|
| `customer_id` | String | Validated customer identifier |
| `behavior_type` | String | Standardized behavior type |
| `behavior_score` | Double | Calculated behavior score |
| `behavior_category` | String | Behavior classification |
| `recorded_date` | Date | Date of behavior |
| `is_anomaly` | Boolean | Anomaly detection flag |
| `processed_at` | Timestamp | Silver processing timestamp |

#### `silver_system_health`
**Description**: System health and monitoring metrics

| Column | Type | Description |
|--------|------|-------------|
| `system_name` | String | System identifier |
| `metric_name` | String | Health metric name |
| `metric_value` | Double | Health metric value |
| `status` | String | System status (healthy/warning/critical) |
| `alert_threshold` | Double | Alert threshold value |
| `recorded_at` | Timestamp | Metric recording time |

#### `silver_logistics_impact`
**Description**: Logistics and operational impact metrics

| Column | Type | Description |
|--------|------|-------------|
| `impact_id` | String | Impact event identifier |
| `impact_type` | String | Type of logistics impact |
| `severity` | String | Impact severity level |
| `affected_systems` | Array[String] | List of affected systems |
| `estimated_cost` | Double | Estimated impact cost |
| `resolution_time` | Integer | Time to resolution (minutes) |
| `recorded_at` | Timestamp | Impact recording time |

## Gold Layer (Business-Ready Analytics)

### Purpose
- **Business aggregations** and KPIs
- **Dimensional modeling** for analytics
- **Performance-optimized** for reporting
- **Historical tracking** and trends

### Dimension Tables

#### `dim_customer`
**Description**: Customer dimension with SCD Type 2 implementation

| Column | Type | Description |
|--------|------|-------------|
| `customer_key` | Long | Surrogate key |
| `customer_id` | String | Business key |
| `customer_name` | String | Customer name |
| `email` | String | Customer email |
| `segment` | String | Customer segment |
| `registration_date` | Date | Registration date |
| `effective_date` | Date | SCD effective date |
| `expiry_date` | Date | SCD expiry date |
| `is_current` | Boolean | Current record flag |
| `version` | Integer | Record version |

#### `dim_product`
**Description**: Product dimension

| Column | Type | Description |
|--------|------|-------------|
| `product_key` | Long | Surrogate key |
| `product_id` | String | Business key |
| `product_name` | String | Product name |
| `category` | String | Product category |
| `subcategory` | String | Product subcategory |
| `price` | Double | Product price |
| `created_date` | Date | Product creation date |
| `is_active` | Boolean | Product status |

#### `dim_date`
**Description**: Date dimension for time-based analytics

| Column | Type | Description |
|--------|------|-------------|
| `date_key` | Integer | Date key (YYYYMMDD) |
| `date` | Date | Actual date |
| `year` | Integer | Year |
| `quarter` | Integer | Quarter |
| `month` | Integer | Month |
| `day` | Integer | Day |
| `day_of_week` | Integer | Day of week |
| `day_name` | String | Day name |
| `month_name` | String | Month name |
| `is_weekend` | Boolean | Weekend flag |
| `is_holiday` | Boolean | Holiday flag |

### Fact Tables

#### `fact_customer_activity`
**Description**: Customer activity metrics and KPIs

| Column | Type | Description |
|--------|------|-------------|
| `activity_key` | Long | Surrogate key |
| `customer_key` | Long | Customer dimension key |
| `product_key` | Long | Product dimension key |
| `date_key` | Integer | Date dimension key |
| `activity_type` | String | Type of activity |
| `activity_count` | Integer | Number of activities |
| `revenue` | Double | Revenue generated |
| `cost` | Double | Associated costs |
| `profit` | Double | Calculated profit |
| `session_duration` | Integer | Session duration (minutes) |

#### `fact_operational_metrics`
**Description**: Operational and system performance metrics

| Column | Type | Description |
|--------|------|-------------|
| `metric_key` | Long | Surrogate key |
| `date_key` | Integer | Date dimension key |
| `system_name` | String | System identifier |
| `metric_name` | String | Metric name |
| `metric_value` | Double | Metric value |
| `target_value` | Double | Target/SLA value |
| `variance` | Double | Variance from target |
| `status` | String | Performance status |

### Aggregated Tables

#### `gold_daily_business_summary`
**Description**: Daily business performance summary

| Column | Type | Description |
|--------|------|-------------|
| `summary_date` | Date | Business date |
| `total_revenue` | Double | Daily revenue |
| `total_orders` | Integer | Number of orders |
| `unique_customers` | Integer | Unique customer count |
| `avg_order_value` | Double | Average order value |
| `customer_acquisition_cost` | Double | Customer acquisition cost |
| `churn_rate` | Double | Daily churn rate |
| `system_uptime_pct` | Double | System uptime percentage |

#### `gold_customer_conversion_features`
**Description**: Customer conversion and behavior features

| Column | Type | Description |
|--------|------|-------------|
| `customer_id` | String | Customer identifier |
| `feature_date` | Date | Feature calculation date |
| `total_sessions` | Integer | Total sessions |
| `avg_session_duration` | Double | Average session duration |
| `conversion_rate` | Double | Customer conversion rate |
| `lifetime_value` | Double | Customer lifetime value |
| `engagement_score` | Double | Engagement score |
| `churn_probability` | Double | Predicted churn probability |
| `next_purchase_days` | Integer | Predicted days to next purchase |

## Data Lineage

```
Kafka Topics (user-events, products, api-logs, user-posts, weather-data)
    ↓
Bronze Layer (Raw ingestion via Spark Streaming)
    ↓
Silver Layer (Cleaned via batch ETL - 02_silver_transformation.py)
    ↓
Gold Layer (Aggregated via batch ETL - 03_gold_aggregation.py)
    ↓
Analytics & Reporting
```

## Storage & Partitioning Strategy

### Bronze Layer
- **Format**: Apache Iceberg
- **Partitioning**: By ingestion date
- **Retention**: 90 days
- **Compression**: Snappy

### Silver Layer  
- **Format**: Apache Iceberg
- **Partitioning**: By business date
- **Retention**: 2 years
- **Compression**: Snappy

### Gold Layer
- **Format**: Apache Iceberg  
- **Partitioning**: By month/year
- **Retention**: 7 years
- **Compression**: ZSTD

## Schema Evolution

The platform supports schema evolution through Apache Iceberg:
- **Add columns**: Supported without downtime
- **Rename columns**: Supported with aliases
- **Change data types**: Supported with compatibility rules
- **Drop columns**: Supported with careful planning

## Data Quality Constraints

### Bronze Layer
- **Primary Keys**: event_id, session_id, product_id
- **Not Null**: All ID fields, timestamps
- **Format Validation**: JSON structure for nested data

### Silver Layer
- **Data Validation**: Range checks, format validation
- **Duplicate Removal**: Based on business keys
- **Referential Integrity**: Foreign key relationships

### Gold Layer
- **Business Rules**: KPI calculations, metric validations
- **Aggregation Accuracy**: Sum/count validations
- **Dimensional Integrity**: SCD implementation rules

## Performance Optimization

### Indexing Strategy
- **Partition Pruning**: Time-based partitioning
- **File Sizing**: Optimal file sizes (128MB-1GB)
- **Columnar Storage**: Optimized for analytical queries

### Query Optimization
- **Predicate Pushdown**: Enabled for all layers
- **Column Pruning**: Projection optimization
- **Join Optimization**: Broadcast joins for dimension tables

This data model provides a robust foundation for real-time analytics while maintaining data quality and performance at scale.