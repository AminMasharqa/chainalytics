# ChainAnalytics Data Quality Framework

## Overview

The ChainAnalytics platform implements a comprehensive **Data Quality Framework** that ensures data accuracy, completeness, consistency, and reliability across all layers of the medallion architecture. Quality checks are automated and integrated into the ETL pipeline.

## Data Quality Dimensions

### 1. **Completeness**
- **Definition**: Degree to which data is present and non-null
- **Metrics**: Null percentage, missing value count
- **Thresholds**: < 5% null values for critical fields

### 2. **Accuracy**
- **Definition**: Degree to which data correctly represents real-world values
- **Metrics**: Format validation, range checks, pattern matching
- **Thresholds**: 99.9% accuracy for business-critical fields

### 3. **Consistency**
- **Definition**: Degree to which data is consistent across systems and time
- **Metrics**: Cross-reference validation, duplicate detection
- **Thresholds**: < 0.1% duplicate records

### 4. **Validity**
- **Definition**: Degree to which data conforms to defined formats and constraints
- **Metrics**: Schema compliance, data type validation
- **Thresholds**: 100% schema compliance

### 5. **Timeliness**
- **Definition**: Degree to which data is current and up-to-date
- **Metrics**: Data freshness, processing lag
- **Thresholds**: < 5 minutes processing delay for streaming data

### 6. **Integrity**
- **Definition**: Degree to which data maintains referential relationships
- **Metrics**: Foreign key validation, relationship consistency
- **Thresholds**: 100% referential integrity

## Quality Check Implementation

### Bronze Layer Quality Checks

#### **Schema Validation**
```python
def validate_bronze_schema(df, expected_schema):
    """
    Validates that incoming data matches expected schema
    """
    checks = {
        'schema_match': df.schema == expected_schema,
        'required_columns': set(expected_schema.fieldNames()).issubset(set(df.columns)),
        'data_types': validate_data_types(df, expected_schema)
    }
    return checks
```

**Rules Applied**:
- ✅ **Event ID Format**: Must match pattern `evt_\d{5}`
- ✅ **Timestamp Format**: ISO 8601 format validation
- ✅ **User ID Range**: Numeric values between 1-1000
- ✅ **Product ID Range**: Numeric values between 1-1000
- ✅ **Event Types**: Must be one of ['click', 'purchase', 'view']

#### **Completeness Checks**
```python
def check_bronze_completeness(df):
    """
    Validates data completeness for bronze layer
    """
    completeness_checks = {
        'event_id_completeness': calculate_null_percentage(df, 'event_id'),
        'user_id_completeness': calculate_null_percentage(df, 'user_id'),
        'timestamp_completeness': calculate_null_percentage(df, 'event_timestamp'),
        'overall_completeness': calculate_overall_completeness(df)
    }
    return completeness_checks
```

**Thresholds**:
- `event_id`: 0% null values (CRITICAL)
- `user_id`: 0% null values (CRITICAL)
- `event_timestamp`: 0% null values (CRITICAL)
- `product_id`: < 5% null values
- `event_type`: 0% null values (CRITICAL)

#### **Timeliness Checks**
```python
def check_data_timeliness(df):
    """
    Validates data timeliness and freshness
    """
    current_time = datetime.now()
    timeliness_checks = {
        'avg_processing_delay': calculate_avg_delay(df, current_time),
        'max_processing_delay': calculate_max_delay(df, current_time),
        'late_arriving_events': count_late_events(df, threshold_minutes=30),
        'future_events': count_future_events(df, current_time)
    }
    return timeliness_checks
```

**Thresholds**:
- Average processing delay: < 5 minutes
- Maximum processing delay: < 30 minutes
- Late arriving events: < 1% of total events
- Future events: 0 (data quality issue)

### Silver Layer Quality Checks

#### **Data Transformation Validation**
```python
def validate_silver_transformations(bronze_df, silver_df):
    """
    Validates data transformations from bronze to silver
    """
    validation_checks = {