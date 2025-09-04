# Funnel Analytics System

A dynamic funnel analysis system that generates BigQuery tables and provides Tableau-ready data for conversion rate analysis. Supports configurable funnel steps, automatic data validation, and backfill logic for complete user journey tracking.

## 🚀 Features

- **Dynamic SQL Generation**: Adapts to any number of funnel steps
- **Automatic Data Validation**: Checks event availability before processing
- **Backfill Logic**: Creates artificial steps for users who skip intermediate steps
- **Tableau Integration**: Ready-to-use calculated fields for conversion analysis
- **Configurable Filters**: Support for business logic and date filtering
- **Percentile Analysis**: Time-based analysis between funnel steps

## 📋 Prerequisites

- Google Cloud BigQuery access
- Python 3.7+
- Required packages: `google-cloud-bigquery`, `google-auth`

## ��️ Installation

```bash
pip install google-cloud-bigquery google-auth
```

## ⚙️ Configuration

### Basic Configuration Structure

```python
FUNNEL_CONFIG = {
    "funnel_name": "Your Funnel Name",
    "project_id": "your-gcp-project-id",
    "source_dataset": "source_dataset_name",
    "destination_dataset": "destination_dataset_name",
    "data_source": "project_id.dataset.table_name",
    "user_id_column": "user_id_field_name",
    "session_id_column": "session_id_field_name",
    "event_column": "event_name_field",
    "timestamp_column": "timestamp_field",
    "partition_column": "partition_field",
    "time_period_days": 90,  # Optional - remove for all data
    "steps": [
        {
            "rank": 1,
            "screen_name": "Step Display Name",
            "loaded_events": ["event_name_1", "event_name_2"]
        }
    ],
    "filters": [
        "column_name = 'value'",
        "another_column = 'another_value'"
    ],
    "create_percentiles_table": True,
    "percentiles_table_name": "your_percentiles_table"
}
```

### Required Source Table Fields

| Field | Description | Example |
|-------|-------------|---------|
| `user_id_column` | Unique user identifier | `actor_id`, `user_id`, `customer_id` |
| `event_column` | Event/action names | `ACTIVITY_NAME`, `event_name`, `action` |
| `timestamp_column` | When events occurred | `ACTIVITY_TIME_IST`, `created_at`, `timestamp` |
| `filters` | Business logic filters | `LOAN_PROGRAM = 'PROGRAM_NAME'` |

### Step Configuration Example

```python
"steps": [
    {
        "rank": 1,  # Sequential order (1, 2, 3...)
        "screen_name": "Offer Generated",  # Display name in Tableau
        "loaded_events": ["Offer_Generated"]  # Actual event names in data
    },
    {
        "rank": 2,
        "screen_name": "Application Created", 
        "loaded_events": ["Application_Created", "App_Request"]  # Multiple events per step
    }
]
```

## 🚀 Usage

### 1. Create Configuration File

Create a new Python file (e.g., `my_funnel_analyzer.py`) with your configuration:

```python
#!/usr/bin/env python3

import sys
import os
from typing import Dict, Any, List
from google.cloud import bigquery

# Your funnel configuration
FUNNEL_CONFIG = {
    "funnel_name": "My Funnel",
    "project_id": "your-project-id",
    "source_dataset": "source_dataset",
    "destination_dataset": "destination_dataset",
    "data_source": "your-project.source_dataset.table_name",
    "user_id_column": "actor_id",
    "session_id_column": "actor_id",
    "event_column": "ACTIVITY_NAME",
    "timestamp_column": "ACTIVITY_TIME_IST",
    "partition_column": "row_updated_time",
    "steps": [
        {
            "rank": 1,
            "screen_name": "Step 1",
            "loaded_events": ["event_1"]
        },
        {
            "rank": 2,
            "screen_name": "Step 2",
            "loaded_events": ["event_2"]
        }
    ],
    "filters": [
        "PROGRAM = 'MY_PROGRAM'",
        "PARTNER = 'my_partner'"
    ],
    "create_percentiles_table": True,
    "percentiles_table_name": "my_percentiles_table"
}

# Copy the StandardFunnelAnalyzer class from funnel_analyzer.py
class StandardFunnelAnalyzer:
    # ... (copy the entire class implementation)
    pass

def main():
    try:
        analyzer = StandardFunnelAnalyzer(project_id="your-project-id")
        config = analyzer.get_config()
        success = analyzer.execute_analysis(config)
        
        if success:
            print("\n🎉 Funnel analysis completed successfully!")
            sys.exit(0)
        else:
            print("💥 Funnel analysis failed!")
            sys.exit(1)
            
    except Exception as e:
        print(f"❌ Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main()
```

### 2. Run Analysis

```bash
python3 my_funnel_analyzer.py
```

### 3. Expected Output

- **STANDARD_FUNNEL table**: Main funnel data with user journeys
- **percentiles_by_stage table**: Time analysis between steps

## �� Output Schema

### STANDARD_FUNNEL Table

| Column | Type | Description |
|--------|------|-------------|
| `actor_id` | STRING | User identifier |
| `session_id` | STRING | Session identifier |
| `screen_name` | STRING | Funnel step name |
| `screen_entry_time` | TIMESTAMP | When user entered step |
| `step_stage` | INTEGER | Step number (1, 2, 3...) |
| `previous_screen_name` | STRING | Previous step name |
| `screen_success_time` | TIMESTAMP | When user completed step |
| `is_conversion_session` | INTEGER | 1 if user moved to next step |
| `time_to_convert` | INTEGER | Seconds to next step |

### Percentiles Table

| Column | Type | Description |
|--------|------|-------------|
| `screen_name` | STRING | Step name |
| `ordered_screen_name` | STRING | Step with rank prefix |
| `step_order` | INTEGER | Step number |
| `percentile_25` | FLOAT | 25th percentile time |
| `percentile_50` | FLOAT | Median time |
| `percentile_75` | FLOAT | 75th percentile time |

## 📈 Tableau Integration

### E2E Conversion Rate

```tableau
// E2E Conversion (users at current step / users at first step)
{ FIXED [Date Value] : COUNTD(IF [Step Stage] = {MAX([Step Stage])} THEN [Actor Id] END) } /
{ FIXED [Date Value] : COUNTD(IF [Step Stage] = {MIN([Step Stage])} THEN [Actor Id] END) }
```

### Screen Conversion Rate

```tableau
// Screen Conversion (users at current step / users at previous step)
COUNTD([Actor Id]) / LOOKUP(COUNTD([Actor Id]), -1)
```

### Completers Count

```tableau
// Users who completed the funnel
{ FIXED [Date Value] : COUNTD(IF [Step Stage] = {MAX([Step Stage])} THEN [Actor Id] END) }
```

## 🔧 Key Features Explained

### 1. Automatic Data Validation

The system checks which events actually exist in your data before processing:

```python
# Checks event availability and filters out missing steps
filtered_steps = self.check_event_availability(config)
```

### 2. Backfill Logic

Creates artificial steps for users who skip intermediate steps:

```sql
-- If user reaches step 5 but skipped steps 2-4, 
-- the system creates artificial entries for steps 2-4
-- This ensures complete funnel analysis
```

### 3. Dynamic SQL Generation

Adapts to any number of funnel steps and different data sources:

```python
# Generates CASE statements dynamically based on your config
step_cases.append(f"when event in ('{events_list}') then {step['rank']}")
```

## 🐛 Troubleshooting

### Common Issues

#### 1. "No steps with available events found"
- **Cause**: Event names in config don't match source data
- **Solution**: Run data validation query to check available events

#### 2. "Missing required field"
- **Cause**: Required fields missing from configuration
- **Solution**: Ensure all required fields are specified

#### 3. Low data volume
- **Cause**: Filters too restrictive or date range too narrow
- **Solution**: Remove/expand filters or date range

### Data Validation Queries

```sql
-- Check available events
SELECT 
    ACTIVITY_NAME,
    COUNT(*) as count
FROM `your-table`
WHERE LOAN_PROGRAM = 'your-program'
GROUP BY ACTIVITY_NAME
ORDER BY count DESC;

-- Check user count
SELECT 
    COUNT(DISTINCT actor_id) as users
FROM `your-table`
WHERE LOAN_PROGRAM = 'your-program';
```

## �� Best Practices

1. **Event Naming**: Use exact event names from source data
2. **Step Ordering**: Maintain sequential rank (1, 2, 3...)
3. **Data Quality**: Validate source data before running analysis
4. **Testing**: Test with small datasets first
5. **Documentation**: Document your funnel steps and business logic

## 📝 Example Use Cases

### Loan Application Funnel
```python
"steps": [
    {"rank": 1, "screen_name": "Offer Generated", "loaded_events": ["Offer_Generated"]},
    {"rank": 2, "screen_name": "Application Created", "loaded_events": ["Application_Created"]},
    {"rank": 3, "screen_name": "KYC Completed", "loaded_events": ["KYC_Done"]},
    {"rank": 4, "screen_name": "Loan Disbursed", "loaded_events": ["Loan_Disbursed"]}
]
```

### E-commerce Checkout Funnel
```python
"steps": [
    {"rank": 1, "screen_name": "Product View", "loaded_events": ["product_view"]},
    {"rank": 2, "screen_name": "Add to Cart", "loaded_events": ["add_to_cart"]},
    {"rank": 3, "screen_name": "Checkout Start", "loaded_events": ["checkout_start"]},
    {"rank": 4, "screen_name": "Payment Complete", "loaded_events": ["payment_success"]}
]
```

## 🤝 Support

For issues or questions:

1. Check data validation queries
2. Verify configuration format
3. Review source table schema
4. Ensure BigQuery permissions

## 📄 License

This project is licensed under the MIT License - see the LICENSE file for details.

## �� Version History

- **v1.0**: Initial release with basic funnel analysis
- **v1.1**: Added percentile analysis
- **v1.2**: Added backfill logic
- **v1.3**: Added automatic data validation