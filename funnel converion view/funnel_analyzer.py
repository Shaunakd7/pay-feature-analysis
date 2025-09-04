#!/usr/bin/env python3
"""
Standalone Funnel Analyzer
Takes config embedded in the script and outputs a single standardized table matching STANDARD_FUNNEL schema
"""

import json
import sys
import os
from typing import Dict, Any, List
from google.cloud import bigquery
from google.oauth2 import service_account
import tempfile
# upload your funnel config below:
# soiurce dataset is is the dataset in which your table is present
# destination dataset is the dataset in which you want to create the tables
# data source is the table in which your events are present

# Configuration embedded at the top of the script
FUNNEL_CONFIG = {
    "funnel_name": "Tiering and Add Funds Funnel",
    "project_id": "common-tech-434709",
    "source_dataset": "events",
    "destination_dataset": "sandbox",
    "data_source": "common-tech-434709.events.events", # add the required path
    "user_id_column": "user_id",
    "session_id_column": "JSON_EXTRACT_SCALAR(properties, '$.session_id')",
    "event_column": "event",
    "timestamp_column": "timestamp",
    "partition_column": "date",
    "time_period_days": 30,# set the time perios for the data
    # below is the funnel steps, you can add more steps if you want to
    # the loaded events are the events that are loaded on the screen and are present in the data source table
    "steps": [
        {
            "rank": 1,
            "screen_name": "Tiering - Overview screen",
            "loaded_events": ["LoadedTierOverviewScreen"]
        },
        {
            "rank": 2,
            "screen_name": "Tiering - Add money to Fi",
            "loaded_events": ["ClickedJoinTierButton"]
        },
        {
            "rank": 3,
            "screen_name": "Add funds - Enter amount",
            "loaded_events": ["ClickedAddFundsButton"]
        },
        {
            "rank": 4,
            "screen_name": "Add funds - Payment options",
            "loaded_events": ["ClickedAddFundCtaPO"]
        },
        {
            "rank": 5,
            "screen_name": "Add funds - Payment success",
            "loaded_events": ["BalanceUpdateEvent"]
        },
        {
            "rank": 6,
            "screen_name": "Tiering - Upgrade screen",
            "loaded_events": ["UserUpgradedTiering"]
        }
    ],
    "filters": [
        "event in ('LoadedTierOverviewScreen', 'ClickedJoinTierButton', 'ClickedAddFundsButton', 'ClickedAddFundCtaPO', 'BalanceUpdateEvent', 'UserUpgradedTiering')"
    ],
    "create_percentiles_table": True,
    "percentiles_table_name": "percentiles_by_stage"
}
# place your config above this line ☝
# below is the code that runs the funnel analyser

class StandardFunnelAnalyzer:
    def __init__(self, project_id: str, credentials_path: str = None):
        """Initialize with BigQuery client"""
        if credentials_path:
            # Use service account credentials
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials_path
        else:
            # Use default credentials (SSO)
            if 'GOOGLE_APPLICATION_CREDENTIALS' in os.environ:
                del os.environ['GOOGLE_APPLICATION_CREDENTIALS']
        
        self.client = bigquery.Client(project=project_id)
        self.project_id = project_id
    
    def get_config(self) -> Dict[str, Any]:
        """Get the embedded configuration"""
        return FUNNEL_CONFIG
    
    def validate_config(self, config: Dict[str, Any]) -> bool:
        """Validate configuration"""
        required_fields = [
            'funnel_name', 'project_id', 'source_dataset', 'destination_dataset',
            'data_source', 'user_id_column', 'session_id_column', 'event_column',
            'timestamp_column', 'partition_column', 'time_period_days', 'steps'
        ]
        
        for field in required_fields:
            if field not in config:
                raise ValueError(f"Missing required field: {field}")
        
        if not isinstance(config['steps'], list) or len(config['steps']) == 0:
            raise ValueError("Steps must be a non-empty array")
        
        return True
    
    def check_event_availability(self, config: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Check which events actually exist in the data and filter out missing ones"""
        print("🔍 Checking event availability in data...")
        
        # Build event list from config
        all_events = []
        for step in config['steps']:
            all_events.extend(step['loaded_events'])
        
        events_list = "', '".join(all_events)
        
        # Query to check event counts
        query = f"""
        SELECT 
            event,
            COUNT(*) as count
        FROM `{config['data_source']}`
        WHERE event in ('{events_list}')
        AND {config['partition_column']} >= date_sub(current_date(), interval {config['time_period_days']} day)
        GROUP BY event
        ORDER BY count DESC
        """
        
        query_job = self.client.query(query)
        results = list(query_job)
        
        # Create lookup of available events
        available_events = {row.event: row.count for row in results}
        
        # Filter steps to only include those with available events
        filtered_steps = []
        for step in config['steps']:
            step_events = step['loaded_events']
            available_step_events = [event for event in step_events if event in available_events]
            
            if available_step_events:
                # Update step with only available events
                step['loaded_events'] = available_step_events
                step['original_events'] = step_events  # Keep original for reference
                step['event_count'] = sum(available_events[event] for event in available_step_events)
                filtered_steps.append(step)
                print(f"✅ Step {step['rank']} ({step['screen_name']}): {len(available_step_events)}/{len(step_events)} events available ({step['event_count']:,} occurrences)")
            else:
                print(f"⚠️ Step {step['rank']} ({step['screen_name']}): No events available, skipping")
        
        # Re-rank the steps to maintain sequential order
        for i, step in enumerate(filtered_steps):
            step['rank'] = i + 1
        
        print(f"📊 Final funnel: {len(filtered_steps)} steps with available events")
        return filtered_steps
    
    def generate_standard_funnel_sql(self, config: Dict[str, Any]) -> str:
        """Generate SQL to create STANDARD_FUNNEL table with exact schema"""
        
        # Check event availability and filter steps
        filtered_steps = self.check_event_availability(config)
        
        if not filtered_steps:
            raise ValueError("No steps with available events found. Check your event names.")
        
        # Build the step mapping for the CASE statement
        step_cases = []
        screen_name_cases = []
        
        for step in filtered_steps:
            events = step['loaded_events']
            events_list = "', '".join(events)
            step_cases.append(f"when event in ('{events_list}') then {step['rank']}")
            screen_name_cases.append(f"when event in ('{events_list}') then '{step['screen_name']}'")
        
        step_case = "\n        ".join(step_cases)
        screen_name_case = "\n        ".join(screen_name_cases)
        
        # Build extraction fields
        extraction_fields = []
        if config.get('app_version_extraction'):
            extraction_fields.append(f"SAFE_CAST(JSON_EXTRACT_SCALAR(properties, '$.{config['app_version_extraction'].replace(':', '.')}') AS STRING) as app_version")
        else:
            extraction_fields.append("null as app_version")
        
        if config.get('os_extraction'):
            extraction_fields.append(f"SAFE_CAST(JSON_EXTRACT_SCALAR(properties, '$.{config['os_extraction'].replace(':', '.')}') AS STRING) as operating_system")
        else:
            extraction_fields.append("null as operating_system")
        
        if config.get('entry_point_extraction'):
            extraction_fields.append(f"SAFE_CAST(JSON_EXTRACT_SCALAR(properties, '$.{config['entry_point_extraction'].replace(':', '.')}') AS STRING) as entry_point")
        else:
            extraction_fields.append("null as entry_point")
        
        # Build user segmentation fields - this is to include tables with user details to left join to the main table 
        # tableau does not pick this up because it does not use this user info - potentially can remove this section, but keeping it
        # just in case we aim to use
        user_seg_fields = []
        if config.get('user_base_table') and config.get('user_segmentation_fields'):
            for field in config['user_segmentation_fields']:
                user_seg_fields.append(f"u.{field}")
        else:
            user_seg_fields = ["null as current_tier", "null as affluence_v11_flag", "null as gender", "null as income_range"]
        
        # Build user base table join
        user_join = ""
        if config.get('user_base_table'):
            user_join = f"left join `{config['user_base_table']}` u on e.{config['user_id_column']} = u.{config['user_id_column']}"
        
        # Build filters - use only available events
        available_events = []
        for step in filtered_steps:
            available_events.extend(step['loaded_events'])
        
        events_filter = "', '".join(available_events)
        filter_clause = f"event in ('{events_filter}')"
        
        # Build funnel completion logic
        funnel_completion_cases = []
        for i, step in enumerate(filtered_steps):
            step_num = step['rank']
            screen_name = step['screen_name']
            # For each step, create a CASE that fills in missing steps
            # this is the backfil logic, using case statements
            funnel_completion_cases.append(f"""
        -- Step {step_num}: {screen_name}
        select 
            actor_id,
            session_id,
            '{screen_name}' as screen_name,
            case 
                when step_stage = {step_num} then screen_entry_time
                else timestamp_sub(screen_entry_time, interval {step_num - 1} minute)
            end as screen_entry_time,
            {step_num} as step_stage,
            current_tier, 
            affluence_v11_flag,
            gender,
            income_range,
            app_version,
            operating_system,
            entry_point,
            case 
                when step_stage = {step_num} then previous_screen_name
                when {step_num} = 1 then null
                else '{filtered_steps[i-1]['screen_name'] if i > 0 else 'null'}'
            end as previous_screen_name,
            case 
                when step_stage = {step_num} then screen_success_time
                when {step_num} = {len(filtered_steps)} then null
                else timestamp_add(screen_entry_time, interval 1 minute)
            end as screen_success_time,
            case 
                when step_stage = {step_num} then is_conversion_session
                when {step_num} = {len(filtered_steps)} then 0
                else 1
            end as is_conversion_session,
            case 
                when step_stage = {step_num} then time_to_convert
                when {step_num} = {len(filtered_steps)} then null
                else 60
            end as time_to_convert
        from funnel_clean
        where step_stage >= {step_num}""")
        
        funnel_completion_sql = "\nunion all\n".join(funnel_completion_cases)
        
        sql = f"""
-- Create STANDARD_FUNNEL table with exact schema
-- Using funnel completion logic to fill in missing steps
create or replace table `{config['project_id']}.{config['destination_dataset']}.STANDARD_FUNNEL` as (
with funnel_base as (
    select 
        e.{config['user_id_column']} as actor_id,
        {config['session_id_column']} as session_id,
        e.{config['event_column']} as event,
        e.{config['timestamp_column']} as screen_entry_time,
        e.properties,
        -- User segmentation fields
        {', '.join(user_seg_fields)},
        -- Extraction fields
        {', '.join(extraction_fields)},
        -- Step mapping (only for events that exist)
        case 
        {step_case}
        else null
        end as step_stage,
        case 
        {screen_name_case}
        else e.{config['event_column']}
        end as screen_name
    from `{config['data_source']}` e
    {user_join}
    where {filter_clause}
    and {config['partition_column']} >= date_sub(current_date(), interval {config['time_period_days']} day)
    and date(e.{config['timestamp_column']}) >= date_sub(current_date(), interval {config['time_period_days']} day)
    and {config['session_id_column']} is not null
)
,funnel_clean as (
    select 
        actor_id,
        session_id,
        screen_name,
        screen_entry_time,
        step_stage,
        current_tier,
        affluence_v11_flag,
        gender,
        income_range,
        app_version,
        operating_system,
        entry_point,
        -- Previous screen name
        lag(screen_name) over (partition by actor_id, session_id order by screen_entry_time) as previous_screen_name,
        -- Next screen entry time
        lead(screen_entry_time) over (partition by actor_id, session_id order by screen_entry_time) as screen_success_time,
        -- Is conversion session (has next step)
        case when lead(screen_entry_time) over (partition by actor_id, session_id order by screen_entry_time) is not null then 1 else 0 end as is_conversion_session,
        -- Time to convert
        case when lead(screen_entry_time) over (partition by actor_id, session_id order by screen_entry_time) is not null 
             then timestamp_diff(lead(screen_entry_time) over (partition by actor_id, session_id order by screen_entry_time), screen_entry_time, second) 
             else null end as time_to_convert
    from funnel_base
    where step_stage is not null
    qualify row_number() over (partition by actor_id, session_id, screen_name, step_stage order by step_stage desc, screen_entry_time desc) = 1
)
,funnel_completed as (
    -- Apply funnel completion logic: if user reaches step N, include them in steps 1 to N-1
{funnel_completion_sql}
)
select 
    actor_id,
    current_tier,
    affluence_v11_flag,
    gender,
    income_range,
    session_id,
    app_version,
    operating_system,
    entry_point,
    screen_name,
    screen_entry_time,
    cast(step_stage as string) as step,
    step_stage,
    previous_screen_name,
    screen_success_time,
    is_conversion_session,
    time_to_convert,
    current_timestamp() as table_created_at
from funnel_completed
order by actor_id, session_id, step_stage, screen_entry_time
)
"""
        return sql
    
    def generate_percentiles_sql(self, config: Dict[str, Any]) -> str:
        """Generate SQL to create percentiles_by_stage table with step order from config"""
        sql = f"""
CREATE OR REPLACE TABLE `{config['project_id']}.{config['destination_dataset']}.{config['percentiles_table_name']}` AS
WITH
  ordered_events AS (
    SELECT
      actor_id,
      step_stage,
      screen_name,
      screen_entry_time,
      LEAD(screen_entry_time) OVER (
        PARTITION BY actor_id
        ORDER BY screen_entry_time -- Order by time for consecutive events
      ) AS next_screen_entry_time,
      LEAD(step_stage) OVER (
        PARTITION BY actor_id
        ORDER BY screen_entry_time
      ) AS next_step_stage
    FROM
      `{config['project_id']}.{config['destination_dataset']}.STANDARD_FUNNEL`
  ),
  calculated_times AS (
    SELECT
      actor_id,
      step_stage,
      screen_name,
      next_step_stage, -- Keep next_step_stage to understand the transition
      TIMESTAMP_DIFF(next_screen_entry_time, screen_entry_time, SECOND) AS time_to_next_step_seconds
    FROM
      ordered_events
    WHERE
      next_screen_entry_time IS NOT NULL AND TIMESTAMP_DIFF(next_screen_entry_time, screen_entry_time, SECOND) >= 0 -- Ensure non-negative time
  ),
  aggregated_times AS (
    SELECT
      screen_name,
      APPROX_QUANTILES(time_to_next_step_seconds, 100)[OFFSET(25)] AS percentile_25,
      APPROX_QUANTILES(time_to_next_step_seconds, 100)[OFFSET(50)] AS percentile_50,
      APPROX_QUANTILES(time_to_next_step_seconds, 100)[OFFSET(75)] AS percentile_75
    FROM
      calculated_times
    GROUP BY
      screen_name
  )
SELECT
  screen_name,
  CASE 
    WHEN screen_name = 'Tiering - Overview screen' THEN '1 - Tiering - Overview screen'
    WHEN screen_name = 'Tiering - Add money to Fi' THEN '2 - Tiering - Add money to Fi'
    WHEN screen_name = 'Add funds - Enter amount' THEN '3 - Add funds - Enter amount'
    WHEN screen_name = 'Add funds - Payment options' THEN '4 - Add funds - Payment options'
    WHEN screen_name = 'Add funds - Payment success' THEN '5 - Add funds - Payment success'
    WHEN screen_name = 'Tiering - Upgrade screen' THEN '6 - Tiering - Upgrade screen'
    ELSE screen_name
  END AS ordered_screen_name,
  CASE 
    WHEN screen_name = 'Tiering - Overview screen' THEN 1
    WHEN screen_name = 'Tiering - Add money to Fi' THEN 2
    WHEN screen_name = 'Add funds - Enter amount' THEN 3
    WHEN screen_name = 'Add funds - Payment options' THEN 4
    WHEN screen_name = 'Add funds - Payment success' THEN 5
    WHEN screen_name = 'Tiering - Upgrade screen' THEN 6
    ELSE 999
  END AS step_order,
  percentile_25,
  percentile_50,
  percentile_75
FROM
  aggregated_times
ORDER BY
  step_order
"""
        return sql
    
    def execute_analysis(self, config: Dict[str, Any]) -> bool:
        """Execute the funnel analysis and create both STANDARD_FUNNEL and percentiles tables"""
        try:
            # Validate config
            self.validate_config(config)
            
            # Step 1: Generate and execute STANDARD_FUNNEL SQL
            print("📊 Step 1: Creating STANDARD_FUNNEL table...")
            sql = self.generate_standard_funnel_sql(config)
            print(f"Generated SQL for {config['funnel_name']}")
            
            # Execute SQL
            print("Executing SQL...")
            query_job = self.client.query(sql)
            query_job.result()  # Wait for completion
            
            print(f"✅ Successfully created STANDARD_FUNNEL table at: {config['project_id']}.{config['destination_dataset']}.STANDARD_FUNNEL")
            
            # Get table info
            table_ref = self.client.dataset(config['destination_dataset']).table('STANDARD_FUNNEL')
            table = self.client.get_table(table_ref)
            print(f"📊 STANDARD_FUNNEL table created with {table.num_rows:,} rows and {len(table.schema)} columns")
            
            # Step 2: Create percentiles table (only if enabled and STANDARD_FUNNEL was successful)
            if config.get('create_percentiles_table', False):
                print(f"\n📊 Step 2: Creating {config['percentiles_table_name']} table...")
                
                # Generate percentiles SQL
                percentiles_sql = self.generate_percentiles_sql(config)
                print(f"Generated percentiles SQL")
                
                # Execute percentiles SQL
                print("Executing percentiles SQL...")
                percentiles_job = self.client.query(percentiles_sql)
                percentiles_job.result()  # Wait for completion
                
                print(f"✅ Successfully created {config['percentiles_table_name']} table at: {config['project_id']}.{config['destination_dataset']}.{config['percentiles_table_name']}")
                
                # Get percentiles table info
                percentiles_table_ref = self.client.dataset(config['destination_dataset']).table(config['percentiles_table_name'])
                percentiles_table = self.client.get_table(percentiles_table_ref)
                print(f"📊 {config['percentiles_table_name']} table created with {percentiles_table.num_rows:,} rows and {len(percentiles_table.schema)} columns")
            else:
                print("⏭️ Skipping percentiles table creation (disabled in config)")
            
            return True
            
        except Exception as e:
            print(f"❌ Error executing analysis: {str(e)}")
            return False

def main():
    """Main function to run the funnel analyzer"""
    try:
        # Initialize analyzer with embedded config
        analyzer = StandardFunnelAnalyzer(project_id="common-tech-434709")
        config = analyzer.get_config()
        
        # Execute analysis
        success = analyzer.execute_analysis(config)
        
        if success:
            print("\n🎉 Funnel analysis completed successfully!")
            print("📊 Tables created:")
            print(f"   - {config['project_id']}.{config['destination_dataset']}.STANDARD_FUNNEL")
            if config.get('create_percentiles_table', False):
                print(f"   - {config['project_id']}.{config['destination_dataset']}.{config['percentiles_table_name']}")
            sys.exit(0)
        else:
            print("💥 Funnel analysis failed!")
            sys.exit(1)
            
    except Exception as e:
        print(f"💥 Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 