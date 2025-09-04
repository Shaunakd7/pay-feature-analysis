#!/usr/bin/env python3
"""
Unified Loans Activities Funnel Analyzer
Takes config embedded in the script and outputs a single standardized table matching STANDARD_FUNNEL schema
"""

import json
import sys
import os
from typing import Dict, Any, List
from google.cloud import bigquery
from google.oauth2 import service_account
import tempfile

# Configuration embedded at the top of the script
FUNNEL_CONFIG = {
    "funnel_name": "Early Salary Funnel",
    "project_id": "common-tech-434709",
    "source_dataset": "cross_db",
    "destination_dataset": "sandbox",
    "data_source": "common-tech-434709.cross_db.unified_loans_activities",
    "user_id_column": "actor_id",
    "session_id_column": "actor_id",  # Using actor_id as session since no properties column
    "event_column": "ACTIVITY_NAME",
    "timestamp_column": "ACTIVITY_TIME_IST",
    "partition_column": "row_updated_time",
    "time_period_days": 9000,
    "steps": [
        {
            "rank": 1,
            "screen_name": "Offer Generated",
            "loaded_events": ["Offer_Generated"]
        },
        {
            "rank": 2,
            "screen_name": "Application Loan Request Created",
            "loaded_events": ["Application Loan Request Created"]
        },
        {
            "rank": 3,
            "screen_name": "Address Step",
            "loaded_events": ["LOAN_STEP_EXECUTION_STEP_NAME_ADDRESS"]
        },
        {
            "rank": 4,
            "screen_name": "Applicant Creation",
            "loaded_events": ["LOAN_STEP_EXECUTION_STEP_NAME_APPLICANT_CREATION"]
        },
        {
            "rank": 5,
            "screen_name": "KYC Document Download",
            "loaded_events": ["LOAN_STEP_EXECUTION_STEP_NAME_KYC_DOCUMENT_DOWNLOAD"]
        },
        {
            "rank": 6,
            "screen_name": "CKYC",
            "loaded_events": ["LOAN_STEP_EXECUTION_STEP_NAME_CKYC"]
        },
        {
            "rank": 7,
            "screen_name": "Liveness Check",
            "loaded_events": ["LOAN_STEP_EXECUTION_STEP_NAME_LIVENESS_CHECK"]
        },
        {
            "rank": 8,
            "screen_name": "Risk Check",
            "loaded_events": ["LOAN_STEP_EXECUTION_STEP_NAME_RISK_CHECK"]
        },
        {
            "rank": 9,
            "screen_name": "AML",
            "loaded_events": ["LOAN_STEP_EXECUTION_STEP_NAME_AML"]
        },
        {
            "rank": 10,
            "screen_name": "Vendor BRE",
            "loaded_events": ["LOAN_STEP_EXECUTION_STEP_NAME_VENDOR_BRE"]
        },
        {
            "rank": 11,
            "screen_name": "Drawdown",
            "loaded_events": ["LOAN_STEP_EXECUTION_STEP_NAME_DRAWDOWN"]
        },
        {
            "rank": 12,
            "screen_name": "Penny Drop",
            "loaded_events": ["LOAN_STEP_EXECUTION_STEP_NAME_PENNY_DROP"]
        },
        {
            "rank": 13,
            "screen_name": "Mandate",
            "loaded_events": ["LOAN_STEP_EXECUTION_STEP_NAME_MANDATE"]
        },
        {
            "rank": 14,
            "screen_name": "Contactability",
            "loaded_events": ["LOAN_STEP_EXECUTION_STEP_NAME_CONTACTABILITY"]
        },
        {
            "rank": 15,
            "screen_name": "KFS",
            "loaded_events": ["LOAN_STEP_EXECUTION_STEP_NAME_KFS"]
        },
        {
            "rank": 16,
            "screen_name": "VKYC",
            "loaded_events": ["LOAN_STEP_EXECUTION_STEP_NAME_VKYC"]
        },
        {
            "rank": 17,
            "screen_name": "Loan Amount Disbursal",
            "loaded_events": ["LOAN_STEP_EXECUTION_STEP_NAME_LOAN_AMOUNT_DISBURSAL"]
        },
        {
            "rank": 18,
            "screen_name": "Loan Account Created",
            "loaded_events": ["Loan Account Created"]
        }
    ],
    "filters": [
        "LOAN_PROGRAM = 'LOAN_PROGRAM_EARLY_SALARY_V2'",
        "PARTNER = 'stock_guardian'"
    ],
    "create_percentiles_table": True,
    "percentiles_table_name": "early_salary_percentiles_by_stage"
}

class UnifiedLoansFunnelAnalyzer:
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
        
        # Build filter conditions
        filter_conditions = " AND ".join(config['filters'])
        
        # Query to check event counts
        query = f"""
        SELECT 
            ACTIVITY_NAME as event,
            COUNT(*) as count
        FROM `{config['data_source']}`
        WHERE ACTIVITY_NAME in ('{events_list}')
        AND {filter_conditions}
        AND date({config['timestamp_column']}) >= date_sub(current_date(), interval {config['time_period_days']} day)
        GROUP BY ACTIVITY_NAME
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
        
        print(f"�� Final funnel: {len(filtered_steps)} steps with available events")
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
            step_cases.append(f"when {config['event_column']} in ('{events_list}') then {step['rank']}")
            screen_name_cases.append(f"when {config['event_column']} in ('{events_list}') then '{step['screen_name']}'")
        
        step_case = "\n        ".join(step_cases)
        screen_name_case = "\n        ".join(screen_name_cases)
        
        # Build filter conditions
        filter_conditions = " AND ".join(config['filters'])
        
        # Build funnel completion logic
        funnel_completion_cases = []
        for i, step in enumerate(filtered_steps):
            step_num = step['rank']
            screen_name = step['screen_name']
            # For each step, create a CASE that fills in missing steps
            funnel_completion_cases.append(f"""
        -- Step {step_num}: {screen_name}
        select 
            actor_id,
            actor_id as session_id,
            '{screen_name}' as screen_name,
            case 
                when step_stage = {step_num} then screen_entry_time
                else timestamp_sub(screen_entry_time, interval {step_num - 1} minute)
            end as screen_entry_time,
            {step_num} as step_stage,
            null as current_tier,
            null as affluence_v11_flag,
            null as gender,
            null as income_range,
            null as app_version,
            null as operating_system,
            null as entry_point,
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
create or replace table `{config['project_id']}.{config['destination_dataset']}.EARLY_SALARY_STANDARD_FUNNEL` as (
with funnel_base as (
    select 
        e.{config['user_id_column']} as actor_id,
        e.{config['user_id_column']} as session_id,
        e.{config['event_column']} as event,
        e.{config['timestamp_column']} as screen_entry_time,
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
    where {filter_conditions}
    and date({config['timestamp_column']}) >= date_sub(current_date(), interval {config['time_period_days']} day)
    and e.{config['user_id_column']} is not null
)
,funnel_clean as (
    select 
        actor_id,
        session_id,
        screen_name,
        screen_entry_time,
        step_stage,
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
    null as current_tier,
    null as affluence_v11_flag,
    null as gender,
    null as income_range,
    session_id,
    null as app_version,
    null as operating_system,
    null as entry_point,
    screen_name,
    screen_entry_time,
    cast(step_stage as string) as step,
    step_stage,
    previous_screen_name,
    screen_success_time,
    is_conversion_session,
    time_to_convert,
    current_timestamp() as table_created_at,
    -- Add aggregated fields for Tableau
    COUNT(*) OVER (PARTITION BY step_stage) as total_users,
    COUNT(*) OVER (PARTITION BY 1) as total_users_first_step
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
      `{config['project_id']}.{config['destination_dataset']}.EARLY_SALARY_STANDARD_FUNNEL`
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
    WHEN screen_name = 'Offer Generated' THEN '1 - Offer Generated'
    WHEN screen_name = 'Application Loan Request Created' THEN '2 - Application Loan Request Created'
    WHEN screen_name = 'Address Step' THEN '3 - Address Step'
    WHEN screen_name = 'Applicant Creation' THEN '4 - Applicant Creation'
    WHEN screen_name = 'KYC Document Download' THEN '5 - KYC Document Download'
    WHEN screen_name = 'CKYC' THEN '6 - CKYC'
    WHEN screen_name = 'Liveness Check' THEN '7 - Liveness Check'
    WHEN screen_name = 'Risk Check' THEN '8 - Risk Check'
    WHEN screen_name = 'AML' THEN '9 - AML'
    WHEN screen_name = 'Vendor BRE' THEN '10 - Vendor BRE'
    WHEN screen_name = 'Drawdown' THEN '11 - Drawdown'
    WHEN screen_name = 'Penny Drop' THEN '12 - Penny Drop'
    WHEN screen_name = 'Mandate' THEN '13 - Mandate'
    WHEN screen_name = 'Contactability' THEN '14 - Contactability'
    WHEN screen_name = 'KFS' THEN '15 - KFS'
    WHEN screen_name = 'VKYC' THEN '16 - VKYC'
    WHEN screen_name = 'Loan Amount Disbursal' THEN '17 - Loan Amount Disbursal'
    WHEN screen_name = 'Loan Account Created' THEN '18 - Loan Account Created'
    ELSE screen_name
  END AS ordered_screen_name,
  CASE 
    WHEN screen_name = 'Offer Generated' THEN 1
    WHEN screen_name = 'Application Loan Request Created' THEN 2
    WHEN screen_name = 'Address Step' THEN 3
    WHEN screen_name = 'Applicant Creation' THEN 4
    WHEN screen_name = 'KYC Document Download' THEN 5
    WHEN screen_name = 'CKYC' THEN 6
    WHEN screen_name = 'Liveness Check' THEN 7
    WHEN screen_name = 'Risk Check' THEN 8
    WHEN screen_name = 'AML' THEN 9
    WHEN screen_name = 'Vendor BRE' THEN 10
    WHEN screen_name = 'Drawdown' THEN 11
    WHEN screen_name = 'Penny Drop' THEN 12
    WHEN screen_name = 'Mandate' THEN 13
    WHEN screen_name = 'Contactability' THEN 14
    WHEN screen_name = 'KFS' THEN 15
    WHEN screen_name = 'VKYC' THEN 16
    WHEN screen_name = 'Loan Amount Disbursal' THEN 17
    WHEN screen_name = 'Loan Account Created' THEN 18
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
            print("📊 Step 1: Creating EARLY_SALARY_STANDARD_FUNNEL table...")
            sql = self.generate_standard_funnel_sql(config)
            print(f"Generated SQL for {config['funnel_name']}")
            
            # Execute SQL
            print("Executing SQL...")
            query_job = self.client.query(sql)
            query_job.result()  # Wait for completion
            
            print(f"✅ Successfully created EARLY_SALARY_STANDARD_FUNNEL table at: {config['project_id']}.{config['destination_dataset']}.EARLY_SALARY_STANDARD_FUNNEL")
            
            # Get table info
            table_ref = self.client.dataset(config['destination_dataset']).table('EARLY_SALARY_STANDARD_FUNNEL')
            table = self.client.get_table(table_ref)
            print(f"�� EARLY_SALARY_STANDARD_FUNNEL table created with {table.num_rows:,} rows and {len(table.schema)} columns")
            
            # Step 2: Create percentiles table (only if enabled and STANDARD_FUNNEL was successful)
            if config.get('create_percentiles_table', False):
                print(f"\n�� Step 2: Creating {config['percentiles_table_name']} table...")
                
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
        analyzer = UnifiedLoansFunnelAnalyzer(project_id="common-tech-434709")
        config = analyzer.get_config()
        
        # Execute analysis
        success = analyzer.execute_analysis(config)
        
        if success:
            print("\n🎉 Early Salary Funnel analysis completed successfully!")
            print("�� Tables created:")
            print(f"   - {config['project_id']}.{config['destination_dataset']}.EARLY_SALARY_STANDARD_FUNNEL")
            if config.get('create_percentiles_table', False):
                print(f"   - {config['project_id']}.{config['destination_dataset']}.{config['percentiles_table_name']}")
            sys.exit(0)
        else:
            print("💥 Early Salary Funnel analysis failed!")
            sys.exit(1)
            
    except Exception as e:
        print(f"�� Error: {str(e)}")
        sys.exit(1)

if __name__ == "__main__":
    main() 