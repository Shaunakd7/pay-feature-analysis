#!/usr/bin/env python3
"""
Test script with smaller dataset (7 days instead of 30)
"""

import json
import os
from funnel_generator import FunnelGenerator
from google.cloud import bigquery

def test_small_dataset():
    """Test the funnel generator with a smaller dataset"""
    
    # Load the config file
    config_file_path = "templates/sample_events_config.json"
    
    print(f"Loading config from: {config_file_path}")
    
    try:
        with open(config_file_path, 'r') as f:
            config = json.load(f)
        print("✅ Config file loaded successfully")
    except Exception as e:
        print(f"❌ Error loading config file: {e}")
        return
    
    # Modify config for smaller dataset
    config['time_period_days'] = 1  # Change from 30 to 1 day
    print(f"\n📋 Modified Config Details:")
    print(f"  Funnel Name: {config.get('funnel_name')}")
    print(f"  Project ID: {config.get('project_id')}")
    print(f"  Time Period: {config.get('time_period_days')} day (reduced from 30)")
    print(f"  Data Source: {config.get('data_source')}")
    
    # Generate SQL
    print(f"\n🔧 Generating SQL...")
    try:
        generator = FunnelGenerator()
        sql = generator.generate_sql(config)
        print("✅ SQL generated successfully")
        
        # Save SQL to file for inspection
        sql_file_path = "generated_funnel_7days.sql"
        with open(sql_file_path, 'w') as f:
            f.write(sql)
        print(f"📄 SQL saved to: {sql_file_path}")
        
    except Exception as e:
        print(f"❌ Error generating SQL: {e}")
        return
    
    # Execute in BigQuery with timeout
    print(f"\n🚀 Attempting to execute in BigQuery (7 days data)...")
    
    try:
        # Initialize BigQuery client
        client = bigquery.Client(project=config['project_id'])
        print(f"✅ BigQuery client initialized for project: {config['project_id']}")
        
        # Split SQL by semicolon and execute each statement
        statements = [stmt.strip() for stmt in sql.split(';') if stmt.strip()]
        
        print(f"📊 Executing {len(statements)} SQL statements...")
        
        for i, statement in enumerate(statements, 1):
            print(f"  Statement {i}/{len(statements)}: {statement[:100]}...")
            
            query_job = client.query(statement)
            
            try:
                query_job.result(timeout=300)  # Wait for completion with 5 min timeout
                print(f"  ✅ Statement {i} completed")
            except Exception as e:
                print(f"  ❌ Statement {i} failed or timed out: {e}")
                return
        
        print("🎉 All SQL statements executed successfully!")
        
        # Query the results table to get data for display
        metrics_table = f"{config['project_id']}.{config['destination_dataset']}.{config['funnel_name']}_metrics"
        
        print(f"\n📈 Querying results from: {metrics_table}")
        
        metrics_query = f"""
        SELECT * FROM `{metrics_table}`
        ORDER BY analysis_level, step_stage
        LIMIT 10
        """
        
        query_job = client.query(metrics_query)
        results_data = []
        for row in query_job:
            results_data.append(dict(row.items()))
        
        print(f"✅ Retrieved {len(results_data)} result rows")
        
        # Display sample results
        if results_data:
            print(f"\n📊 Sample Results:")
            for i, row in enumerate(results_data[:3]):
                print(f"  Row {i+1}: {row}")
        
    except Exception as e:
        print(f"❌ Error executing in BigQuery: {e}")
        print(f"💡 This might be due to authentication or permissions.")

if __name__ == "__main__":
    test_small_dataset() 