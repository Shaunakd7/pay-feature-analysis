#!/usr/bin/env python3
"""
Test script to directly test the funnel generator with a config file
"""

import json
import os
from funnel_generator import FunnelGenerator
from google.cloud import bigquery

def test_funnel_generator():
    """Test the funnel generator directly with a config file"""
    
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
    
    # Print config details
    print(f"\n📋 Config Details:")
    print(f"  Funnel Name: {config.get('funnel_name')}")
    print(f"  Project ID: {config.get('project_id')}")
    print(f"  Source Dataset: {config.get('source_dataset')}")
    print(f"  Destination Dataset: {config.get('destination_dataset')}")
    print(f"  Data Source: {config.get('data_source')}")
    print(f"  Steps: {len(config.get('steps', []))}")
    
    # Generate SQL
    print(f"\n🔧 Generating SQL...")
    try:
        generator = FunnelGenerator()
        sql = generator.generate_sql(config)
        print("✅ SQL generated successfully")
        
        # Save SQL to file for inspection
        sql_file_path = "generated_funnel.sql"
        with open(sql_file_path, 'w') as f:
            f.write(sql)
        print(f"📄 SQL saved to: {sql_file_path}")
        
    except Exception as e:
        print(f"❌ Error generating SQL: {e}")
        return
    
    # Execute in BigQuery (if credentials are available)
    print(f"\n🚀 Attempting to execute in BigQuery...")
    
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
            query_job.result()  # Wait for completion
            print(f"  ✅ Statement {i} completed")
        
        print("🎉 All SQL statements executed successfully!")
        
        # Query the results table to get data for display
        metrics_table = f"{config['project_id']}.{config['destination_dataset']}.{config['funnel_name']}_metrics"
        funnel_table = f"{config['project_id']}.{config['destination_dataset']}.{config['funnel_name']}_funnel"
        
        print(f"\n📈 Querying results from: {metrics_table}")
        
        metrics_query = f"""
        SELECT * FROM `{metrics_table}`
        ORDER BY analysis_level, step_stage
        LIMIT 20
        """
        
        query_job = client.query(metrics_query)
        results_data = []
        for row in query_job:
            results_data.append(dict(row.items()))
        
        print(f"✅ Retrieved {len(results_data)} result rows")
        
        # Display sample results
        if results_data:
            print(f"\n📊 Sample Results:")
            for i, row in enumerate(results_data[:5]):
                print(f"  Row {i+1}: {row}")
        
        # Check table sizes
        print(f"\n📋 Table Information:")
        
        # Check metrics table
        try:
            metrics_count_query = f"SELECT COUNT(*) as count FROM `{metrics_table}`"
            metrics_count_job = client.query(metrics_count_query)
            metrics_count = next(metrics_count_job.result()).count
            print(f"  {metrics_table}: {metrics_count} rows")
        except Exception as e:
            print(f"  ❌ Error checking {metrics_table}: {e}")
        
        # Check funnel table
        try:
            funnel_count_query = f"SELECT COUNT(*) as count FROM `{funnel_table}`"
            funnel_count_job = client.query(funnel_count_query)
            funnel_count = next(funnel_count_job.result()).count
            print(f"  {funnel_table}: {funnel_count} rows")
        except Exception as e:
            print(f"  ❌ Error checking {funnel_table}: {e}")
        
    except Exception as e:
        print(f"❌ Error executing in BigQuery: {e}")
        print(f"💡 This might be due to authentication or permissions. Check your gcloud auth status.")
        print(f"💡 Run: gcloud auth application-default login")

if __name__ == "__main__":
    test_funnel_generator() 