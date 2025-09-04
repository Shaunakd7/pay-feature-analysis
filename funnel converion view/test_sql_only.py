#!/usr/bin/env python3
"""
Test script to only generate SQL without executing it
"""

import json
from funnel_generator import FunnelGenerator

def test_sql_generation_only():
    """Test the funnel generator to only generate SQL without executing"""
    
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
        
        # Show a preview of the SQL
        print(f"\n📝 SQL Preview (first 500 characters):")
        print("-" * 50)
        print(sql[:500])
        print("-" * 50)
        
        # Count SQL statements
        statements = [stmt.strip() for stmt in sql.split(';') if stmt.strip()]
        print(f"\n📊 SQL Analysis:")
        print(f"  Number of statements: {len(statements)}")
        for i, stmt in enumerate(statements, 1):
            print(f"  Statement {i}: {stmt[:100]}...")
        
        print(f"\n✅ SQL generation completed successfully!")
        print(f"💡 You can now manually execute the SQL in BigQuery console if needed.")
        
    except Exception as e:
        print(f"❌ Error generating SQL: {e}")
        return

if __name__ == "__main__":
    test_sql_generation_only() 