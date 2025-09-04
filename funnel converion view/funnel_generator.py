#!/usr/bin/env python3
"""
Generic Funnel Generator
Creates funnel analysis SQL from configuration files using Jinja2 templates.
"""

import json
import argparse
import os
from typing import Dict, Any
import jinja2
import snowflake.connector
from pathlib import Path


class FunnelGenerator:
    def __init__(self, template_path: str = "funnel_template.sql"):
        """
        Initialize the funnel generator with a SQL template.
        
        Args:
            template_path: Path to the Jinja2 SQL template
        """
        self.template_path = template_path
        self.jinja_env = jinja2.Environment(
            loader=jinja2.FileSystemLoader('.'),
            trim_blocks=True,
            lstrip_blocks=True
        )
    
    def load_config(self, config_path: str) -> Dict[str, Any]:
        """
        Load configuration from JSON file.
        
        Args:
            config_path: Path to the configuration JSON file
            
        Returns:
            Dictionary containing the configuration
        """
        if not os.path.exists(config_path):
            raise FileNotFoundError(f"Configuration file not found: {config_path}")
        
        with open(config_path, 'r') as f:
            config = json.load(f)
        
        # Validate required fields
        required_fields = [
            'funnel_name', 'database', 'schema', 'data_source', 
            'user_base_table', 'time_period_days', 'steps', 'filters'
        ]
        
        for field in required_fields: # this part may beed alteration later
            if field not in config:
                raise ValueError(f"Missing required field in config: {field}")
        
        return config
    
    def generate_sql(self, config: Dict[str, Any]) -> str:
        """
        Generate SQL from template and configuration.
        
        Args:
            config: Configuration dictionary
            
        Returns:
            Generated SQL string
        """
        if not os.path.exists(self.template_path):
            raise FileNotFoundError(f"Template file not found: {self.template_path}")
        
        template = self.jinja_env.get_template(self.template_path)
        
        # Generate the SQL
        sql = template.render(**config)
        return sql
    
    def save_sql(self, sql: str, output_path: str) -> None:
        """
        Save generated SQL to a file.
        
        Args:
            sql: Generated SQL string
            output_path: Path to save the SQL file
        """
        with open(output_path, 'w') as f:
            f.write(sql)
        print(f"SQL saved to: {output_path}")
    
    def execute_sql(self, sql: str, connection_params: Dict[str, str]) -> None:
        """
        Execute the generated SQL against Snowflake.
        
        Args:
            sql: Generated SQL string
            connection_params: Snowflake connection parameters
        """
        try:
            # Connect to Snowflake
            conn = snowflake.connector.connect(
                user=connection_params.get('user'),
                password=connection_params.get('password'),
                account=connection_params.get('account'),
                warehouse=connection_params.get('warehouse'),
                database=connection_params.get('database'),
                schema=connection_params.get('schema')
            )
            
            cursor = conn.cursor()
            
            # Split SQL by semicolon and execute each statement
            statements = [stmt.strip() for stmt in sql.split(';') if stmt.strip()]
            
            for statement in statements:
                print(f"Executing: {statement[:100]}...")
                cursor.execute(statement)
            
            cursor.close()
            conn.close()
            print("SQL executed successfully!")
            
        except Exception as e:
            print(f"Error executing SQL: {str(e)}")
            raise
    
    def generate_and_save(self, config_path: str, output_path: str = None) -> str:
        """
        Generate SQL from config and save to file.
        
        Args:
            config_path: Path to configuration file
            output_path: Path to save generated SQL (optional)
            
        Returns:
            Generated SQL string
        """
        # Load configuration
        config = self.load_config(config_path)
        
        # Generate SQL
        sql = self.generate_sql(config)
        
        # Save to file if output path provided
        if output_path:
            self.save_sql(sql, output_path)
        else:
            # Auto-generate output path
            config_name = Path(config_path).stem
            output_path = f"generated_{config_name}.sql"
            self.save_sql(sql, output_path)
        
        return sql


def main():
    """Main function to run the funnel generator."""
    parser = argparse.ArgumentParser(description='Generate funnel analysis SQL from configuration')
    parser.add_argument('config_path', help='Path to configuration JSON file')
    parser.add_argument('--output', '-o', help='Output SQL file path (optional)')
    parser.add_argument('--template', '-t', default='funnel_template.sql', 
                       help='Path to SQL template file (default: funnel_template.sql)')
    parser.add_argument('--execute', '-e', action='store_true', 
                       help='Execute the generated SQL (requires Snowflake connection)')
    parser.add_argument('--snowflake-user', help='Snowflake username')
    parser.add_argument('--snowflake-password', help='Snowflake password')
    parser.add_argument('--snowflake-account', help='Snowflake account')
    parser.add_argument('--snowflake-warehouse', help='Snowflake warehouse')
    
    args = parser.parse_args()
    
    try:
        # Initialize generator
        generator = FunnelGenerator(args.template)
        
        # Generate and save SQL
        sql = generator.generate_and_save(args.config_path, args.output)
        
        # Execute if requested
        if args.execute:
            if not all([args.snowflake_user, args.snowflake_password, args.snowflake_account]):
                print("Error: Snowflake connection parameters required for execution")
                return
            
            connection_params = {
                'user': args.snowflake_user,
                'password': args.snowflake_password,
                'account': args.snowflake_account,
                'warehouse': args.snowflake_warehouse
            }
            
            generator.execute_sql(sql, connection_params)
        
        print("Funnel generation completed successfully!")
        
    except Exception as e:
        print(f"Error: {str(e)}")
        return 1
    
    return 0


if __name__ == "__main__":
    exit(main()) 