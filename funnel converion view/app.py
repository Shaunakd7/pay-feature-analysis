#!/usr/bin/env python3
"""
Simple Funnel Tracker Web Application
Two screens: Input (credentials + config) and Results (table display)
"""

import os
import json
import tempfile
from typing import Dict, Any, Optional

from flask import Flask, request, render_template, jsonify, session, flash, redirect, url_for
from werkzeug.utils import secure_filename
from google.cloud import bigquery
from google.oauth2 import service_account
import jinja2
import plotly.express as px
import plotly.graph_objects as go
import plotly.utils
import pandas as pd

# Import our funnel generator
from funnel_generator import FunnelGenerator

app = Flask(__name__)
app.secret_key = os.environ.get('SECRET_KEY', 'dev-secret-key-change-in-production')

# Configuration
UPLOAD_FOLDER = 'uploads'
ALLOWED_EXTENSIONS = {'json'}
MAX_CONTENT_LENGTH = 16 * 1024 * 1024  # 16MB max file size

app.config['UPLOAD_FOLDER'] = UPLOAD_FOLDER
app.config['MAX_CONTENT_LENGTH'] = MAX_CONTENT_LENGTH

# Ensure upload directory exists
os.makedirs(UPLOAD_FOLDER, exist_ok=True)

def allowed_file(filename):
    """Check if file extension is allowed"""
    return '.' in filename and filename.rsplit('.', 1)[1].lower() in ALLOWED_EXTENSIONS

def validate_config(config: Dict[str, Any]) -> tuple[bool, str]:
    """Validate configuration file"""
    required_fields = [
        'funnel_name', 'project_id', 'dataset', 'data_source', 
        'user_base_table', 'time_period_days', 'steps', 'filters',
        'user_id_column', 'session_id_column', 'event_column', 
        'timestamp_column', 'partition_column', 'user_segmentation_fields',
        'max_step'
    ]
    
    for field in required_fields:
        if field not in config:
            return False, f"Missing required field: {field}"
    
    # Validate steps
    if not isinstance(config['steps'], list) or len(config['steps']) == 0:
        return False, "Steps must be a non-empty array"
    
    for i, step in enumerate(config['steps']):
        if 'rank' not in step or 'screen_name' not in step or 'loaded_events' not in step:
            return False, f"Step {i+1} missing required fields: rank, screen_name, or loaded_events"
    
    return True, "Configuration is valid"

def execute_funnel_analysis(config: Dict[str, Any], auth_method: str, credentials_json: str = None, project_id: str = None, dataset: str = None) -> tuple[bool, str, Optional[Dict]]:
    """Execute funnel analysis with provided credentials"""
    try:
        if auth_method == 'service_account':
            # Service Account Authentication
            if not credentials_json:
                return False, "Service account credentials are required", None
            
            # Create temporary credentials file
            with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
                f.write(credentials_json)
                temp_credentials_path = f.name
            
            # Set environment variable for BigQuery
            os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = temp_credentials_path
            
            # Use project_id from config
            project_id = config['project_id']
            
        elif auth_method == 'sso':
            # SSO Authentication - use default credentials
            # Clear any existing service account credentials
            if 'GOOGLE_APPLICATION_CREDENTIALS' in os.environ:
                del os.environ['GOOGLE_APPLICATION_CREDENTIALS']
            
            # Use provided project_id and dataset, or from config
            project_id = project_id or config['project_id']
            dataset = dataset or config['dataset']
            
            # Update config with provided values
            config['project_id'] = project_id
            config['dataset'] = dataset
        else:
            return False, "Invalid authentication method", None
        
        # Generate SQL
        generator = FunnelGenerator()
        sql = generator.generate_sql(config)
        print(f"DEBUG: Generated SQL: {sql}")
        
        # Execute in BigQuery
        client = bigquery.Client(project=project_id)
        
        # Split SQL by semicolon and execute each statement
        statements = [stmt.strip() for stmt in sql.split(';') if stmt.strip()]
        
        results = []
        for statement in statements:
            print(f"Executing: {statement[:100]}...")
            query_job = client.query(statement)
            query_job.result()  # Wait for completion
            results.append(f"Executed: {statement[:50]}...")
        
        # Query the results table to get data for display
        metrics_query = f"""
        SELECT * FROM `{project_id}.{dataset}.{config['funnel_name']}_metrics`
        ORDER BY analysis_level, step_stage
        """
        
        query_job = client.query(metrics_query)
        results_data = []
        for row in query_job:
            results_data.append(dict(row.items()))
        
        # Clean up temporary file if service account was used
        if auth_method == 'service_account' and 'temp_credentials_path' in locals():
            os.unlink(temp_credentials_path)
        
        return True, "Funnel analysis completed successfully!", {
            'sql': sql,
            'executed_statements': results,
            'tables_created': [
                f"{project_id}.{dataset}.{config['funnel_name']}_funnel",
                f"{project_id}.{dataset}.{config['funnel_name']}_metrics"
            ],
            'results_data': results_data
        }
        
    except Exception as e:
        # Clean up temporary file if it exists
        if auth_method == 'service_account' and 'temp_credentials_path' in locals():
            try:
                os.unlink(temp_credentials_path)
            except:
                pass
        
        return False, f"Error executing funnel analysis: {str(e)}", None

@app.route('/')
def index():
    """Screen 1: Input page with GCP credentials and config upload"""
    return render_template('input.html')

@app.route('/builder')
def builder():
    """Screen 1.5: Config builder form"""
    return render_template('builder.html')

@app.route('/create_config', methods=['POST'])
def create_config():
    """Handle config creation from form and execute funnel analysis"""
    try:
        # Get form data
        form_data = request.form
        
        # Build config from form data
        config = {
            'funnel_name': form_data.get('funnel_name', '').strip(),
            'project_id': form_data.get('project_id', '').strip(),
            'source_dataset': form_data.get('source_dataset', '').strip(),
            'destination_dataset': form_data.get('destination_dataset', '').strip(),
            'data_source': f"{form_data.get('project_id', '').strip()}.{form_data.get('source_dataset', '').strip()}.{form_data.get('table_name', '').strip()}",
            'user_id_column': form_data.get('user_id_column', '').strip(),
            'session_id_column': form_data.get('session_id_column', '').strip(),
            'event_column': form_data.get('event_column', '').strip(),
            'timestamp_column': form_data.get('timestamp_column', '').strip(),
            'partition_column': form_data.get('partition_column', '').strip(),
            'time_period_days': int(form_data.get('time_period_days', 30)),
            'user_base_table': form_data.get('user_base_table', '').strip() or None,
            'user_segmentation_fields': form_data.get('user_segmentation_fields', '').strip().split(',') if form_data.get('user_segmentation_fields', '').strip() else [],
            'app_version_extraction': form_data.get('app_version_extraction', '').strip() or None,
            'os_extraction': form_data.get('os_extraction', '').strip() or None,
            'entry_point_extraction': form_data.get('entry_point_extraction', '').strip() or None,
            'filters': form_data.get('filters', '').strip().split('\n') if form_data.get('filters', '').strip() else [],
            'steps': []
        }
        
        # Parse steps from form data
        step_count = int(form_data.get('step_count', 0))
        for i in range(1, step_count + 1):
            step = {
                'rank': i,
                'screen_name': form_data.get(f'step_{i}_screen_name', '').strip(),
                'loaded_events': form_data.get(f'step_{i}_events', '').strip().split(',')
            }
            # Clean up events (remove empty strings)
            step['loaded_events'] = [event.strip() for event in step['loaded_events'] if event.strip()]
            config['steps'].append(step)
        
        # Set max_step
        config['max_step'] = len(config['steps'])
        
        # Validate required fields
        required_fields = {
            'funnel_name': 'Funnel Name',
            'project_id': 'Project ID',
            'source_dataset': 'Source Dataset',
            'destination_dataset': 'Destination Dataset',
            'table_name': 'Table Name',
            'user_id_column': 'User ID Column',
            'session_id_column': 'Session ID Column',
            'event_column': 'Event Column',
            'timestamp_column': 'Timestamp Column'
        }
        
        for field, display_name in required_fields.items():
            field_value = form_data.get(field, '').strip()
            if not field_value:
                flash(f'{display_name} is required', 'error')
                return redirect(url_for('builder'))
        
        # Validate steps
        if not config['steps']:
            flash('At least one step is required', 'error')
            return redirect(url_for('builder'))
        
        for i, step in enumerate(config['steps']):
            if not step['screen_name']:
                flash(f'Step {i+1} screen name is required', 'error')
                return redirect(url_for('builder'))
            if not step['loaded_events']:
                flash(f'Step {i+1} must have at least one event', 'error')
                return redirect(url_for('builder'))
        
        # Determine authentication method
        auth_method = form_data.get('auth_method', 'sso')
        
        if auth_method == 'service_account':
            credentials_json = form_data.get('gcp_credentials', '').strip()
            if not credentials_json:
                flash('Service account credentials are required', 'error')
                return redirect(url_for('builder'))
            
            try:
                json.loads(credentials_json)
            except json.JSONDecodeError:
                flash('Invalid JSON format for service account credentials', 'error')
                return redirect(url_for('builder'))
        else:
            credentials_json = None
        
        # Execute funnel analysis
        print(f"DEBUG: Config being used: {json.dumps(config, indent=2)}")
        success, message, results = execute_funnel_analysis(
            config, 
            auth_method, 
            credentials_json, 
            config['project_id'], 
            config['destination_dataset']
        )
        
        if success:
            # Store minimal results info in session to avoid cookie size issues
            session['analysis_results'] = {
                'status': 'success',
                'tables_created': results.get('tables_created', []),
                'project_id': config['project_id'],
                'destination_dataset': config['destination_dataset'],
                'funnel_name': config['funnel_name']
            }
            session['config_used'] = config
            flash('Funnel analysis completed successfully!', 'success')
            return redirect(url_for('dashboard'))
        else:
            flash(f'Analysis failed: {message}', 'error')
            return redirect(url_for('builder'))
            
    except Exception as e:
        flash(f'Error creating configuration: {str(e)}', 'error')
        return redirect(url_for('builder'))

@app.route('/analyze', methods=['POST'])
def analyze():
    """Handle form submission and execute funnel analysis"""
    try:
        # Check if file was uploaded
        if 'config_file' not in request.files:
            flash('No file selected', 'error')
            return redirect(url_for('index'))
        
        file = request.files['config_file']
        if file.filename == '':
            flash('No file selected', 'error')
            return redirect(url_for('index'))
        
        if not allowed_file(file.filename):
            flash('Invalid file type. Please upload a JSON file.', 'error')
            return redirect(url_for('index'))
        
        # Read and parse config file
        try:
            config_content = file.read().decode('utf-8')
            config = json.loads(config_content)
        except json.JSONDecodeError as e:
            flash(f'Invalid JSON file: {str(e)}', 'error')
            return redirect(url_for('index'))
        
        # Validate configuration
        is_valid, validation_message = validate_config(config)
        if not is_valid:
            flash(f'Configuration validation failed: {validation_message}', 'error')
            return redirect(url_for('index'))
        
        # Determine authentication method
        auth_method = request.form.get('auth_method', 'sso')  # Default to SSO
        
        if auth_method == 'service_account':
            # Service Account Authentication
            credentials_json = request.form.get('gcp_credentials', '').strip()
            if not credentials_json:
                flash('GCP service account credentials are required', 'error')
                return redirect(url_for('index'))
            
            # Validate credentials JSON
            try:
                credentials_data = json.loads(credentials_json)
                if 'type' not in credentials_data or credentials_data['type'] != 'service_account':
                    flash('Invalid service account credentials format', 'error')
                    return redirect(url_for('index'))
            except json.JSONDecodeError:
                flash('Invalid JSON format for GCP credentials', 'error')
                return redirect(url_for('index'))
            
            # Execute funnel analysis with service account
            success, message, results = execute_funnel_analysis(
                config, 'service_account', credentials_json=credentials_json
            )
            
        elif auth_method == 'sso':
            # SSO Authentication
            project_id = request.form.get('project_id', '').strip()
            dataset = request.form.get('dataset', '').strip()
            
            if not project_id:
                flash('Project ID is required for SSO authentication', 'error')
                return redirect(url_for('index'))
            
            if not dataset:
                flash('Dataset is required for SSO authentication', 'error')
                return redirect(url_for('index'))
            
            # Execute funnel analysis with SSO
            success, message, results = execute_funnel_analysis(
                config, 'sso', project_id=project_id, dataset=dataset
            )
            
        else:
            flash('Invalid authentication method selected', 'error')
            return redirect(url_for('index'))
        
        if success:
            session['results'] = results
            session['config'] = config
            flash(message, 'success')
            return redirect(url_for('results'))
        else:
            flash(message, 'error')
            return redirect(url_for('index'))
            
    except Exception as e:
        flash(f'Unexpected error: {str(e)}', 'error')
        return redirect(url_for('index'))

@app.route('/results')
def results():
    """Screen 2: Results page with table display"""
    # Check for results from both create_config and analyze routes
    results_data = session.get('analysis_results') or session.get('results')
    config_data = session.get('config_used') or session.get('config')
    
    if not results_data:
        flash('No results available. Please run analysis first.', 'error')
        return redirect(url_for('index'))
    
    # If we have minimal session data, fetch from BigQuery
    if results_data.get('status') == 'success' and not results_data.get('results_data'):
        try:
            client = bigquery.Client(project=results_data['project_id'])
            
            # Fetch metrics data from BigQuery
            metrics_table = f"{results_data['project_id']}.{results_data['destination_dataset']}.{results_data['funnel_name']}_metrics"
            query = f"SELECT * FROM `{metrics_table}` ORDER BY analysis_level, step_stage"
            
            query_job = client.query(query)
            results_rows = [dict(row.items()) for row in query_job]
            
            # Add the fetched data to results_data
            results_data['results_data'] = results_rows
            
        except Exception as e:
            flash(f'Error fetching results from BigQuery: {str(e)}', 'error')
            return redirect(url_for('index'))
    
    return render_template('results.html', 
                         results=results_data, 
                         config=config_data)

@app.route('/dashboard')
def dashboard():
    """Looker Studio Dashboard Integration"""
    # Check for results from both create_config and analyze routes
    results_data = session.get('analysis_results') or session.get('results')
    config_data = session.get('config_used') or session.get('config')
    
    if not results_data:
        flash('No results available. Please run analysis first.', 'error')
        return redirect(url_for('index'))
    
    # Extract table information for Looker Studio
    tables_created = results_data.get('tables_created', [])
    
    # Get actual table schemas from BigQuery
    table_schemas = {}
    try:
        client = bigquery.Client(project=config_data['project_id'])
        
        for table_path in tables_created:
            # Parse table path: project.dataset.table
            parts = table_path.split('.')
            if len(parts) == 3:
                project, dataset, table = parts
                
                # Get table schema
                table_ref = client.dataset(dataset).table(table)
                table_obj = client.get_table(table_ref)
                
                # Extract column information
                columns = []
                for field in table_obj.schema:
                    columns.append({
                        'name': field.name,
                        'type': field.field_type,
                        'mode': field.mode,
                        'description': field.description or ''
                    })
                
                table_schemas[table] = {
                    'full_path': table_path,
                    'columns': columns,
                    'row_count': table_obj.num_rows
                }
    except Exception as e:
        print(f"Warning: Could not fetch table schemas: {e}")
        # Continue without schemas if there's an error
    
    return render_template('dashboard.html', 
                         results=results_data, 
                         config=config_data,
                         tables_created=tables_created,
                         table_schemas=table_schemas)

@app.route('/charts')
def charts():
    """Screen 3: Charts page for interactive funnel analysis"""
    results_data = session.get('analysis_results') or session.get('results')
    config_data = session.get('config_used') or session.get('config')
    
    if not results_data:
        flash('No results available. Please run analysis first.', 'error')
        return redirect(url_for('index'))
    
    # If we have minimal session data, fetch from BigQuery
    if results_data.get('status') == 'success' and not results_data.get('results_data'):
        try:
            client = bigquery.Client(project=results_data['project_id'])
            
            # Fetch metrics data from BigQuery
            metrics_table = f"{results_data['project_id']}.{results_data['destination_dataset']}.{results_data['funnel_name']}_metrics"
            query = f"SELECT * FROM `{metrics_table}` ORDER BY analysis_level, step_stage"
            
            query_job = client.query(query)
            results_rows = [dict(row.items()) for row in query_job]
            
            # Add the fetched data to results_data
            results_data['results_data'] = results_rows
            
        except Exception as e:
            flash(f'Error fetching results from BigQuery: {str(e)}', 'error')
            return redirect(url_for('index'))
    
    # Extract relevant data for charts
    funnel_data = results_data.get('results_data', [])
    
    if not funnel_data:
        flash('No funnel data available for chart generation.', 'error')
        return redirect(url_for('results'))
    
    # Convert to pandas DataFrame for easier manipulation
    df = pd.DataFrame(funnel_data)
    
    # Filter for stage-level data (exclude e2e)
    stage_data = df[df['analysis_level'] == 'stage'].copy()
    
    if stage_data.empty:
        flash('No stage-level data available for charts.', 'error')
        return redirect(url_for('results'))
    
    # Create funnel conversion chart
    funnel_fig = go.Figure()
    
    # Add funnel bars
    funnel_fig.add_trace(go.Bar(
        x=stage_data['screen_name'].tolist(),
        y=stage_data['starters_count'].tolist(),
        name='Starters',
        marker_color='#2563eb',
        text=stage_data['starters_count'].tolist(),
        textposition='auto'
    ))
    
    funnel_fig.add_trace(go.Bar(
        x=stage_data['screen_name'].tolist(),
        y=stage_data['completes_count'].tolist(),
        name='Completes',
        marker_color='#10b981',
        text=stage_data['completes_count'].tolist(),
        textposition='auto'
    ))
    
    funnel_fig.update_layout(
        title='Funnel Conversion Analysis',
        xaxis_title='Funnel Steps',
        yaxis_title='Number of Users',
        barmode='group',
        height=400
    )
    
    # Create drop-off chart
    dropoff_fig = go.Figure()
    
    dropoff_fig.add_trace(go.Scatter(
        x=stage_data['screen_name'].tolist(),
        y=stage_data['drop_off_rate'].tolist(),
        mode='lines+markers',
        name='Drop-off Rate',
        line=dict(color='#ef4444', width=3),
        marker=dict(size=8)
    ))
    
    dropoff_fig.update_layout(
        title='Drop-off Rate by Step',
        xaxis_title='Funnel Steps',
        yaxis_title='Drop-off Rate (%)',
        height=400
    )
    
    # Calculate summary metrics
    total_starters = stage_data['starters_count'].sum()
    total_completes = stage_data['completes_count'].sum()
    total_dropoffs = stage_data['drop_offs_count'].sum()
    overall_conversion_rate = round((total_completes / total_starters * 100) if total_starters > 0 else 0, 2)
    
    return render_template('charts.html',
                         funnel_chart_json=funnel_fig.to_json(),
                         dropoff_chart_json=dropoff_fig.to_json(),
                         config=config_data,
                         total_starters=total_starters,
                         total_completes=total_completes,
                         total_dropoffs=total_dropoffs,
                         overall_conversion_rate=overall_conversion_rate)

@app.errorhandler(413)
def too_large(e):
    """Handle file too large error"""
    flash('File too large. Maximum size is 16MB.', 'error')
    return redirect(url_for('index'))

@app.errorhandler(500)
def internal_error(e):
    """Handle internal server error"""
    flash('An internal error occurred. Please try again.', 'error')
    return redirect(url_for('index'))

if __name__ == '__main__':
    app.run(debug=True, host='0.0.0.0', port=int(os.environ.get('PORT', 5000))) 