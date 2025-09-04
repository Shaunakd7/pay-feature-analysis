#!/usr/bin/env python3
"""
Debug script to check if BalanceUpdateEvent exists in events data
"""

from google.cloud import bigquery
import os

def check_balance_update_event():
    """Check if BalanceUpdateEvent exists in the events data"""
    
    # Use default credentials (SSO)
    if 'GOOGLE_APPLICATION_CREDENTIALS' in os.environ:
        del os.environ['GOOGLE_APPLICATION_CREDENTIALS']
    
    client = bigquery.Client(project="common-tech-434709")
    
    # Check if BalanceUpdateEvent exists
    query = """
    SELECT 
        event,
        COUNT(*) as count,
        MIN(date) as min_date,
        MAX(date) as max_date
    FROM `common-tech-434709.events.events`
    WHERE event = 'BalanceUpdateEvent'
    AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 30 DAY)
    GROUP BY event
    """
    
    print("🔍 Checking for BalanceUpdateEvent...")
    query_job = client.query(query)
    results = list(query_job)
    
    if results:
        for row in results:
            print(f"✅ Found {row.event}: {row.count:,} occurrences")
            print(f"📅 Date range: {row.min_date} to {row.max_date}")
    else:
        print("❌ BalanceUpdateEvent not found in last 30 days")
    
    # Check for similar events
    print("\n🔍 Checking for similar payment/balance events...")
    similar_query = """
    SELECT 
        event,
        COUNT(*) as count
    FROM `common-tech-434709.events.events`
    WHERE LOWER(event) LIKE '%balance%' 
       OR LOWER(event) LIKE '%payment%'
       OR LOWER(event) LIKE '%success%'
       OR LOWER(event) LIKE '%update%'
    AND date >= DATE_SUB(CURRENT_DATE(), INTERVAL 7 DAY)
    GROUP BY event
    ORDER BY count DESC
    LIMIT 10
    """
    
    similar_job = client.query(similar_query)
    similar_results = list(similar_job)
    
    if similar_results:
        print("📋 Similar events found:")
        for row in similar_results:
            print(f"   {row.event}: {row.count:,}")
    else:
        print("❌ No similar events found")

if __name__ == "__main__":
    check_balance_update_event() 