"""
Export weather data from PostgreSQL to CSV
Run this script after data collection to generate weather_data.csv
"""

import psycopg2
import csv
from datetime import datetime

# ==============================
# DATABASE CONFIGURATION
# ==============================

DB_CONFIG = {
    'dbname': 'weather_db',
    'user': 'postgres',
    'password': 'password',  # Güvenlik için .env önerilir
    'host': 'localhost',
    'port': '5432'
}

# ==============================
# EXPORT FUNCTION
# ==============================

def export_to_csv(filename='weather_data.csv'):
    """
    Exports all records from weather_data table to a CSV file.
    Orders by recorded_at timestamp.
    """
    conn = None
    cursor = None
    
    try:
        print("Connecting to database...")
        conn = psycopg2.connect(**DB_CONFIG)
        cursor = conn.cursor()
        
        print("Fetching data from weather_data table...")
        cursor.execute("SELECT * FROM weather_data ORDER BY recorded_at")
        rows = cursor.fetchall()
        
        # Get column names automatically from cursor description
        column_names = [desc[0] for desc in cursor.description]
        
        print(f"Exporting {len(rows)} records to '{filename}'...")
        
        with open(filename, 'w', newline='', encoding='utf-8') as f:
            writer = csv.writer(f)
            writer.writerow(column_names)    # Header row
            writer.writerows(rows)           # Data rows
        
        print(f"Export completed successfully!")
        print(f"   File: {filename}")
        print(f"   Total records: {len(rows)}")
        print(f"   Exported at: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
    except psycopg2.Error as e:
        print(f"Database error: {e}")
    except Exception as e:
        print(f"Unexpected error: {e}")
    finally:
        if cursor:
            cursor.close()
        if conn:
            conn.close()
        print("Database connection closed.")

# ==============================
# MAIN
# ==============================

if __name__ == "__main__":
    export_to_csv()