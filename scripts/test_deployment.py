# scripts/test_deployment.py
"""
Deployment verification script.
Tests all system components are working correctly.
"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import requests
import pandas as pd
from src.db.engine import ENGINE

print("=" * 60)
print("DEPLOYMENT VERIFICATION")
print("=" * 60)
print()

# Test 1: Database
print("[1/4] Testing database connection...")
try:
    count = pd.read_sql("SELECT COUNT(*) FROM merton_outputs", ENGINE).iloc[0, 0]
    print(f"Database: {count} rows in merton_outputs")
except Exception as e:
    print(f"Database: {e}")
    sys.exit(1)

# Test 2: API
print("\n[2/4] Testing REST API...")
try:
    r = requests.get('http://localhost:5000/api/health', timeout=5)
    status = r.json().get('status', 'unknown')
    print(f"API: {status}")
except Exception as e:
    print(f"API: {e}")
    print("    (Make sure API is running: python api/merton_api.py)")

# Test 3: Streamlit
print("\n[3/4] Testing Streamlit...")
try:
    r = requests.get('http://localhost:8501', timeout=5)
    print(f"Streamlit: Running (status {r.status_code})")
except Exception as e:
    print(f"Streamlit: {e}")
    print("    (Make sure Streamlit is running: streamlit run streamlit_app/dashboard.py)")

# Test 4: Airflow
print("\n[4/4] Testing Airflow...")
try:
    r = requests.get('http://localhost:8080/health', timeout=5)
    print(f"Airflow: Running")
except Exception as e:
    print(f"Airflow: {e}")
    print("    (Make sure Airflow is running: airflow webserver)")

print("\n" + "=" * 60)
print("VERIFICATION COMPLETE")
print("=" * 60)
