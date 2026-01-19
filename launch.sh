#!/bin/bash
# launch.sh - One-click launcher for Merton PD System

set -e  # Exit on error

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Project root
PROJECT_ROOT="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
cd "$PROJECT_ROOT"

echo -e "${BLUE}============================================================${NC}"
echo -e "${BLUE}  MERTON CREDIT RISK SYSTEM - LAUNCHER${NC}"
echo -e "${BLUE}============================================================${NC}"
echo ""

# ============================================================
# CHECK DEPENDENCIES
# ============================================================

echo -e "${YELLOW}[1/6] Checking dependencies...${NC}"

# Check Python version
if ! command -v python3 &> /dev/null; then
    echo -e "${RED}[ERROR] Python 3 not found. Please install Python 3.8+${NC}"
    exit 1
fi

PYTHON_VERSION=$(python3 -c 'import sys; print(".".join(map(str, sys.version_info[:2])))')
echo -e "${GREEN}[OK] Python ${PYTHON_VERSION} found${NC}"

# Check PostgreSQL
if ! command -v psql &> /dev/null; then
    echo -e "${YELLOW}[WARNING] psql not found. Assuming PostgreSQL is running...${NC}"
else
    echo -e "${GREEN}[OK] PostgreSQL found${NC}"
fi

# Check if virtual environment exists
if [ ! -d ".venv" ]; then
    echo -e "${YELLOW}[INFO] Creating virtual environment...${NC}"
    python3 -m venv .venv
fi

# Activate virtual environment
source .venv/bin/activate || source .venv/Scripts/activate 2>/dev/null

# Install/upgrade dependencies
echo -e "${YELLOW}[INFO] Installing dependencies...${NC}"
pip install -q --upgrade pip
pip install -q -r requirements.txt

echo -e "${GREEN}[OK] Dependencies ready${NC}"
echo ""

# ============================================================
# CHECK DATABASE CONNECTION
# ============================================================

echo -e "${YELLOW}[2/6] Checking database connection...${NC}"

python3 - <<EOF
import sys
try:
    from src.db.engine import ENGINE
    import pandas as pd
    pd.read_sql("SELECT 1", ENGINE)
    print("${GREEN}[OK] Database connected${NC}")
except Exception as e:
    print("${RED}[ERROR] Database connection failed: ${e}${NC}")
    sys.exit(1)
EOF

echo ""

# ============================================================
# INITIALIZE AIRFLOW
# ============================================================

echo -e "${YELLOW}[3/6] Initializing Airflow...${NC}"

export AIRFLOW_HOME="$PROJECT_ROOT/airflow"

# Create airflow directory if it doesn't exist
mkdir -p "$AIRFLOW_HOME/dags"

# Check if Airflow DB is initialized
if [ ! -f "$AIRFLOW_HOME/airflow.db" ]; then
    echo -e "${YELLOW}[INFO] Initializing Airflow database...${NC}"
    airflow db init
    
    # Create admin user
    airflow users create \
        --username admin \
        --firstname Admin \
        --lastname User \
        --role Admin \
        --email admin@example.com \
        --password admin 2>/dev/null || echo -e "${YELLOW}[INFO] Admin user already exists${NC}"
    
    echo -e "${GREEN}[OK] Airflow initialized${NC}"
else
    echo -e "${GREEN}[OK] Airflow already initialized${NC}"
fi

echo ""

# ============================================================
# CREATE RESULTS DIRECTORY
# ============================================================

echo -e "${YELLOW}[4/6] Setting up directories...${NC}"

mkdir -p results
mkdir -p logs

echo -e "${GREEN}[OK] Directories ready${NC}"
echo ""

# ============================================================
# LAUNCH SERVICES
# ============================================================

echo -e "${YELLOW}[5/6] Launching services...${NC}"
echo ""

# Function to kill processes on exit
cleanup() {
    echo ""
    echo -e "${YELLOW}[INFO] Shutting down services...${NC}"
    kill $(jobs -p) 2>/dev/null || true
    echo -e "${GREEN}[OK] All services stopped${NC}"
    exit
}

trap cleanup SIGINT SIGTERM

# Launch Airflow Scheduler
echo -e "${BLUE}[LAUNCHING] Airflow Scheduler (background)${NC}"
airflow scheduler > logs/airflow_scheduler.log 2>&1 &
SCHEDULER_PID=$!

# Wait for scheduler to start
sleep 3

# Launch Airflow Webserver
echo -e "${BLUE}[LAUNCHING] Airflow Webserver (port 8080)${NC}"
airflow webserver --port 8080 > logs/airflow_webserver.log 2>&1 &
WEBSERVER_PID=$!

# Wait for webserver to start
sleep 5

# Launch REST API
echo -e "${BLUE}[LAUNCHING] REST API (port 5000)${NC}"
python3 api/merton_api.py > logs/api.log 2>&1 &
API_PID=$!

# Wait for API to start
sleep 2

# Launch Streamlit Dashboard
echo -e "${BLUE}[LAUNCHING] Streamlit Dashboard (port 8501)${NC}"
streamlit run streamlit_app/dashboard.py > logs/streamlit.log 2>&1 &
STREAMLIT_PID=$!

# Wait for Streamlit to start
sleep 3

echo ""

# ============================================================
# SYSTEM STATUS
# ============================================================

echo -e "${YELLOW}[6/6] Checking service status...${NC}"
echo ""

# Check if services are running
check_service() {
    local name=$1
    local port=$2
    
    if lsof -Pi :$port -sTCP:LISTEN -t >/dev/null 2>&1 || netstat -an 2>/dev/null | grep -q ":$port.*LISTEN"; then
        echo -e "${GREEN}[OK] ${name} running on port ${port}${NC}"
        return 0
    else
        echo -e "${RED}[ERROR] ${name} failed to start on port ${port}${NC}"
        return 1
    fi
}

check_service "Airflow Webserver" 8080
check_service "REST API" 5000
check_service "Streamlit Dashboard" 8501

echo ""

# ============================================================
# SUCCESS MESSAGE
# ============================================================

echo -e "${GREEN}============================================================${NC}"
echo -e "${GREEN}  MERTON CREDIT RISK SYSTEM - READY!${NC}"
echo -e "${GREEN}============================================================${NC}"
echo ""
echo -e "${BLUE}Access your dashboards:${NC}"
echo ""
echo -e "  ${YELLOW}Airflow UI:${NC}     http://localhost:8080"
echo -e "                  ${YELLOW}Username:${NC} admin"
echo -e "                  ${YELLOW}Password:${NC} admin"
echo ""
echo -e "  ${YELLOW}Streamlit:${NC}      http://localhost:8501"
echo ""
echo -e "  ${YELLOW}REST API:${NC}       http://localhost:5000/api/health"
echo ""
echo -e "${BLUE}Logs:${NC}"
echo -e "  Airflow Scheduler: logs/airflow_scheduler.log"
echo -e "  Airflow Webserver: logs/airflow_webserver.log"
echo -e "  REST API:          logs/api.log"
echo -e "  Streamlit:         logs/streamlit.log"
echo ""
echo -e "${YELLOW}Press Ctrl+C to stop all services${NC}"
echo -e "${GREEN}============================================================${NC}"
echo ""

# Wait for user interrupt
wait