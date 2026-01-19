# launch.ps1 - Windows launcher for Merton PD System

param(
    [switch]$SkipInstall = $false
)

$ErrorActionPreference = "Stop"

# Colors
function Write-ColorOutput($ForegroundColor) {
    $fc = $host.UI.RawUI.ForegroundColor
    $host.UI.RawUI.ForegroundColor = $ForegroundColor
    if ($args) {
        Write-Output $args
    }
    $host.UI.RawUI.ForegroundColor = $fc
}

Write-ColorOutput Cyan "============================================================"
Write-ColorOutput Cyan "  MERTON CREDIT RISK SYSTEM - LAUNCHER"
Write-ColorOutput Cyan "============================================================"
Write-Output ""

# Get project root
$PROJECT_ROOT = $PSScriptRoot
Set-Location $PROJECT_ROOT

# ============================================================
# CHECK DEPENDENCIES
# ============================================================

Write-ColorOutput Yellow "[1/6] Checking dependencies..."

# Check Python
try {
    $pythonVersion = python --version 2>&1
    Write-ColorOutput Green "[OK] $pythonVersion found"
} catch {
    Write-ColorOutput Red "[ERROR] Python not found. Please install Python 3.8+"
    exit 1
}

# Check PostgreSQL
try {
    $psqlVersion = psql --version 2>&1
    Write-ColorOutput Green "[OK] PostgreSQL found"
} catch {
    Write-ColorOutput Yellow "[WARNING] psql not found. Assuming PostgreSQL is running..."
}

# Check/create virtual environment
if (-not (Test-Path ".venv")) {
    Write-ColorOutput Yellow "[INFO] Creating virtual environment..."
    python -m venv .venv
}

# Activate virtual environment
& .\.venv\Scripts\Activate.ps1

# Install dependencies
if (-not $SkipInstall) {
    Write-ColorOutput Yellow "[INFO] Installing dependencies..."
    python -m pip install --upgrade pip --quiet
    pip install -r requirements.txt --quiet
}

Write-ColorOutput Green "[OK] Dependencies ready"
Write-Output ""

# ============================================================
# CHECK DATABASE CONNECTION
# ============================================================

Write-ColorOutput Yellow "[2/6] Checking database connection..."

$dbCheck = python -c @"
import sys
try:
    from src.db.engine import ENGINE
    import pandas as pd
    pd.read_sql('SELECT 1', ENGINE)
    print('[OK] Database connected')
except Exception as e:
    print(f'[ERROR] Database connection failed: {e}')
    sys.exit(1)
"@

if ($LASTEXITCODE -ne 0) {
    Write-ColorOutput Red $dbCheck
    exit 1
}

Write-ColorOutput Green $dbCheck
Write-Output ""

# ============================================================
# INITIALIZE AIRFLOW
# ============================================================

Write-ColorOutput Yellow "[3/6] Initializing Airflow..."

$env:AIRFLOW_HOME = "$PROJECT_ROOT\airflow"

# Create airflow directory
New-Item -ItemType Directory -Force -Path "$env:AIRFLOW_HOME\dags" | Out-Null

# Initialize Airflow DB if needed
if (-not (Test-Path "$env:AIRFLOW_HOME\airflow.db")) {
    Write-ColorOutput Yellow "[INFO] Initializing Airflow database..."
    airflow db init
    
    # Create admin user
    $createUser = @"
from airflow import settings
from airflow.models import User
import sys

session = settings.Session()
user = session.query(User).filter(User.username == 'admin').first()

if not user:
    user = User(
        username='admin',
        email='admin@example.com',
        is_active=True,
        is_superuser=True
    )
    user.password = 'admin'
    session.add(user)
    session.commit()
    print('[INFO] Admin user created')
else:
    print('[INFO] Admin user already exists')
session.close()
"@
    
    python -c $createUser
    
    Write-ColorOutput Green "[OK] Airflow initialized"
} else {
    Write-ColorOutput Green "[OK] Airflow already initialized"
}

Write-Output ""

# ============================================================
# CREATE DIRECTORIES
# ============================================================

Write-ColorOutput Yellow "[4/6] Setting up directories..."

New-Item -ItemType Directory -Force -Path "results" | Out-Null
New-Item -ItemType Directory -Force -Path "logs" | Out-Null

Write-ColorOutput Green "[OK] Directories ready"
Write-Output ""

# ============================================================
# LAUNCH SERVICES
# ============================================================

Write-ColorOutput Yellow "[5/6] Launching services..."
Write-Output ""

# Function to start a process and save it
$jobs = @()

function Start-BackgroundJob($Name, $Command) {
    Write-ColorOutput Cyan "[LAUNCHING] $Name"
    
    $job = Start-Job -ScriptBlock {
        param($cmd, $env_airflow_home)
        $env:AIRFLOW_HOME = $env_airflow_home
        Set-Location $using:PROJECT_ROOT
        & .\.venv\Scripts\Activate.ps1
        Invoke-Expression $cmd
    } -ArgumentList $Command, $env:AIRFLOW_HOME
    
    $script:jobs += $job
    return $job
}

# Launch Airflow Scheduler
$schedulerJob = Start-BackgroundJob "Airflow Scheduler" "airflow scheduler > logs\airflow_scheduler.log 2>&1"
Start-Sleep -Seconds 5

# Launch Airflow Webserver
$webserverJob = Start-BackgroundJob "Airflow Webserver (port 8080)" "airflow webserver --port 8080 > logs\airflow_webserver.log 2>&1"
Start-Sleep -Seconds 8

# Launch REST API
$apiJob = Start-BackgroundJob "REST API (port 5000)" "python api\merton_api.py > logs\api.log 2>&1"
Start-Sleep -Seconds 3

# Launch Streamlit
Write-ColorOutput Cyan "[LAUNCHING] Streamlit Dashboard (port 8501)"
Start-Process powershell -ArgumentList "-NoExit", "-Command", "& { Set-Location '$PROJECT_ROOT'; .\.venv\Scripts\Activate.ps1; streamlit run streamlit_app\dashboard.py }" -WindowStyle Normal

Start-Sleep -Seconds 5

Write-Output ""

# ============================================================
# CHECK SERVICE STATUS
# ============================================================

Write-ColorOutput Yellow "[6/6] Checking service status..."
Write-Output ""

function Test-Port($Port) {
    $connection = Test-NetConnection -ComputerName localhost -Port $Port -WarningAction SilentlyContinue
    return $connection.TcpTestSucceeded
}

function Check-Service($Name, $Port) {
    if (Test-Port $Port) {
        Write-ColorOutput Green "[OK] $Name running on port $Port"
        return $true
    } else {
        Write-ColorOutput Red "[ERROR] $Name failed to start on port $Port"
        return $false
    }
}

$allRunning = $true
$allRunning = (Check-Service "Airflow Webserver" 8080) -and $allRunning
$allRunning = (Check-Service "REST API" 5000) -and $allRunning
$allRunning = (Check-Service "Streamlit Dashboard" 8501) -and $allRunning

Write-Output ""

# ============================================================
# SUCCESS MESSAGE
# ============================================================

if ($allRunning) {
    Write-ColorOutput Green "============================================================"
    Write-ColorOutput Green "  MERTON CREDIT RISK SYSTEM - READY!"
    Write-ColorOutput Green "============================================================"
    Write-Output ""
    Write-ColorOutput Cyan "Access your dashboards:"
    Write-Output ""
    Write-ColorOutput Yellow "  Airflow UI:     " -NoNewline
    Write-Output "http://localhost:8080"
    Write-Output "                  Username: admin"
    Write-Output "                  Password: admin"
    Write-Output ""
    Write-ColorOutput Yellow "  Streamlit:      " -NoNewline
    Write-Output "http://localhost:8501"
    Write-Output ""
    Write-ColorOutput Yellow "  REST API:       " -NoNewline
    Write-Output "http://localhost:5000/api/health"
    Write-Output ""
    Write-ColorOutput Cyan "Logs:"
    Write-Output "  Airflow Scheduler: logs\airflow_scheduler.log"
    Write-Output "  Airflow Webserver: logs\airflow_webserver.log"
    Write-Output "  REST API:          logs\api.log"
    Write-Output ""
    Write-ColorOutput Yellow "Press Ctrl+C to stop background services"
    Write-ColorOutput Green "============================================================"
    Write-Output ""
} else {
    Write-ColorOutput Red "Some services failed to start. Check logs for details."
}

# Cleanup function
function Cleanup {
    Write-Output ""
    Write-ColorOutput Yellow "[INFO] Shutting down services..."
    
    Get-Job | Stop-Job
    Get-Job | Remove-Job
    
    # Kill processes on ports
    $ports = @(8080, 5000, 8501)
    foreach ($port in $ports) {
        $processes = Get-NetTCPConnection -LocalPort $port -ErrorAction SilentlyContinue | 
                     Select-Object -ExpandProperty OwningProcess -Unique
        foreach ($proc in $processes) {
            Stop-Process -Id $proc -Force -ErrorAction SilentlyContinue
        }
    }
    
    Write-ColorOutput Green "[OK] All services stopped"
}

# Wait for user interrupt
try {
    Write-Output "Services are running. Press Ctrl+C to stop..."
    while ($true) {
        Start-Sleep -Seconds 1
        
        # Check if background jobs are still running
        $runningJobs = Get-Job | Where-Object { $_.State -eq 'Running' }
        if ($runningJobs.Count -eq 0) {
            Write-ColorOutput Yellow "[WARNING] Background services stopped unexpectedly"
            break
        }
    }
} finally {
    Cleanup
}