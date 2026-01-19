# Deployment Checklist - Merton Credit Risk System

## ✅ Pre-Installation Checklist

- [ ] Python 3.8+ installed
- [ ] PostgreSQL installed and running
- [ ] Database `merton_db` created with user `merton_user`
- [ ] Git repository cloned/downloaded
- [ ] Terminal/PowerShell access

---

## 📦 File Placement Checklist

### New Files to Create

Copy these artifacts to your project:

#### **1. API Layer**
- [ ] Create `api/` directory
- [ ] Copy `api/merton_api.py` from artifact "REST API"

#### **2. Streamlit Dashboard**
- [ ] Create `streamlit_app/` directory
- [ ] Copy `streamlit_app/dashboard.py` from artifact "Interactive Dashboard"

#### **3. Airflow DAGs**
- [ ] Create `airflow/dags/` directory
- [ ] Copy `airflow/dags/daily_merton_pipeline.py` from artifact "Daily Airflow DAG"
- [ ] Copy `airflow/dags/weekly_advanced_analysis.py` from artifact "Weekly Analysis DAG"

#### **4. Launch Scripts**
- [ ] Copy `launch.sh` from artifact "One-Click Launcher" to project root
- [ ] Copy `launch.ps1` from artifact "Windows PowerShell Launcher" to project root
- [ ] Make executable: `chmod +x launch.sh` (Linux/Mac)

#### **5. Documentation**
- [ ] Copy `AIRFLOW_SETUP.md` from artifact "Complete Setup Guide"
- [ ] Copy `README.md` from artifact "Complete System Documentation"
- [ ] Copy `DEPLOYMENT_CHECKLIST.md` from this artifact

#### **6. Dependencies**
- [ ] Update `requirements.txt` with artifact "Complete Dependencies"

### File Structure Verification

After copying, your structure should look like:

```
Merton_PD/
├── api/
│   └── merton_api.py                    ✅ NEW
├── airflow/
│   └── dags/
│       ├── daily_merton_pipeline.py     ✅ NEW
│       └── weekly_advanced_analysis.py  ✅ NEW
├── streamlit_app/
│   └── dashboard.py                     ✅ NEW
├── src/
│   ├── merton/
│   │   ├── solver.py                    ✅ EXISTING
│   │   ├── pipeline.py                  ✅ EXISTING
│   │   ├── bootstrap.py                 ✅ EXISTING
│   │   ├── sensitivity.py               ✅ EXISTING
│   │   └── stress_testing.py            ✅ EXISTING
│   └── db/
│       └── engine.py                    ✅ EXISTING
├── scripts/
│   └── run_merton_model.py              ✅ EXISTING
├── config/
│   └── merton.yaml                      ✅ EXISTING
├── launch.sh                            ✅ NEW
├── launch.ps1                           ✅ NEW
├── requirements.txt                     ✅ UPDATED
├── README.md                            ✅ NEW
├── AIRFLOW_SETUP.md                     ✅ NEW
└── DEPLOYMENT_CHECKLIST.md              ✅ NEW
```

---

## 🔧 Installation Steps

### Step 1: Install Dependencies (5 minutes)

```bash
# Windows PowerShell
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install -r requirements.txt

# Linux/Mac
python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

**Checklist:**
- [ ] Virtual environment created
- [ ] All packages installed without errors
- [ ] Test: `python -c "import airflow; print(airflow.__version__)"`

### Step 2: Initialize Airflow (3 minutes)

```bash
# Set Airflow home
export AIRFLOW_HOME=$(pwd)/airflow  # Linux/Mac
$env:AIRFLOW_HOME = "$PWD\airflow"  # Windows

# Initialize database
airflow db init

# Create admin user
airflow users create \
    --username admin \
    --firstname Admin \
    --lastname User \
    --role Admin \
    --email admin@example.com \
    --password admin
```

**Checklist:**
- [ ] `airflow/airflow.db` file created
- [ ] Admin user created successfully
- [ ] No errors in output

### Step 3: Verify Database Connection (1 minute)

```bash
python -c "from src.db.engine import ENGINE; import pandas as pd; pd.read_sql('SELECT 1', ENGINE); print('Database connected!')"
```

**Checklist:**
- [ ] Output shows "Database connected!"
- [ ] No connection errors

### Step 4: Test Individual Components (5 minutes)

```bash
# Test API
python api/merton_api.py &
curl http://localhost:5000/api/health
# Should return: {"status": "healthy", ...}
kill %1  # Stop API

# Test Streamlit (will open browser)
streamlit run streamlit_app/dashboard.py
# Press Ctrl+C to stop

# Test Airflow
airflow dags list | grep merton
# Should show: daily_merton_pipeline, weekly_advanced_analysis
```

**Checklist:**
- [ ] API responds with "healthy" status
- [ ] Streamlit dashboard loads without errors
- [ ] Both Airflow DAGs appear in list

### Step 5: Launch Full System (1 minute)

```bash
# Windows
.\launch.ps1

# Linux/Mac
./launch.sh
```

**Checklist:**
- [ ] All 4 services start (Airflow scheduler, webserver, API, Streamlit)
- [ ] No errors in startup logs
- [ ] Ports 8080, 5000, 8501 are listening

---

## 🧪 Verification Steps

### Test 1: Airflow UI (2 minutes)

1. Open http://localhost:8080
2. Login with admin/admin
3. Check DAGs page

**Checklist:**
- [ ] Can access Airflow UI
- [ ] See `daily_merton_pipeline` DAG
- [ ] See `weekly_advanced_analysis` DAG
- [ ] Both DAGs show as "active" (toggle should be ON)

### Test 2: Streamlit Dashboard (2 minutes)

1. Open http://localhost:8501
2. Navigate through pages
3. Select a company (e.g., AAPL)

**Checklist:**
- [ ] Dashboard loads
- [ ] Can see Home page with portfolio overview
- [ ] Can search company in "Company Analysis"
- [ ] Charts render correctly
- [ ] Data appears in tables

### Test 3: REST API (2 minutes)

```bash
# Test health endpoint
curl http://localhost:5000/api/health

# Test get latest PD
curl http://localhost:5000/api/pd/AAPL

# Test universe
curl http://localhost:5000/api/universe
```

**Checklist:**
- [ ] Health endpoint returns "healthy"
- [ ] PD endpoint returns data for AAPL
- [ ] Universe endpoint lists all tickers

### Test 4: Run Pipeline Manually (5 minutes)

In Airflow UI:
1. Click on `daily_merton_pipeline`
2. Click the ▶️ (play) button
3. Watch tasks execute

**Checklist:**
- [ ] All tasks turn green (success)
- [ ] No red tasks (failures)
- [ ] Check database: `SELECT COUNT(*) FROM merton_outputs;`
- [ ] Count > 0 (data was inserted)

---

## 🎯 Post-Deployment Verification

### Quick Smoke Test (5 minutes)

Run this script to verify everything:

```python
# test_deployment.py
import requests
import pandas as pd
from src.db.engine import ENGINE

print("=" * 60)
print("DEPLOYMENT VERIFICATION")
print("=" * 60)

# Test 1: Database
try:
    count = pd.read_sql("SELECT COUNT(*) FROM merton_outputs", ENGINE).iloc[0, 0]
    print(f"✅ Database: {count} rows in merton_outputs")
except Exception as e:
    print(f"❌ Database: {e}")

# Test 2: API
try:
    r = requests.get('http://localhost:5000/api/health', timeout=5)
    print(f"✅ API: {r.json()['status']}")
except Exception as e:
    print(f"❌ API: {e}")

# Test 3: Streamlit (just check port)
try:
    r = requests.get('http://localhost:8501', timeout=5)
    print(f"✅ Streamlit: Running (status {r.status_code})")
except Exception as e:
    print(f"❌ Streamlit: {e}")

# Test 4: Airflow
try:
    r = requests.get('http://localhost:8080/health', timeout=5)
    print(f"✅ Airflow: Running")
except Exception as e:
    print(f"❌ Airflow: {e}")

print("=" * 60)
```

Run with:
```bash
python test_deployment.py
```

**Expected Output:**
```
============================================================
DEPLOYMENT VERIFICATION
============================================================
✅ Database: 1455 rows in merton_outputs
✅ API: healthy
✅ Streamlit: Running (status 200)
✅ Airflow: Running
============================================================
```

---

## 🐛 Common Issues & Fixes

### Issue: "Port already in use"

**Error**: `Address already in use: Port 8080`

**Fix**:
```bash
# Windows
Get-Process -Id (Get-NetTCPConnection -LocalPort 8080).OwningProcess | Stop-Process

# Linux/Mac
lsof -ti:8080 | xargs kill -9
```

### Issue: "Cannot connect to database"

**Error**: `FATAL: password authentication failed`

**Fix**:
1. Check `src/db/engine.py` has correct credentials
2. Verify PostgreSQL is running: `pg_isready`
3. Test connection: `psql -U merton_user -d merton_db`

### Issue: "Module not found: airflow"

**Error**: `ModuleNotFoundError: No module named 'airflow'`

**Fix**:
```bash
# Ensure virtual environment is activated
source .venv/bin/activate  # Linux/Mac
.\.venv\Scripts\Activate.ps1  # Windows

# Reinstall
pip install apache-airflow==2.8.0
```

### Issue: "DAGs not showing in Airflow"

**Fix**:
```bash
# Check for import errors
airflow dags list-import-errors

# Verify dags folder path
echo $AIRFLOW_HOME/dags

# Refresh
airflow dags list
```

### Issue: "Streamlit shows empty dashboard"

**Fix**:
1. Check if data exists: `psql -U merton_user -d merton_db -c "SELECT COUNT(*) FROM merton_outputs;"`
2. If 0, run: `python scripts/run_merton_model.py AAPL`
3. Refresh Streamlit page

---

## 📊 Expected Timeline

| Phase | Duration | Tasks |
|-------|----------|-------|
| **File Setup** | 10 min | Copy all artifacts, verify structure |
| **Installation** | 10 min | Install dependencies, init Airflow |
| **Testing** | 10 min | Test each component individually |
| **First Run** | 5 min | Launch system, verify all services |
| **Verification** | 5 min | Run smoke tests, check data |
| **Total** | **40 min** | Complete deployment from scratch |

---

## ✅ Final Checklist

Before considering deployment complete:

### Services Running
- [ ] Airflow Scheduler (background process)
- [ ] Airflow Webserver (http://localhost:8080)
- [ ] REST API (http://localhost:5000)
- [ ] Streamlit Dashboard (http://localhost:8501)

### Data Verification
- [ ] Can see companies in Streamlit Home page
- [ ] Can view DD/PD trends for AAPL
- [ ] API returns data for `/api/pd/AAPL`
- [ ] Database has > 0 rows in `merton_outputs`

### Airflow Verification
- [ ] Both DAGs visible and active
- [ ] Can trigger `daily_merton_pipeline` manually
- [ ] Tasks execute successfully (all green)
- [ ] New data appears in database after run

### API Verification
- [ ] `/api/health` returns "healthy"
- [ ] `/api/pd/AAPL` returns current DD/PD
- [ ] `/api/signals` returns trading signals
- [ ] All endpoints respond < 1 second

### Dashboard Verification
- [ ] All 5 pages load without errors
- [ ] Charts render correctly
- [ ] Can search different companies
- [ ] Trading signals page shows opportunities

---

## 🎉 Success Criteria

You've successfully deployed when:

1. ✅ All services start with `./launch.sh` or `.\launch.ps1`
2. ✅ Airflow UI shows both DAGs active
3. ✅ Streamlit dashboard displays company data
4. ✅ API responds to all endpoints
5. ✅ Can manually trigger pipeline and see results
6. ✅ Trading signals appear based on DD changes

---

## 📞 What to Do If Stuck

1. **Check logs** in `logs/` directory
2. **Review** the relevant troubleshooting section above
3. **Test** individual components to isolate the issue
4. **Verify** database connection and data availability
5. **Restart** all services with the launcher script

---

## 🚀 You're Ready!

Once all checkboxes are ticked, your Merton Credit Risk System is fully operational!

**Next steps:**
- Add more companies to track
- Customize signal thresholds
- Set up email alerts
- Deploy to production

Happy analyzing! 📊