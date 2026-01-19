# Airflow + Streamlit Setup Guide

## 🎯 Complete System Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                   AIRFLOW ORCHESTRATION                      │
├─────────────────────────────────────────────────────────────┤
│  Daily DAG (6 PM weekdays):                                 │
│    1. Fetch equity prices                                   │
│    2. Update balance sheets                                 │
│    3. Calculate volatility                                  │
│    4. Build Merton inputs                                   │
│    5. Run Merton solver                                     │
│    6. Generate CDS signals                                  │
│                                                              │
│  Weekly DAG (8 AM Sundays):                                 │
│    1. Bootstrap analysis                                    │
│    2. Sensitivity analysis                                  │
│    3. Stress testing                                        │
│    4. Generate reports                                      │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                  PostgreSQL DATABASE                         │
│  - merton_outputs (PD/DD history)                           │
│  - merton_inputs (daily panel)                              │
│  - equity_prices_raw, balance_sheet_normalized, etc.        │
└──────────────────────┬──────────────────────────────────────┘
                       │
        ┌──────────────┴──────────────┐
        ▼                              ▼
┌──────────────────┐          ┌──────────────────┐
│  STREAMLIT UI    │          │   REST API       │
│  (Port 8501)     │          │   (Port 5000)    │
│                  │          │                  │
│ - Search company │          │ - External       │
│ - View trends    │          │   projects       │
│ - CDS signals    │          │ - Integration    │
│ - Stress tests   │          │ - Export data    │
└──────────────────┘          └──────────────────┘
```

---

## 📦 Installation

### 1. Install Dependencies

```bash
# Install all required packages
pip install apache-airflow==2.8.0
pip install streamlit==1.31.0
pip install flask==3.0.0
pip install flask-cors==4.0.0
pip install plotly==5.18.0

# Or use the updated requirements file
pip install -r requirements.txt
```

### 2. Initialize Airflow

```bash
# Set Airflow home directory (optional, defaults to ~/airflow)
export AIRFLOW_HOME=$(pwd)/airflow

# Initialize the database
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

### 3. Configure Airflow

Edit `airflow/airflow.cfg`:

```ini
[core]
# Use your project directory
dags_folder = /path/to/Merton_PD/airflow/dags
load_examples = False

[scheduler]
catchup_by_default = False

[webserver]
web_server_port = 8080
```

### 4. Project Structure

Ensure your project has this structure:

```
Merton_PD/
├── airflow/
│   ├── dags/
│   │   ├── daily_merton_pipeline.py
│   │   └── weekly_advanced_analysis.py
│   └── airflow.cfg
├── api/
│   └── merton_api.py
├── streamlit_app/
│   └── dashboard.py
├── src/
│   ├── merton/
│   │   ├── solver.py
│   │   ├── pipeline.py
│   │   ├── bootstrap.py
│   │   ├── sensitivity.py
│   │   └── stress_testing.py
│   ├── db/
│   │   └── engine.py
│   └── utils/
├── scripts/
├── config/
├── results/  # Will be created automatically
└── requirements.txt
```

---

## 🚀 Running the System

### Option 1: All-in-One (Development)

Run each component in a separate terminal:

**Terminal 1: Airflow Scheduler**
```bash
cd /path/to/Merton_PD
export AIRFLOW_HOME=$(pwd)/airflow
airflow scheduler
```

**Terminal 2: Airflow Webserver**
```bash
cd /path/to/Merton_PD
export AIRFLOW_HOME=$(pwd)/airflow
airflow webserver --port 8080
```

**Terminal 3: REST API**
```bash
cd /path/to/Merton_PD
python api/merton_api.py
```

**Terminal 4: Streamlit Dashboard**
```bash
cd /path/to/Merton_PD
streamlit run streamlit_app/dashboard.py
```

### Option 2: Production Setup

Create a supervisor/systemd config or use Docker Compose (see below).

---

## 🐳 Docker Compose (Optional but Recommended)

Create `docker-compose.yml`:

```yaml
version: '3.8'

services:
  postgres:
    image: postgres:14
    environment:
      POSTGRES_USER: merton_user
      POSTGRES_PASSWORD: your_password
      POSTGRES_DB: merton_db
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data

  airflow-webserver:
    build: .
    command: webserver
    ports:
      - "8080:8080"
    depends_on:
      - postgres
    environment:
      AIRFLOW__CORE__SQL_ALCHEMY_CONN: postgresql://merton_user:your_password@postgres/merton_db
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./src:/opt/airflow/src
      - ./scripts:/opt/airflow/scripts

  airflow-scheduler:
    build: .
    command: scheduler
    depends_on:
      - postgres
    environment:
      AIRFLOW__CORE__SQL_ALCHEMY_CONN: postgresql://merton_user:your_password@postgres/merton_db
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
    volumes:
      - ./airflow/dags:/opt/airflow/dags
      - ./src:/opt/airflow/src
      - ./scripts:/opt/airflow/scripts

  api:
    build: .
    command: python api/merton_api.py
    ports:
      - "5000:5000"
    depends_on:
      - postgres

  streamlit:
    build: .
    command: streamlit run streamlit_app/dashboard.py
    ports:
      - "8501:8501"
    depends_on:
      - postgres

volumes:
  postgres_data:
```

Then run:
```bash
docker-compose up -d
```

---

## 🎯 Using the System

### 1. Airflow UI

Access: http://localhost:8080

**Default credentials:**
- Username: `admin`
- Password: `admin`

**Enable DAGs:**
1. Navigate to DAGs page
2. Toggle ON for:
   - `daily_merton_pipeline`
   - `weekly_advanced_analysis`

**Manual Trigger:**
- Click the ▶️ play button on any DAG to run it manually

### 2. Streamlit Dashboard

Access: http://localhost:8501

**Features:**
- 🏠 **Home**: Portfolio overview
- 🔍 **Company Analysis**: Search any ticker, view trends
- 📈 **Portfolio View**: Compare multiple companies
- ⚡ **Trading Signals**: CDS opportunities
- 🧪 **Stress Testing**: Scenario analysis

### 3. REST API

Access: http://localhost:5000

**Endpoints:**

```bash
# Health check
curl http://localhost:5000/api/health

# Get latest PD for AAPL
curl http://localhost:5000/api/pd/AAPL

# Get 90-day history
curl http://localhost:5000/api/pd/AAPL/history?days=90

# Get multiple tickers
curl -X POST http://localhost:5000/api/pd/batch \
  -H "Content-Type: application/json" \
  -d '{"tickers": ["AAPL", "MSFT", "TSLA"]}'

# Get CDS trading signals
curl http://localhost:5000/api/signals?lookback_days=30

# Get tracked companies
curl http://localhost:5000/api/universe
```

---

## 📊 Using Outputs in Other Projects

### Python Integration

```python
import requests
import pandas as pd

# Get latest PD for a company
response = requests.get('http://localhost:5000/api/pd/AAPL')
data = response.json()
print(f"AAPL Distance to Default: {data['distance_to_default']:.2f}")
print(f"AAPL PD: {data['probability_default']:.2e}")

# Get historical data
response = requests.get('http://localhost:5000/api/pd/AAPL/history?days=180')
data = response.json()
df = pd.DataFrame(data['data'])
print(df.head())

# Get batch data for portfolio
response = requests.post(
    'http://localhost:5000/api/pd/batch',
    json={'tickers': ['AAPL', 'MSFT', 'TSLA', 'JPM', 'XOM']}
)
portfolio_data = response.json()['data']

# Get CDS signals
response = requests.get('http://localhost:5000/api/signals')
signals = response.json()['signals']
for signal in signals:
    print(f"{signal['ticker']}: {signal['action']} (DD change: {signal['dd_change']:.2f})")
```

### R Integration

```r
library(httr)
library(jsonlite)

# Get latest PD
response <- GET("http://localhost:5000/api/pd/AAPL")
data <- fromJSON(content(response, "text"))
print(paste("DD:", data$distance_to_default))

# Get historical data
response <- GET("http://localhost:5000/api/pd/AAPL/history?days=90")
data <- fromJSON(content(response, "text"))
df <- as.data.frame(data$data)
```

### Excel/VBA Integration

```vba
Sub GetMertonData()
    Dim http As Object
    Set http = CreateObject("MSXML2.XMLHTTP")
    
    http.Open "GET", "http://localhost:5000/api/pd/AAPL", False
    http.send
    
    Dim json As Object
    Set json = JsonConverter.ParseJson(http.responseText)
    
    Range("A1").Value = json("ticker")
    Range("B1").Value = json("distance_to_default")
    Range("C1").Value = json("probability_default")
End Sub
```

---

## 🔧 Troubleshooting

### Airflow Issues

**DAGs not showing up:**
```bash
# Check for errors
airflow dags list-import-errors

# Verify dags folder
airflow dags list
```

**Task failures:**
- Check logs in Airflow UI (Graph view → Click task → Log)
- Verify database connection
- Check Python path includes project root

### Streamlit Issues

**Database connection error:**
- Verify `src/db/engine.py` has correct credentials
- Test: `python -c "from src.db.engine import ENGINE; print(ENGINE)"`

**Import errors:**
- Ensure you're running from project root
- Check `PYTHONPATH` includes project directory

### API Issues

**Port already in use:**
```bash
# Find and kill process using port 5000
lsof -ti:5000 | xargs kill -9

# Or use a different port
python api/merton_api.py --port 5001
```

---

## 📈 CDS Trading Strategy

### Signal Generation Logic

The API generates signals based on DD changes:

**LONG PROTECTION (Buy CDS):**
- Trigger: DD drops by ≥ 1.0σ in 30 days
- Interpretation: Credit quality deteriorating
- Action: Buy credit protection (profit if company defaults)

**SHORT PROTECTION (Sell CDS):**
- Trigger: DD rises by ≥ 1.0σ in 30 days
- Interpretation: Credit quality improving  
- Action: Sell credit protection (collect premium)

### Integration Example

```python
import requests

def get_cds_signals():
    """Get latest CDS trading signals."""
    response = requests.get('http://localhost:5000/api/signals')
    signals = response.json()['signals']
    
    for signal in signals:
        ticker = signal['ticker']
        action = signal['action']
        dd_change = signal['dd_change']
        strength = signal['signal_strength']
        
        if action == 'LONG_PROTECTION' and strength > 0.7:
            print(f"STRONG BUY SIGNAL: {ticker}")
            print(f"  DD Change: {dd_change:.2f}σ")
            print(f"  Strength: {strength:.0%}")
            # Execute trade: buy_cds(ticker)
        
        elif action == 'SHORT_PROTECTION' and strength > 0.7:
            print(f"STRONG SELL SIGNAL: {ticker}")
            print(f"  DD Change: {dd_change:.2f}σ")
            print(f"  Strength: {strength:.0%}")
            # Execute trade: sell_cds(ticker)

# Run daily
get_cds_signals()
```

---

## 🎓 Next Steps

1. **Test the Pipeline:**
   ```bash
   # Trigger daily DAG manually
   airflow dags trigger daily_merton_pipeline
   
   # Monitor in UI
   # Visit http://localhost:8080
   ```

2. **Add More Tickers:**
   - Edit `airflow/dags/daily_merton_pipeline.py`
   - Update `tickers` list in `fetch_equity_prices()`

3. **Customize Signals:**
   - Modify `generate_trading_signals()` in daily DAG
   - Adjust thresholds, lookback periods

4. **Set Up Alerts:**
   - Add email/Slack notifications in `send_alerts()`
   - Configure SMTP settings in Airflow

5. **Production Deployment:**
   - Use Docker Compose
   - Set up monitoring (Prometheus/Grafana)
   - Configure backups
   - Add authentication

---

## 📚 Resources

- **Airflow Docs**: https://airflow.apache.org/docs/
- **Streamlit Docs**: https://docs.streamlit.io/
- **Flask Docs**: https://flask.palletsprojects.com/

---

## 🚀 You're Ready!

Your complete Merton credit risk system is now operational:

✅ **Automated Daily Pipeline** (Airflow)  
✅ **Interactive Dashboard** (Streamlit)  
✅ **REST API** (Flask)  
✅ **CDS Trading Signals**  
✅ **Advanced Analytics** (Weekly)  

Access your dashboards:
- **Airflow**: http://localhost:8080
- **Streamlit**: http://localhost:8501
- **API**: http://localhost:5000/api/health

Happy trading! 📊