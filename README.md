# Merton Credit Risk System 📊

A comprehensive credit risk analysis platform using the Merton structural model with automated pipelines, interactive dashboards, and CDS trading signals.

## 🎯 Features

### Core Analytics
- ✅ **Merton Structural Model** - Calculate Distance to Default (DD) and Probability of Default (PD)
- ✅ **Real-time Data Pipeline** - Automated data ingestion from Yahoo Finance
- ✅ **Advanced Analytics** - Bootstrap uncertainty, sensitivity analysis, stress testing
- ✅ **Credit Ratings** - Implied credit ratings from DD values

### Automation
- ✅ **Airflow Orchestration** - Daily and weekly automated pipelines
- ✅ **Scheduled Execution** - Runs after market close every weekday
- ✅ **Error Handling** - Robust retry logic and logging

### User Interfaces
- ✅ **Streamlit Dashboard** - Interactive web interface for analysis
- ✅ **REST API** - Export data to other projects
- ✅ **Trading Signals** - CDS opportunities based on credit deterioration/improvement

### Trading Strategy
- ✅ **Signal Generation** - Automated detection of credit quality changes
- ✅ **Risk Metrics** - DD changes, signal strength, confidence intervals
- ✅ **Backtesting Ready** - Historical data for strategy validation

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                   AIRFLOW ORCHESTRATION                      │
├─────────────────────────────────────────────────────────────┤
│  📅 Daily Pipeline (Weekdays @ 6 PM):                       │
│    • Fetch equity prices                                    │
│    • Update balance sheets                                  │
│    • Calculate Merton PD/DD                                 │
│    • Generate CDS signals                                   │
│                                                              │
│  📅 Weekly Pipeline (Sundays @ 8 AM):                       │
│    • Bootstrap analysis                                     │
│    • Sensitivity analysis                                   │
│    • Stress testing                                         │
└──────────────────────┬──────────────────────────────────────┘
                       │
                       ▼
┌─────────────────────────────────────────────────────────────┐
│                  📊 PostgreSQL DATABASE                      │
│  • merton_outputs (DD/PD time series)                       │
│  • merton_inputs (daily panel)                              │
│  • equity_prices, balance_sheets, etc.                      │
└──────────────────────┬──────────────────────────────────────┘
                       │
        ┌──────────────┴──────────────┐
        ▼                              ▼
┌──────────────────┐          ┌──────────────────┐
│  🎨 STREAMLIT    │          │   🔌 REST API    │
│  (Port 8501)     │          │   (Port 5000)    │
│                  │          │                  │
│ • Search company │          │ • Python/R/Excel │
│ • View trends    │          │ • Integration    │
│ • CDS signals    │          │ • Real-time data │
│ • Stress tests   │          │ • Trading bots   │
└──────────────────┘          └──────────────────┘
```

---

## 🚀 Quick Start

### Option 1: One-Click Launch (Windows)

```powershell
# Open PowerShell in project directory
.\launch.ps1
```

### Option 2: One-Click Launch (Mac/Linux)

```bash
# Make script executable
chmod +x launch.sh

# Run
./launch.sh
```

### Option 3: Manual Launch

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Initialize Airflow
export AIRFLOW_HOME=$(pwd)/airflow
airflow db init
airflow users create --username admin --password admin --role Admin --email admin@example.com

# 3. Start services (in separate terminals)
airflow scheduler                    # Terminal 1
airflow webserver --port 8080        # Terminal 2
python api/merton_api.py             # Terminal 3
streamlit run streamlit_app/dashboard.py  # Terminal 4
```

---

## 📊 Access Your System

Once running, access these interfaces:

| Service | URL | Credentials |
|---------|-----|-------------|
| 🎨 **Streamlit Dashboard** | http://localhost:8501 | None required |
| ⚙️ **Airflow UI** | http://localhost:8080 | admin / admin |
| 🔌 **REST API** | http://localhost:5000 | None required |

---

## 🎨 Streamlit Dashboard

### Home Page
- Portfolio overview with all tracked companies
- Latest DD, PD, credit ratings
- Active trading signals count

### Company Analysis
- Search any ticker (AAPL, MSFT, etc.)
- Time series charts for DD and PD
- Summary statistics
- Credit rating visualization

### Portfolio View
- Compare multiple companies
- Side-by-side DD comparison
- Historical trend comparison

### Trading Signals
- **LONG PROTECTION**: Buy CDS when credit deteriorates
- **SHORT PROTECTION**: Sell CDS when credit improves
- Signal strength scoring (0-100%)
- Detailed rationale for each signal

### Stress Testing
- GFC 2008 scenario
- COVID 2020 scenario
- Custom stress scenarios
- DD changes under stress

---

## 🔌 REST API Usage

### Get Latest PD for a Company

```python
import requests

response = requests.get('http://localhost:5000/api/pd/AAPL')
data = response.json()

print(f"Distance to Default: {data['distance_to_default']:.2f}σ")
print(f"Probability of Default: {data['probability_default']:.2e}")
print(f"Credit Rating: {data['rating']}")
```

### Get Historical Data

```python
response = requests.get('http://localhost:5000/api/pd/AAPL/history?days=180')
data = response.json()

import pandas as pd
df = pd.DataFrame(data['data'])
df.plot(x='date', y='distance_to_default')
```

### Get Batch Data for Portfolio

```python
response = requests.post(
    'http://localhost:5000/api/pd/batch',
    json={'tickers': ['AAPL', 'MSFT', 'TSLA', 'JPM', 'XOM']}
)

portfolio = response.json()['data']
for ticker, metrics in portfolio.items():
    print(f"{ticker}: DD={metrics['distance_to_default']:.2f}")
```

### Get CDS Trading Signals

```python
response = requests.get('http://localhost:5000/api/signals?lookback_days=30')
signals = response.json()['signals']

for signal in signals:
    print(f"{signal['ticker']}: {signal['action']}")
    print(f"  DD Change: {signal['dd_change']:.2f}σ")
    print(f"  Signal Strength: {signal['signal_strength']:.0%}")
```

---

## ⚙️ Airflow DAGs

### Daily Pipeline (`daily_merton_pipeline`)

**Schedule**: Monday-Friday at 6 PM EST (after market close)

**Tasks**:
1. Fetch equity prices from Yahoo Finance
2. Update balance sheets (if quarterly data available)
3. Calculate equity volatility
4. Build Merton inputs (join equity, debt, volatility)
5. Run Merton solver (calculate V, σ_V, DD, PD)
6. Generate CDS trading signals
7. Send alerts (if configured)
8. Cleanup old data (>2 years)

**Duration**: ~5-10 minutes for 5 companies

### Weekly Pipeline (`weekly_advanced_analysis`)

**Schedule**: Sundays at 8 AM EST

**Tasks**:
1. Bootstrap uncertainty quantification (1000 iterations)
2. Sensitivity analysis (volatility, debt, rates)
3. Stress testing (GFC, COVID, custom scenarios)
4. Generate weekly summary report

**Duration**: ~20-30 minutes for 5 companies

---

## 📈 CDS Trading Strategy

### Signal Logic

The system generates two types of signals:

#### 🔴 LONG PROTECTION (Buy CDS)

**Trigger**: Distance to Default drops by ≥ 1.0σ over 30 days

**Interpretation**: Credit quality is deteriorating

**Action**: Buy credit protection (profit if company defaults)

**Example**:
```
TICKER: XYZ
DD Change: -2.5σ (from 6.0 to 3.5)
Signal Strength: 85%
Recommendation: BUY CDS - Credit risk increasing significantly
```

#### 🟢 SHORT PROTECTION (Sell CDS)

**Trigger**: Distance to Default rises by ≥ 1.0σ over 30 days

**Interpretation**: Credit quality is improving

**Action**: Sell credit protection (collect premium as credit improves)

**Example**:
```
TICKER: ABC
DD Change: +3.2σ (from 4.5 to 7.7)
Signal Strength: 95%
Recommendation: SELL CDS - Credit risk decreasing, collect premium
```

### Integration with Trading Systems

```python
import requests

def execute_cds_strategy():
    """Daily CDS strategy execution."""
    
    # Get signals from API
    response = requests.get('http://localhost:5000/api/signals')
    signals = response.json()['signals']
    
    for signal in signals:
        ticker = signal['ticker']
        action = signal['action']
        strength = signal['signal_strength']
        
        # Only act on strong signals (>70%)
        if strength < 0.7:
            continue
        
        if action == 'LONG_PROTECTION':
            # Buy CDS protection
            print(f"BUY CDS: {ticker}")
            # buy_cds(ticker, notional=1_000_000)
            
        elif action == 'SHORT_PROTECTION':
            # Sell CDS protection
            print(f"SELL CDS: {ticker}")
            # sell_cds(ticker, notional=1_000_000)

# Run daily after market close
execute_cds_strategy()
```

---

## 📁 Project Structure

```
Merton_PD/
├── airflow/
│   ├── dags/
│   │   ├── daily_merton_pipeline.py       # Daily automation
│   │   └── weekly_advanced_analysis.py    # Weekly analytics
│   └── airflow.cfg                         # Airflow config
│
├── api/
│   └── merton_api.py                       # REST API endpoints
│
├── streamlit_app/
│   └── dashboard.py                        # Interactive dashboard
│
├── src/
│   ├── merton/
│   │   ├── solver.py                       # Core Merton solver
│   │   ├── pipeline.py                     # End-to-end pipeline
│   │   ├── distance_to_default.py          # DD/PD calculations
│   │   ├── bootstrap.py                    # Uncertainty quantification
│   │   ├── sensitivity.py                  # Sensitivity analysis
│   │   └── stress_testing.py               # Stress scenarios
│   ├── db/
│   │   ├── engine.py                       # Database connection
│   │   └── schema.sql                      # Table definitions
│   └── utils/
│       ├── logger.py                       # Logging
│       └── config_loader.py                # YAML config
│
├── scripts/
│   ├── fetchers/                           # Data ingestion
│   ├── run_merton_model.py                 # Standalone runner
│   └── test_merton_integration.py          # Integration tests
│
├── features/
│   ├── equity_volatility.py                # Volatility calculation
│   ├── merton_inputs.py                    # Input assembly
│   └── debt_daily.py                       # Debt forward-fill
│
├── config/
│   ├── merton.yaml                         # Merton config
│   └── logging.yaml                        # Logging config
│
├── results/                                 # Generated reports
├── logs/                                    # Service logs
│
├── launch.sh                                # Linux/Mac launcher
├── launch.ps1                               # Windows launcher
├── requirements.txt                         # Python dependencies
└── README.md                                # This file
```

---

## 🔧 Configuration

### Add New Companies

Edit `airflow/dags/daily_merton_pipeline.py`:

```python
def fetch_equity_prices(**context):
    # Add your tickers here
    tickers = ['AAPL', 'MSFT', 'TSLA', 'JPM', 'XOM', 'GOOGL', 'AMZN']
    ...
```

### Adjust Signal Thresholds

Edit `config/merton.yaml`:

```yaml
signals:
  lookback_days: 30          # Period for DD change calculation
  min_dd_change: 1.0         # Minimum DD change to trigger signal (σ)
  min_signal_strength: 0.7   # Minimum strength to execute trade
```

### Change Schedule

Edit DAG files to adjust timing:

```python
# Daily at 6 PM EST, Monday-Friday
schedule_interval='0 18 * * 1-5'

# Weekly on Sundays at 8 AM EST
schedule_interval='0 8 * * 0'
```

---

## 🧪 Testing

### Run Integration Tests

```bash
python scripts/test_merton_integration.py
```

**Expected Output**:
```
TEST 1: Solver with Known Inputs
  ✅ PASSED

TEST 2: DD/PD Calculations
  ✅ PASSED

TEST 3: DataFrame Processing
  ✅ PASSED

TEST 4: Database Storage
  ✅ PASSED

TEST 5: End-to-End Pipeline
  ✅ PASSED

All tests passed! ✅
```

### Test Individual Components

```bash
# Test API
python scripts/test_api.py

# Test Streamlit (manual)
streamlit run streamlit_app/dashboard.py

# Test Airflow DAG syntax
airflow dags test daily_merton_pipeline 2026-01-18
```

---

## 📊 Understanding the Outputs

### Distance to Default (DD)

**Definition**: Number of standard deviations the asset value is above the default threshold

| DD Range | Credit Rating | Default Risk | Interpretation |
|----------|---------------|--------------|----------------|
| > 10 | AAA | < 0.01% | Extremely safe |
| 8-10 | AA | 0.01-0.05% | Very safe |
| 6-8 | A | 0.05-0.2% | Safe |
| 4-6 | BBB | 0.2-1% | Investment grade |
| 2-4 | BB | 1-5% | Speculative |
| < 2 | B/CCC | > 5% | High risk |

### Probability of Default (PD)

**Definition**: Likelihood of default within 1 year

**Display Format**: Basis points (bps) where 1 bp = 0.01%

**Example**:
```
AAPL: PD = 0.0004 bps (0.000004%)
      DD = 11.74σ
      Rating: AAA
```

**Interpretation**: AAPL has a 0.000004% chance of defaulting in the next year (virtually zero risk).

---

## 🐛 Troubleshooting

### Services Won't Start

```bash
# Check if ports are in use
netstat -an | grep -E "8080|5000|8501"

# Kill processes on ports
kill -9 $(lsof -ti:8080,5000,8501)

# Restart
./launch.sh
```

### Database Connection Errors

```python
# Test connection
python -c "from src.db.engine import ENGINE; import pandas as pd; pd.read_sql('SELECT 1', ENGINE)"

# Check credentials in src/db/engine.py
```

### Airflow DAG Not Showing

```bash
# Check for import errors
airflow dags list-import-errors

# Verify DAG exists
airflow dags list | grep merton

# Refresh
airflow dags list
```

### No Data in Dashboard

```bash
# Run pipeline manually
python scripts/run_merton_model.py AAPL

# Check database
psql -U merton_user -d merton_db -c "SELECT COUNT(*) FROM merton_outputs;"
```

---

## 📚 Key Files Reference

| File | Purpose |
|------|---------|
| `src/merton/solver.py` | Core Merton model implementation |
| `src/merton/pipeline.py` | End-to-end orchestration |
| `airflow/dags/daily_merton_pipeline.py` | Daily automation |
| `api/merton_api.py` | REST API for external access |
| `streamlit_app/dashboard.py` | Interactive UI |
| `config/merton.yaml` | Model configuration |
| `requirements.txt` | Python dependencies |
| `AIRFLOW_SETUP.md` | Detailed Airflow guide |

---

## 🎓 Next Steps

1. **Add More Companies**: Expand your coverage
2. **Backtest Signals**: Validate CDS strategy performance
3. **Set Up Alerts**: Email/Slack notifications for signals
4. **Deploy to Cloud**: AWS/GCP for production use
5. **Add LGD/EAD**: Full credit risk framework
6. **Portfolio Analysis**: Cross-company correlations

---

## 📝 License

MIT License - feel free to use for commercial or personal projects.

---

## 🙏 Acknowledgments

- Merton (1974) - Structural credit risk model
- KMV Corporation - Practical implementation methodology
- Yahoo Finance - Free financial data

---

## 📧 Support

For issues or questions:
1. Check logs in `logs/` directory
2. Review troubleshooting section
3. Test individual components
4. Check Airflow UI for task logs

---

**Built with ❤️ for credit risk analysis and CDS trading**