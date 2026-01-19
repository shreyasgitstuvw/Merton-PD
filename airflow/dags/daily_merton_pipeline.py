"""
airflow/dags/daily_merton_pipeline.py

Daily Merton model pipeline DAG.

Schedule: Every weekday at 6 PM EST (after market close)

Tasks:
1. Fetch latest equity prices
2. Update balance sheets (if available)
3. Calculate equity volatility
4. Build Merton inputs
5. Run Merton solver
6. Generate trading signals
7. Send alerts (if configured)
"""

from datetime import datetime, timedelta
from pathlib import Path
import sys

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.dates import days_ago

# Default arguments
default_args = {
    'owner': 'merton_pipeline',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
}


# ============================================================
# TASK FUNCTIONS
# ============================================================

def fetch_equity_prices(**context):
    """Fetch latest equity prices from Yahoo Finance."""
    from scripts.fetchers.equity.fetch_equity import main as fetch_equity

    print("=" * 60)
    print("FETCHING EQUITY PRICES")
    print("=" * 60)

    # Get tickers from config or default list
    tickers = ['AAPL', 'MSFT', 'TSLA', 'JPM', 'XOM']

    for ticker in tickers:
        print(f"\nFetching {ticker}...")
        try:
            fetch_equity(ticker)
            print(f"[OK] {ticker} fetched successfully")
        except Exception as e:
            print(f"[ERROR] {ticker} failed: {e}")
            raise

    print("\n[OK] All equity prices fetched")
    return tickers


def update_balance_sheets(**context):
    """Update balance sheets if new quarterly data is available."""
    from scripts.fetchers.balance_sheet.run_balance_sheet_ingestion import main as ingest_balance_sheets

    print("=" * 60)
    print("UPDATING BALANCE SHEETS")
    print("=" * 60)

    tickers = context['ti'].xcom_pull(task_ids='fetch_equity_prices')

    try:
        ingest_balance_sheets(tickers)
        print("[OK] Balance sheets updated")
    except Exception as e:
        print(f"[WARNING] Balance sheet update failed: {e}")
        # Don't fail the DAG if balance sheets fail (they're quarterly)
        pass


def calculate_volatility(**context):
    """Calculate equity volatility for all tickers."""
    from features.equity_volatility import calculate_equity_volatility
    from src.db.engine import ENGINE

    print("=" * 60)
    print("CALCULATING EQUITY VOLATILITY")
    print("=" * 60)

    tickers = context['ti'].xcom_pull(task_ids='fetch_equity_prices')

    for ticker in tickers:
        print(f"\nCalculating volatility for {ticker}...")
        try:
            calculate_equity_volatility(ticker, ENGINE)
            print(f"[OK] {ticker} volatility calculated")
        except Exception as e:
            print(f"[ERROR] {ticker} failed: {e}")
            raise

    print("\n[OK] All volatilities calculated")


def build_merton_inputs(**context):
    """Build daily Merton input panel."""
    from scripts.run_daily_panel import main as build_panel

    print("=" * 60)
    print("BUILDING MERTON INPUTS")
    print("=" * 60)

    tickers = context['ti'].xcom_pull(task_ids='fetch_equity_prices')

    for ticker in tickers:
        print(f"\nBuilding inputs for {ticker}...")
        try:
            build_panel(ticker)
            print(f"[OK] {ticker} inputs built")
        except Exception as e:
            print(f"[ERROR] {ticker} failed: {e}")
            raise

    print("\n[OK] All inputs built")


def run_merton_model(**context):
    """Run Merton model for all tickers."""
    from src.merton.pipeline import MertonPipeline
    from src.db.engine import ENGINE

    print("=" * 60)
    print("RUNNING MERTON MODEL")
    print("=" * 60)

    tickers = context['ti'].xcom_pull(task_ids='fetch_equity_prices')

    pipeline = MertonPipeline(ENGINE)

    results = {}
    for ticker in tickers:
        print(f"\nProcessing {ticker}...")
        try:
            result = pipeline.run(ticker, validate=True, store=True)
            results[ticker] = result

            print(f"[OK] {ticker}:")
            print(f"  Rows processed: {result['count']}")
            print(f"  Converged: {result['converged_count']}/{result['count']}")
            if result['count'] > 0:
                latest = result['data'].iloc[-1]
                print(f"  Latest DD: {latest['distance_to_default']:.2f}")
                print(f"  Latest PD: {latest['probability_default']:.2e}")
        except Exception as e:
            print(f"[ERROR] {ticker} failed: {e}")
            raise

    print("\n[OK] Merton model completed for all tickers")

    # Store results in XCom
    context['ti'].xcom_push(key='merton_results', value=results)
    return results


def generate_trading_signals(**context):
    """Generate CDS trading signals based on DD changes."""
    import pandas as pd
    from src.db.engine import ENGINE

    print("=" * 60)
    print("GENERATING TRADING SIGNALS")
    print("=" * 60)

    # Query for DD changes
    query = """
        WITH current_dd AS (
            SELECT DISTINCT ON (ticker)
                ticker,
                date as current_date,
                distance_to_default as current_dd,
                probability_default as current_pd
            FROM merton_outputs
            ORDER BY ticker, date DESC
        ),
        historical_dd AS (
            SELECT DISTINCT ON (ticker)
                ticker,
                date as historical_date,
                distance_to_default as historical_dd
            FROM merton_outputs
            WHERE date <= CURRENT_DATE - INTERVAL '30 days'
            ORDER BY ticker, date DESC
        )
        SELECT 
            c.ticker,
            c.current_dd,
            h.historical_dd,
            (c.current_dd - h.historical_dd) as dd_change
        FROM current_dd c
        LEFT JOIN historical_dd h ON c.ticker = h.ticker
        WHERE h.historical_dd IS NOT NULL
        AND ABS(c.current_dd - h.historical_dd) >= 1.0
        ORDER BY ABS(c.current_dd - h.historical_dd) DESC
    """

    df = pd.read_sql(query, ENGINE)

    if df.empty:
        print("[INFO] No significant DD changes detected")
        return []

    # Generate signals
    signals = []
    for _, row in df.iterrows():
        dd_change = row['dd_change']

        if dd_change < -1.0:
            action = 'LONG_PROTECTION'
            reason = 'Credit quality deteriorating'
        elif dd_change > 1.0:
            action = 'SHORT_PROTECTION'
            reason = 'Credit quality improving'
        else:
            continue

        signal = {
            'ticker': row['ticker'],
            'action': action,
            'reason': reason,
            'current_dd': float(row['current_dd']),
            'dd_change': float(dd_change),
            'timestamp': datetime.now()
        }
        signals.append(signal)

        print(f"\n[SIGNAL] {signal['ticker']}:")
        print(f"  Action: {action}")
        print(f"  DD Change: {dd_change:.2f}σ")
        print(f"  Reason: {reason}")

    print(f"\n[OK] Generated {len(signals)} trading signals")

    # Store signals in XCom
    context['ti'].xcom_push(key='trading_signals', value=signals)
    return signals


def send_alerts(**context):
    """Send alerts if significant signals are detected."""
    signals = context['ti'].xcom_pull(task_ids='generate_signals', key='trading_signals')

    if not signals:
        print("[INFO] No alerts to send")
        return

    print("=" * 60)
    print("SENDING ALERTS")
    print("=" * 60)

    # TODO: Implement email/Slack notifications
    # For now, just log
    for signal in signals:
        print(f"\n[ALERT] {signal['ticker']} - {signal['action']}")
        print(f"  {signal['reason']}")
        print(f"  DD Change: {signal['dd_change']:.2f}σ")

    print(f"\n[OK] Processed {len(signals)} alerts")


def cleanup_old_data(**context):
    """Clean up data older than 2 years."""
    from src.db.engine import ENGINE

    print("=" * 60)
    print("CLEANING UP OLD DATA")
    print("=" * 60)

    cutoff_date = (datetime.now() - timedelta(days=730)).date()

    with ENGINE.connect() as conn:
        result = conn.execute(f"""
            DELETE FROM merton_outputs
            WHERE date < '{cutoff_date}'
        """)

        rows_deleted = result.rowcount
        conn.commit()

    print(f"[OK] Deleted {rows_deleted} old rows (before {cutoff_date})")


# ============================================================
# DAG DEFINITION
# ============================================================

with DAG(
        'daily_merton_pipeline',
        default_args=default_args,
        description='Daily Merton model credit risk calculation',
        schedule_interval='0 18 * * 1-5',  # 6 PM EST, Monday-Friday
        start_date=datetime(2026, 1, 1),
        catchup=False,
        tags=['merton', 'credit-risk', 'daily'],
) as dag:
    # Task 1: Fetch equity prices
    task_fetch_equity = PythonOperator(
        task_id='fetch_equity_prices',
        python_callable=fetch_equity_prices,
        provide_context=True,
    )

    # Task 2: Update balance sheets
    task_update_balance_sheets = PythonOperator(
        task_id='update_balance_sheets',
        python_callable=update_balance_sheets,
        provide_context=True,
    )

    # Task 3: Calculate volatility
    task_calc_volatility = PythonOperator(
        task_id='calculate_volatility',
        python_callable=calculate_volatility,
        provide_context=True,
    )

    # Task 4: Build Merton inputs
    task_build_inputs = PythonOperator(
        task_id='build_merton_inputs',
        python_callable=build_merton_inputs,
        provide_context=True,
    )

    # Task 5: Run Merton model
    task_run_merton = PythonOperator(
        task_id='run_merton_model',
        python_callable=run_merton_model,
        provide_context=True,
    )

    # Task 6: Generate signals
    task_generate_signals = PythonOperator(
        task_id='generate_signals',
        python_callable=generate_trading_signals,
        provide_context=True,
    )

    # Task 7: Send alerts
    task_send_alerts = PythonOperator(
        task_id='send_alerts',
        python_callable=send_alerts,
        provide_context=True,
    )

    # Task 8: Cleanup old data (weekly)
    task_cleanup = PythonOperator(
        task_id='cleanup_old_data',
        python_callable=cleanup_old_data,
        provide_context=True,
    )

    # Define task dependencies
    task_fetch_equity >> task_update_balance_sheets >> task_calc_volatility
    task_calc_volatility >> task_build_inputs >> task_run_merton
    task_run_merton >> task_generate_signals >> task_send_alerts
    task_send_alerts >> task_cleanup