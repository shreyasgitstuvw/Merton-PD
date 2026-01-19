"""
airflow/dags/weekly_advanced_analysis.py

Weekly advanced analysis DAG.

Schedule: Every Sunday at 8 AM EST

Tasks:
1. Bootstrap uncertainty quantification
2. Sensitivity analysis
3. Stress testing
4. Generate reports
"""

from datetime import datetime, timedelta
from pathlib import Path
import sys

# Add project root to path
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago

# Default arguments
default_args = {
    'owner': 'merton_advanced',
    'depends_on_past': False,
    'start_date': datetime(2026, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=10),
}

# ============================================================
# TASK FUNCTIONS
# ============================================================

def run_bootstrap_analysis(**context):
    """Run bootstrap uncertainty quantification."""
    from src.merton.bootstrap import MertonBootstrap
    from src.db.engine import ENGINE
    import pandas as pd
    
    print("=" * 60)
    print("BOOTSTRAP UNCERTAINTY QUANTIFICATION")
    print("=" * 60)
    
    # Get tickers
    tickers_df = pd.read_sql("SELECT DISTINCT ticker FROM merton_outputs", ENGINE)
    tickers = tickers_df['ticker'].tolist()
    
    bootstrap = MertonBootstrap(n_iterations=1000, random_seed=42)
    
    all_results = []
    for ticker in tickers:
        print(f"\nBootstrapping {ticker}...")
        
        # Get recent data
        query = f"""
            SELECT *
            FROM merton_inputs
            WHERE ticker = '{ticker}'
            ORDER BY date DESC
            LIMIT 10
        """
        df = pd.read_sql(query, ENGINE)
        
        if df.empty:
            print(f"[SKIP] No data for {ticker}")
            continue
        
        try:
            results = bootstrap.run_bootstrap_dataframe(df, show_progress=False)
            results['ticker'] = ticker
            all_results.append(results)
            
            print(f"[OK] {ticker}: {len(results)} rows bootstrapped")
        except Exception as e:
            print(f"[ERROR] {ticker} failed: {e}")
    
    # Save results
    if all_results:
        combined = pd.concat(all_results, ignore_index=True)
        output_file = Path("results") / f"bootstrap_weekly_{datetime.now().strftime('%Y%m%d')}.csv"
        output_file.parent.mkdir(exist_ok=True)
        combined.to_csv(output_file, index=False)
        print(f"\n[OK] Results saved to {output_file}")
    
    return len(all_results)


def run_sensitivity_analysis(**context):
    """Run sensitivity analysis."""
    from src.merton.sensitivity import MertonSensitivity
    from src.db.engine import ENGINE
    import pandas as pd
    
    print("=" * 60)
    print("SENSITIVITY ANALYSIS")
    print("=" * 60)
    
    # Get tickers
    tickers_df = pd.read_sql("SELECT DISTINCT ticker FROM merton_outputs", ENGINE)
    tickers = tickers_df['ticker'].tolist()
    
    sensitivity = MertonSensitivity()
    
    all_results = {}
    for ticker in tickers:
        print(f"\nAnalyzing {ticker}...")
        
        # Get latest data
        query = f"""
            SELECT *
            FROM merton_inputs
            WHERE ticker = '{ticker}'
            ORDER BY date DESC
            LIMIT 1
        """
        df = pd.read_sql(query, ENGINE)
        
        if df.empty:
            print(f"[SKIP] No data for {ticker}")
            continue
        
        try:
            inputs = df.iloc[0]
            results = sensitivity.analyze_all(
                E=inputs['equity_value'],
                D=inputs['debt_value'],
                sigma_E=inputs['equity_volatility'],
                r=inputs['risk_free_rate'],
                T=inputs['time_to_maturity']
            )
            
            all_results[ticker] = results
            print(f"[OK] {ticker}: {len(results)} sensitivity tests")
        except Exception as e:
            print(f"[ERROR] {ticker} failed: {e}")
    
    # Save results
    if all_results:
        for ticker, results in all_results.items():
            for name, df in results.items():
                output_file = Path("results") / f"sensitivity_{ticker}_{name}_{datetime.now().strftime('%Y%m%d')}.csv"
                output_file.parent.mkdir(exist_ok=True)
                df.to_csv(output_file, index=False)
        
        print(f"\n[OK] Sensitivity results saved for {len(all_results)} tickers")
    
    return len(all_results)


def run_stress_testing(**context):
    """Run stress testing scenarios."""
    from src.merton.stress_testing import MertonStressTesting
    from src.db.engine import ENGINE
    import pandas as pd
    
    print("=" * 60)
    print("STRESS TESTING")
    print("=" * 60)
    
    # Get tickers
    tickers_df = pd.read_sql("SELECT DISTINCT ticker FROM merton_outputs", ENGINE)
    tickers = tickers_df['ticker'].tolist()
    
    stress_tester = MertonStressTesting()
    
    all_results = []
    for ticker in tickers:
        print(f"\nStress testing {ticker}...")
        
        # Get latest data
        query = f"""
            SELECT *
            FROM merton_inputs
            WHERE ticker = '{ticker}'
            ORDER BY date DESC
            LIMIT 1
        """
        df = pd.read_sql(query, ENGINE)
        
        if df.empty:
            print(f"[SKIP] No data for {ticker}")
            continue
        
        try:
            inputs = df.iloc[0]
            results = stress_tester.run_all_scenarios(
                E=inputs['equity_value'],
                D=inputs['debt_value'],
                sigma_E=inputs['equity_volatility'],
                r=inputs['risk_free_rate'],
                T=inputs['time_to_maturity']
            )
            
            results['ticker'] = ticker
            all_results.append(results)
            
            print(f"[OK] {ticker}: {len(results)} scenarios tested")
        except Exception as e:
            print(f"[ERROR] {ticker} failed: {e}")
    
    # Save results
    if all_results:
        combined = pd.concat(all_results, ignore_index=True)
        output_file = Path("results") / f"stress_test_weekly_{datetime.now().strftime('%Y%m%d')}.csv"
        output_file.parent.mkdir(exist_ok=True)
        combined.to_csv(output_file, index=False)
        print(f"\n[OK] Stress test results saved to {output_file}")
    
    return len(all_results)


def generate_weekly_report(**context):
    """Generate weekly summary report."""
    import pandas as pd
    from src.db.engine import ENGINE
    
    print("=" * 60)
    print("GENERATING WEEKLY REPORT")
    print("=" * 60)
    
    # Get summary statistics for the week
    query = """
        WITH weekly_stats AS (
            SELECT 
                ticker,
                AVG(distance_to_default) as avg_dd,
                MIN(distance_to_default) as min_dd,
                MAX(distance_to_default) as max_dd,
                STDDEV(distance_to_default) as std_dd,
                AVG(probability_default) as avg_pd,
                COUNT(*) as days_tracked
            FROM merton_outputs
            WHERE date >= CURRENT_DATE - INTERVAL '7 days'
            GROUP BY ticker
        )
        SELECT * FROM weekly_stats
        ORDER BY avg_dd DESC
    """
    
    df = pd.read_sql(query, ENGINE)
    
    # Create report
    report_lines = [
        "=" * 60,
        "MERTON MODEL - WEEKLY SUMMARY REPORT",
        f"Report Date: {datetime.now().strftime('%Y-%m-%d')}",
        "=" * 60,
        "",
        "PORTFOLIO OVERVIEW",
        "-" * 60,
    ]
    
    for _, row in df.iterrows():
        report_lines.extend([
            f"\n{row['ticker']}:",
            f"  Average DD: {row['avg_dd']:.2f}σ (range: {row['min_dd']:.2f} - {row['max_dd']:.2f})",
            f"  Volatility: {row['std_dd']:.2f}σ",
            f"  Average PD: {row['avg_pd']:.2e}",
            f"  Days tracked: {row['days_tracked']}",
        ])
    
    # Get signals
    signals_query = """
        SELECT 
            ticker,
            current_dd,
            historical_dd,
            (current_dd - historical_dd) as dd_change
        FROM (
            SELECT DISTINCT ON (ticker)
                ticker,
                distance_to_default as current_dd,
                LAG(distance_to_default, 5) OVER (PARTITION BY ticker ORDER BY date) as historical_dd
            FROM merton_outputs
            ORDER BY ticker, date DESC
        ) sub
        WHERE ABS(current_dd - historical_dd) >= 1.0
    """
    
    signals_df = pd.read_sql(signals_query, ENGINE)
    
    if not signals_df.empty:
        report_lines.extend([
            "",
            "SIGNIFICANT DD CHANGES (PAST WEEK)",
            "-" * 60,
        ])
        
        for _, row in signals_df.iterrows():
            action = "DETERIORATING" if row['dd_change'] < 0 else "IMPROVING"
            report_lines.append(
                f"  {row['ticker']}: {row['dd_change']:+.2f}σ ({action})"
            )
    
    report_lines.extend([
        "",
        "=" * 60,
        f"Bootstrap runs: {context['ti'].xcom_pull(task_ids='bootstrap')}",
        f"Sensitivity tests: {context['ti'].xcom_pull(task_ids='sensitivity')}",
        f"Stress scenarios: {context['ti'].xcom_pull(task_ids='stress_testing')}",
        "=" * 60,
    ])
    
    report = "\n".join(report_lines)
    
    # Save report
    report_file = Path("results") / f"weekly_report_{datetime.now().strftime('%Y%m%d')}.txt"
    report_file.parent.mkdir(exist_ok=True)
    report_file.write_text(report)
    
    print(report)
    print(f"\n[OK] Report saved to {report_file}")


# ============================================================
# DAG DEFINITION
# ============================================================

with DAG(
    'weekly_advanced_analysis',
    default_args=default_args,
    description='Weekly advanced Merton model analysis',
    schedule_interval='0 8 * * 0',  # 8 AM EST every Sunday
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=['merton', 'credit-risk', 'weekly', 'advanced'],
) as dag:
    
    # Task 1: Bootstrap
    task_bootstrap = PythonOperator(
        task_id='bootstrap',
        python_callable=run_bootstrap_analysis,
        provide_context=True,
    )
    
    # Task 2: Sensitivity
    task_sensitivity = PythonOperator(
        task_id='sensitivity',
        python_callable=run_sensitivity_analysis,
        provide_context=True,
    )
    
    # Task 3: Stress Testing
    task_stress = PythonOperator(
        task_id='stress_testing',
        python_callable=run_stress_testing,
        provide_context=True,
    )
    
    # Task 4: Generate Report
    task_report = PythonOperator(
        task_id='generate_report',
        python_callable=generate_weekly_report,
        provide_context=True,
    )
    
    # Run bootstrap, sensitivity, and stress testing in parallel
    [task_bootstrap, task_sensitivity, task_stress] >> task_report