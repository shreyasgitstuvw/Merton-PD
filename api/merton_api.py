"""
api/merton_api.py

REST API for Merton model outputs.
Allows other projects to query PD/DD data programmatically.

Usage:
    python api/merton_api.py

Endpoints:
    GET  /api/pd/{ticker}           - Latest PD/DD for a ticker
    GET  /api/pd/{ticker}/history   - Historical PD/DD
    GET  /api/pd/batch              - Multiple tickers at once
    GET  /api/signals               - CDS trading signals
    GET  /api/health                - API health check
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta
from typing import Optional, List

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from flask import Flask, jsonify, request
from flask_cors import CORS
import pandas as pd
from src.db.engine import ENGINE

app = Flask(__name__)
CORS(app)  # Enable CORS for external access


# ============================================================
# CORE API ENDPOINTS
# ============================================================

@app.route('/api/health', methods=['GET'])
def health_check():
    """Health check endpoint."""
    try:
        # Test database connection
        pd.read_sql("SELECT 1", ENGINE)
        return jsonify({
            'status': 'healthy',
            'timestamp': datetime.now().isoformat(),
            'database': 'connected'
        }), 200
    except Exception as e:
        return jsonify({
            'status': 'unhealthy',
            'error': str(e)
        }), 500


@app.route('/api/pd/<ticker>', methods=['GET'])
def get_latest_pd(ticker: str):
    """
    Get latest PD/DD for a ticker.
    
    Example:
        GET /api/pd/AAPL
        
    Response:
        {
            "ticker": "AAPL",
            "date": "2026-01-18",
            "asset_value": 232500000000,
            "asset_volatility": 0.2276,
            "distance_to_default": 11.74,
            "probability_default": 4.09e-32,
            "leverage_ratio": 0.013,
            "converged": true
        }
    """
    try:
        ticker = ticker.upper()
        
        query = f"""
            SELECT 
                ticker,
                date,
                asset_value,
                asset_volatility,
                distance_to_default,
                probability_default,
                leverage_ratio,
                equity_to_asset_ratio,
                converged,
                iterations,
                created_at
            FROM merton_outputs
            WHERE ticker = '{ticker}'
            ORDER BY date DESC
            LIMIT 1
        """
        
        df = pd.read_sql(query, ENGINE)
        
        if df.empty:
            return jsonify({
                'error': f'No data found for ticker {ticker}'
            }), 404
        
        result = df.iloc[0].to_dict()
        result['date'] = result['date'].isoformat()
        result['created_at'] = result['created_at'].isoformat()
        
        return jsonify(result), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/pd/<ticker>/history', methods=['GET'])
def get_pd_history(ticker: str):
    """
    Get historical PD/DD for a ticker.
    
    Query Parameters:
        days: Number of days to retrieve (default: 30)
        start_date: Start date (YYYY-MM-DD)
        end_date: End date (YYYY-MM-DD)
    
    Example:
        GET /api/pd/AAPL/history?days=90
        GET /api/pd/AAPL/history?start_date=2025-01-01&end_date=2026-01-18
        
    Response:
        {
            "ticker": "AAPL",
            "count": 90,
            "data": [
                {
                    "date": "2026-01-18",
                    "distance_to_default": 11.74,
                    "probability_default": 4.09e-32,
                    ...
                },
                ...
            ],
            "summary": {
                "avg_dd": 11.72,
                "min_dd": 11.45,
                "max_dd": 12.03,
                "avg_pd": 5.2e-32
            }
        }
    """
    try:
        ticker = ticker.upper()
        
        # Parse query parameters
        days = request.args.get('days', 30, type=int)
        start_date = request.args.get('start_date')
        end_date = request.args.get('end_date')
        
        # Build query
        if start_date and end_date:
            date_filter = f"AND date BETWEEN '{start_date}' AND '{end_date}'"
        else:
            cutoff_date = (datetime.now() - timedelta(days=days)).date()
            date_filter = f"AND date >= '{cutoff_date}'"
        
        query = f"""
            SELECT 
                date,
                asset_value,
                asset_volatility,
                distance_to_default,
                probability_default,
                leverage_ratio,
                converged
            FROM merton_outputs
            WHERE ticker = '{ticker}'
            {date_filter}
            ORDER BY date DESC
        """
        
        df = pd.read_sql(query, ENGINE)
        
        if df.empty:
            return jsonify({
                'error': f'No data found for ticker {ticker}'
            }), 404
        
        # Convert to records
        data = df.to_dict(orient='records')
        for record in data:
            record['date'] = record['date'].isoformat()
        
        # Calculate summary statistics
        summary = {
            'avg_dd': float(df['distance_to_default'].mean()),
            'min_dd': float(df['distance_to_default'].min()),
            'max_dd': float(df['distance_to_default'].max()),
            'avg_pd': float(df['probability_default'].mean()),
            'convergence_rate': float(df['converged'].mean())
        }
        
        return jsonify({
            'ticker': ticker,
            'count': len(data),
            'data': data,
            'summary': summary
        }), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/pd/batch', methods=['POST'])
def get_batch_pd():
    """
    Get latest PD/DD for multiple tickers.
    
    Request Body:
        {
            "tickers": ["AAPL", "MSFT", "TSLA"]
        }
        
    Response:
        {
            "count": 3,
            "data": {
                "AAPL": {...},
                "MSFT": {...},
                "TSLA": {...}
            }
        }
    """
    try:
        data = request.get_json()
        tickers = [t.upper() for t in data.get('tickers', [])]
        
        if not tickers:
            return jsonify({'error': 'No tickers provided'}), 400
        
        # Query for all tickers
        tickers_str = "','".join(tickers)
        query = f"""
            WITH latest_dates AS (
                SELECT ticker, MAX(date) as max_date
                FROM merton_outputs
                WHERE ticker IN ('{tickers_str}')
                GROUP BY ticker
            )
            SELECT 
                m.ticker,
                m.date,
                m.asset_value,
                m.asset_volatility,
                m.distance_to_default,
                m.probability_default,
                m.leverage_ratio,
                m.converged
            FROM merton_outputs m
            JOIN latest_dates ld
                ON m.ticker = ld.ticker
                AND m.date = ld.max_date
        """
        
        df = pd.read_sql(query, ENGINE)
        
        # Convert to dict keyed by ticker
        results = {}
        for _, row in df.iterrows():
            ticker = row['ticker']
            row_dict = row.to_dict()
            row_dict['date'] = row_dict['date'].isoformat()
            results[ticker] = row_dict
        
        return jsonify({
            'count': len(results),
            'data': results
        }), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/signals', methods=['GET'])
def get_trading_signals():
    """
    Get CDS trading signals based on PD changes.
    
    Query Parameters:
        lookback_days: Period for change calculation (default: 30)
        min_dd_change: Minimum DD change to trigger signal (default: 1.0)
        
    Response:
        {
            "count": 5,
            "signals": [
                {
                    "ticker": "XYZ",
                    "action": "LONG_PROTECTION",
                    "current_dd": 3.5,
                    "previous_dd": 5.2,
                    "dd_change": -1.7,
                    "pd_change_bps": 150,
                    "signal_strength": 0.85
                },
                ...
            ]
        }
    """
    try:
        lookback_days = request.args.get('lookback_days', 30, type=int)
        min_dd_change = request.args.get('min_dd_change', 1.0, type=float)
        
        # Get current and historical DD
        query = f"""
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
                    distance_to_default as historical_dd,
                    probability_default as historical_pd
                FROM merton_outputs
                WHERE date <= CURRENT_DATE - INTERVAL '{lookback_days} days'
                ORDER BY ticker, date DESC
            )
            SELECT 
                c.ticker,
                c.current_dd,
                c.current_pd,
                h.historical_dd,
                h.historical_pd,
                (c.current_dd - h.historical_dd) as dd_change,
                ((c.current_pd - h.historical_pd) * 10000) as pd_change_bps
            FROM current_dd c
            LEFT JOIN historical_dd h ON c.ticker = h.ticker
            WHERE h.historical_dd IS NOT NULL
            AND ABS(c.current_dd - h.historical_dd) >= {min_dd_change}
            ORDER BY ABS(c.current_dd - h.historical_dd) DESC
        """
        
        df = pd.read_sql(query, ENGINE)
        
        if df.empty:
            return jsonify({
                'count': 0,
                'signals': [],
                'message': 'No significant DD changes detected'
            }), 200
        
        # Generate signals
        signals = []
        for _, row in df.iterrows():
            dd_change = row['dd_change']
            
            # Determine action
            if dd_change < -min_dd_change:
                action = 'LONG_PROTECTION'  # DD falling = credit deteriorating
                reason = 'Credit quality deteriorating'
            elif dd_change > min_dd_change:
                action = 'SHORT_PROTECTION'  # DD rising = credit improving
                reason = 'Credit quality improving'
            else:
                continue
            
            # Calculate signal strength (0-1)
            signal_strength = min(abs(dd_change) / 5.0, 1.0)
            
            signals.append({
                'ticker': row['ticker'],
                'action': action,
                'reason': reason,
                'current_dd': float(row['current_dd']),
                'previous_dd': float(row['historical_dd']),
                'dd_change': float(dd_change),
                'pd_change_bps': float(row['pd_change_bps']),
                'signal_strength': float(signal_strength)
            })
        
        return jsonify({
            'count': len(signals),
            'lookback_days': lookback_days,
            'signals': signals
        }), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/universe', methods=['GET'])
def get_universe():
    """
    Get list of all tracked companies.
    
    Response:
        {
            "count": 5,
            "tickers": ["AAPL", "MSFT", "TSLA", "JPM", "XOM"],
            "summary": {...}
        }
    """
    try:
        query = """
            SELECT DISTINCT
                ticker,
                COUNT(*) as days_tracked,
                MAX(date) as latest_date,
                AVG(distance_to_default) as avg_dd,
                AVG(probability_default) as avg_pd
            FROM merton_outputs
            GROUP BY ticker
            ORDER BY ticker
        """
        
        df = pd.read_sql(query, ENGINE)
        
        data = df.to_dict(orient='records')
        for record in data:
            record['latest_date'] = record['latest_date'].isoformat()
        
        return jsonify({
            'count': len(data),
            'tickers': df['ticker'].tolist(),
            'summary': data
        }), 200
        
    except Exception as e:
        return jsonify({'error': str(e)}), 500


# ============================================================
# MAIN
# ============================================================

if __name__ == '__main__':
    print("=" * 60)
    print("MERTON MODEL API")
    print("=" * 60)
    print("\nEndpoints:")
    print("  GET  /api/health")
    print("  GET  /api/pd/{ticker}")
    print("  GET  /api/pd/{ticker}/history")
    print("  POST /api/pd/batch")
    print("  GET  /api/signals")
    print("  GET  /api/universe")
    print("\nStarting server on http://localhost:5000")
    print("=" * 60)
    
    app.run(host='0.0.0.0', port=5000, debug=True)