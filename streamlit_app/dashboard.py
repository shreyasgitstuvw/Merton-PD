"""
streamlit_app/dashboard.py

Interactive Streamlit dashboard for Merton model credit risk analysis.

Usage:
    streamlit run streamlit_app/dashboard.py

Features:
    - Search any company by ticker
    - View PD/DD trends over time
    - Compare multiple companies
    - CDS trading signals
    - Stress test scenarios
"""

import sys
from pathlib import Path
from datetime import datetime, timedelta

# Add project root to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

import streamlit as st
import pandas as pd
import plotly.graph_objects as go
import plotly.express as px
from src.db.engine import ENGINE

# ============================================================
# PAGE CONFIG
# ============================================================

st.set_page_config(
    page_title="Merton Credit Risk Dashboard",
    page_icon="📊",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ============================================================
# HELPER FUNCTIONS
# ============================================================

@st.cache_data(ttl=300)  # Cache for 5 minutes
def get_available_tickers():
    """Get list of all tickers in the database."""
    query = """
        SELECT DISTINCT ticker
        FROM merton_outputs
        ORDER BY ticker
    """
    df = pd.read_sql(query, ENGINE)
    return df['ticker'].tolist()


@st.cache_data(ttl=60)
def get_latest_pd(ticker: str):
    """Get latest PD/DD for a ticker."""
    query = f"""
        SELECT *
        FROM merton_outputs
        WHERE ticker = '{ticker}'
        ORDER BY date DESC
        LIMIT 1
    """
    df = pd.read_sql(query, ENGINE)
    return df.iloc[0] if not df.empty else None


@st.cache_data(ttl=60)
def get_pd_history(ticker: str, days: int = 180):
    """Get historical PD/DD data."""
    cutoff_date = (datetime.now() - timedelta(days=days)).date()
    query = f"""
        SELECT 
            date,
            distance_to_default,
            probability_default,
            asset_volatility,
            leverage_ratio
        FROM merton_outputs
        WHERE ticker = '{ticker}'
        AND date >= '{cutoff_date}'
        ORDER BY date
    """
    return pd.read_sql(query, ENGINE)


@st.cache_data(ttl=60)
def get_trading_signals(lookback_days: int = 30, min_dd_change: float = 1.0):
    """Get CDS trading signals."""
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
                distance_to_default as historical_dd
            FROM merton_outputs
            WHERE date <= CURRENT_DATE - INTERVAL '{lookback_days} days'
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
        AND ABS(c.current_dd - h.historical_dd) >= {min_dd_change}
        ORDER BY ABS(c.current_dd - h.historical_dd) DESC
    """
    return pd.read_sql(query, ENGINE)


def dd_to_rating(dd: float) -> str:
    """Convert DD to credit rating."""
    if dd > 10:
        return "AAA"
    elif dd > 8:
        return "AA"
    elif dd > 6:
        return "A"
    elif dd > 4:
        return "BBB"
    elif dd > 2:
        return "BB"
    else:
        return "B/CCC"


def pd_to_bps(pd_value: float) -> float:
    """Convert PD to basis points."""
    return pd_value * 10000


# ============================================================
# SIDEBAR
# ============================================================

st.sidebar.title("📊 Merton Credit Risk")
st.sidebar.markdown("---")

page = st.sidebar.radio(
    "Navigation",
    ["🏠 Home", "🔍 Company Analysis", "📈 Portfolio View", "⚡ Trading Signals", "🧪 Stress Testing"]
)

st.sidebar.markdown("---")
st.sidebar.info("""
**About This Dashboard**

This tool provides real-time credit risk analysis using the Merton structural model.

**Key Metrics:**
- **DD (Distance to Default)**: How far the company is from default (in std devs)
- **PD (Probability of Default)**: Likelihood of default
- **Credit Rating**: Implied rating from DD
""")

# ============================================================
# PAGE: HOME
# ============================================================

if page == "🏠 Home":
    st.title("Merton Credit Risk Dashboard")
    st.markdown("### Real-time Credit Analysis for Your Portfolio")
    
    # Get summary statistics
    tickers = get_available_tickers()
    
    col1, col2, col3 = st.columns(3)
    
    with col1:
        st.metric("Companies Tracked", len(tickers))
    
    with col2:
        # Get latest update time
        query = "SELECT MAX(created_at) FROM merton_outputs"
        last_update = pd.read_sql(query, ENGINE).iloc[0, 0]
        st.metric("Last Updated", last_update.strftime("%Y-%m-%d %H:%M"))
    
    with col3:
        # Count signals
        signals = get_trading_signals()
        st.metric("Active Signals", len(signals))
    
    st.markdown("---")
    
    # Show overview table
    st.subheader("Portfolio Overview")
    
    query = """
        WITH latest AS (
            SELECT DISTINCT ON (ticker)
                ticker,
                distance_to_default,
                probability_default,
                leverage_ratio,
                date
            FROM merton_outputs
            ORDER BY ticker, date DESC
        )
        SELECT * FROM latest ORDER BY ticker
    """
    df = pd.read_sql(query, ENGINE)
    
    # Add rating column
    df['rating'] = df['distance_to_default'].apply(dd_to_rating)
    df['pd_bps'] = df['probability_default'].apply(pd_to_bps)
    
    # Format display
    display_df = df[['ticker', 'rating', 'distance_to_default', 'pd_bps', 'leverage_ratio', 'date']].copy()
    display_df.columns = ['Ticker', 'Rating', 'DD', 'PD (bps)', 'Leverage', 'Date']
    
    st.dataframe(
        display_df.style.format({
            'DD': '{:.2f}',
            'PD (bps)': '{:.4f}',
            'Leverage': '{:.2%}'
        }),
        use_container_width=True
    )

# ============================================================
# PAGE: COMPANY ANALYSIS
# ============================================================

elif page == "🔍 Company Analysis":
    st.title("Company Credit Analysis")
    
    tickers = get_available_tickers()
    
    # Ticker selection
    col1, col2 = st.columns([3, 1])
    with col1:
        selected_ticker = st.selectbox(
            "Select Company",
            options=tickers,
            index=0 if tickers else None
        )
    with col2:
        lookback = st.selectbox(
            "Time Period",
            options=[30, 90, 180, 365],
            format_func=lambda x: f"{x} days",
            index=2
        )
    
    if selected_ticker:
        # Get latest data
        latest = get_latest_pd(selected_ticker)
        
        if latest is not None:
            # Display current metrics
            st.markdown(f"### {selected_ticker} - Current Credit Metrics")
            
            col1, col2, col3, col4 = st.columns(4)
            
            with col1:
                rating = dd_to_rating(latest['distance_to_default'])
                st.metric("Credit Rating", rating)
            
            with col2:
                st.metric(
                    "Distance to Default",
                    f"{latest['distance_to_default']:.2f}σ"
                )
            
            with col3:
                pd_bps = pd_to_bps(latest['probability_default'])
                st.metric("PD (basis points)", f"{pd_bps:.4f}")
            
            with col4:
                st.metric(
                    "Leverage",
                    f"{latest['leverage_ratio']:.1%}"
                )
            
            st.markdown("---")
            
            # Get historical data
            hist_df = get_pd_history(selected_ticker, lookback)
            
            if not hist_df.empty:
                # Plot DD over time
                st.subheader("Distance to Default Trend")
                
                fig = go.Figure()
                fig.add_trace(go.Scatter(
                    x=hist_df['date'],
                    y=hist_df['distance_to_default'],
                    mode='lines',
                    name='DD',
                    line=dict(color='#1f77b4', width=2)
                ))
                
                # Add rating thresholds
                fig.add_hline(y=10, line_dash="dash", line_color="green", annotation_text="AAA")
                fig.add_hline(y=8, line_dash="dash", line_color="lightgreen", annotation_text="AA")
                fig.add_hline(y=6, line_dash="dash", line_color="yellow", annotation_text="A")
                fig.add_hline(y=4, line_dash="dash", line_color="orange", annotation_text="BBB")
                fig.add_hline(y=2, line_dash="dash", line_color="red", annotation_text="BB")
                
                fig.update_layout(
                    xaxis_title="Date",
                    yaxis_title="Distance to Default (σ)",
                    hovermode='x unified',
                    height=400
                )
                
                st.plotly_chart(fig, use_container_width=True)
                
                # Plot PD over time
                st.subheader("Probability of Default Trend")
                
                fig2 = go.Figure()
                fig2.add_trace(go.Scatter(
                    x=hist_df['date'],
                    y=hist_df['probability_default'] * 10000,  # Convert to bps
                    mode='lines',
                    name='PD',
                    line=dict(color='#ff7f0e', width=2),
                    fill='tozeroy'
                ))
                
                fig2.update_layout(
                    xaxis_title="Date",
                    yaxis_title="PD (basis points)",
                    hovermode='x unified',
                    height=400
                )
                
                st.plotly_chart(fig2, use_container_width=True)
                
                # Statistics
                st.subheader("Summary Statistics")
                col1, col2 = st.columns(2)
                
                with col1:
                    st.markdown(f"""
                    **Distance to Default:**
                    - Average: {hist_df['distance_to_default'].mean():.2f}σ
                    - Min: {hist_df['distance_to_default'].min():.2f}σ
                    - Max: {hist_df['distance_to_default'].max():.2f}σ
                    - Std Dev: {hist_df['distance_to_default'].std():.2f}σ
                    """)
                
                with col2:
                    avg_pd_bps = hist_df['probability_default'].mean() * 10000
                    st.markdown(f"""
                    **Probability of Default:**
                    - Average: {avg_pd_bps:.4f} bps
                    - Current: {pd_bps:.4f} bps
                    - Volatility: {hist_df['asset_volatility'].mean():.2%}
                    - Leverage: {hist_df['leverage_ratio'].mean():.2%}
                    """)

# ============================================================
# PAGE: PORTFOLIO VIEW
# ============================================================

elif page == "📈 Portfolio View":
    st.title("Portfolio Comparison")
    
    tickers = get_available_tickers()
    
    # Multi-select
    selected_tickers = st.multiselect(
        "Select Companies to Compare",
        options=tickers,
        default=tickers[:3] if len(tickers) >= 3 else tickers
    )
    
    if selected_tickers:
        # Get latest data for all selected tickers
        tickers_str = "','".join(selected_tickers)
        query = f"""
            WITH latest AS (
                SELECT DISTINCT ON (ticker)
                    ticker,
                    date,
                    distance_to_default,
                    probability_default,
                    leverage_ratio
                FROM merton_outputs
                WHERE ticker IN ('{tickers_str}')
                ORDER BY ticker, date DESC
            )
            SELECT * FROM latest
        """
        df = pd.read_sql(query, ENGINE)
        
        # Bar chart comparison
        st.subheader("Distance to Default Comparison")
        
        fig = px.bar(
            df,
            x='ticker',
            y='distance_to_default',
            color='distance_to_default',
            color_continuous_scale='RdYlGn',
            labels={'distance_to_default': 'DD (σ)'}
        )
        fig.update_layout(height=400)
        st.plotly_chart(fig, use_container_width=True)
        
        # Historical comparison
        st.subheader("DD Trend Comparison (90 days)")
        
        # Get historical data
        cutoff_date = (datetime.now() - timedelta(days=90)).date()
        query = f"""
            SELECT 
                ticker,
                date,
                distance_to_default
            FROM merton_outputs
            WHERE ticker IN ('{tickers_str}')
            AND date >= '{cutoff_date}'
            ORDER BY date
        """
        hist_df = pd.read_sql(query, ENGINE)
        
        fig = px.line(
            hist_df,
            x='date',
            y='distance_to_default',
            color='ticker',
            labels={'distance_to_default': 'DD (σ)'}
        )
        fig.update_layout(height=400, hovermode='x unified')
        st.plotly_chart(fig, use_container_width=True)

# ============================================================
# PAGE: TRADING SIGNALS
# ============================================================

elif page == "⚡ Trading Signals":
    st.title("CDS Trading Signals")
    st.markdown("Identify credit protection opportunities based on DD changes")
    
    col1, col2 = st.columns(2)
    with col1:
        lookback = st.slider("Lookback Period (days)", 7, 90, 30)
    with col2:
        min_change = st.slider("Minimum DD Change (σ)", 0.5, 3.0, 1.0, 0.1)
    
    signals = get_trading_signals(lookback, min_change)
    
    if not signals.empty:
        # Add signal classification
        signals['action'] = signals['dd_change'].apply(
            lambda x: 'LONG PROTECTION' if x < 0 else 'SHORT PROTECTION'
        )
        signals['strength'] = signals['dd_change'].abs().apply(
            lambda x: min(x / 5.0, 1.0)
        )
        
        st.subheader(f"Found {len(signals)} Trading Signals")
        
        # Display signals
        for _, signal in signals.iterrows():
            action = signal['action']
            color = "🔴" if action == 'LONG PROTECTION' else "🟢"
            
            with st.expander(f"{color} {signal['ticker']} - {action}"):
                col1, col2, col3 = st.columns(3)
                
                with col1:
                    st.metric("Current DD", f"{signal['current_dd']:.2f}σ")
                with col2:
                    st.metric(
                        "DD Change",
                        f"{signal['dd_change']:.2f}σ",
                        delta=f"{signal['dd_change']:.2f}σ"
                    )
                with col3:
                    strength_pct = signal['strength'] * 100
                    st.metric("Signal Strength", f"{strength_pct:.0f}%")
                
                if action == 'LONG PROTECTION':
                    st.warning(f"""
                    **Recommendation:** Consider buying credit protection (CDS)
                    
                    **Rationale:** Distance to default has declined from {signal['historical_dd']:.2f}σ to {signal['current_dd']:.2f}σ 
                    over the past {lookback} days, indicating deteriorating credit quality.
                    """)
                else:
                    st.success(f"""
                    **Recommendation:** Consider selling credit protection (CDS)
                    
                    **Rationale:** Distance to default has increased from {signal['historical_dd']:.2f}σ to {signal['current_dd']:.2f}σ 
                    over the past {lookback} days, indicating improving credit quality.
                    """)
    else:
        st.info("No significant DD changes detected in the current period.")

# ============================================================
# PAGE: STRESS TESTING
# ============================================================

elif page == "🧪 Stress Testing":
    st.title("Stress Testing")
    st.markdown("See how credit risk changes under adverse scenarios")
    
    tickers = get_available_tickers()
    selected_ticker = st.selectbox("Select Company", options=tickers)
    
    if selected_ticker:
        latest = get_latest_pd(selected_ticker)
        
        if latest is not None:
            st.markdown(f"### Base Case: {selected_ticker}")
            col1, col2, col3 = st.columns(3)
            with col1:
                st.metric("DD", f"{latest['distance_to_default']:.2f}σ")
            with col2:
                pd_bps = pd_to_bps(latest['probability_default'])
                st.metric("PD", f"{pd_bps:.4f} bps")
            with col3:
                st.metric("Rating", dd_to_rating(latest['distance_to_default']))
            
            st.markdown("---")
            st.subheader("Scenario Analysis")
            
            # Define scenarios
            scenarios = {
                "Mild Recession": {
                    "volatility_mult": 1.3,
                    "asset_shock": -0.10
                },
                "Severe Recession (GFC-like)": {
                    "volatility_mult": 1.8,
                    "asset_shock": -0.35
                },
                "Market Crash (COVID-like)": {
                    "volatility_mult": 2.0,
                    "asset_shock": -0.25
                },
                "Stagflation": {
                    "volatility_mult": 1.4,
                    "asset_shock": -0.15
                }
            }
            
            # Simulate scenarios
            results = []
            for scenario_name, params in scenarios.items():
                # Simplified stress calculation
                stressed_vol = latest['asset_volatility'] * params['volatility_mult']
                stressed_asset = latest['asset_value'] * (1 + params['asset_shock'])
                
                # Approximate stressed DD
                import numpy as np
                from scipy.stats import norm
                
                D = stressed_asset * latest['leverage_ratio']
                stressed_dd = (np.log(stressed_asset / D) + 0.04 - 0.5 * stressed_vol**2) / stressed_vol
                stressed_pd = norm.cdf(-stressed_dd)
                
                results.append({
                    'Scenario': scenario_name,
                    'DD': stressed_dd,
                    'PD (bps)': stressed_pd * 10000,
                    'Rating': dd_to_rating(stressed_dd),
                    'DD Change': stressed_dd - latest['distance_to_default']
                })
            
            results_df = pd.DataFrame(results)
            
            st.dataframe(
                results_df.style.format({
                    'DD': '{:.2f}',
                    'PD (bps)': '{:.2f}',
                    'DD Change': '{:.2f}'
                }),
                use_container_width=True
            )
            
            # Visualization
            fig = go.Figure()
            
            fig.add_trace(go.Bar(
                x=results_df['Scenario'],
                y=results_df['DD'],
                name='Stressed DD',
                marker_color='lightcoral'
            ))
            
            fig.add_hline(
                y=latest['distance_to_default'],
                line_dash="dash",
                line_color="green",
                annotation_text="Base Case"
            )
            
            fig.update_layout(
                title="Distance to Default Under Stress",
                yaxis_title="DD (σ)",
                height=400
            )
            
            st.plotly_chart(fig, use_container_width=True)

# ============================================================
# FOOTER
# ============================================================

st.sidebar.markdown("---")
st.sidebar.markdown("""
<small>
Merton Credit Risk Dashboard v1.0  
Last updated: 2026-01-18
</small>
""", unsafe_allow_html=True)