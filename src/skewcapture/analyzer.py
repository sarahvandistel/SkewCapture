"""
Analysis functions for computing realized volatility and momentum.
"""

import pandas as pd
import numpy as np
from typing import Dict, List
from datetime import date


class Analyzer:
    """
    Computes realized volatility and momentum for symbols,
    and merges these metrics into the signal snapshots.
    """
    
    def __init__(self, cfg):
        self.realized_windows = cfg.get('realized_vol_windows', [10, 20, 60])
        self.momentum_windows = cfg.get('momentum_windows', [10, 30, 60])

    def compute_realized_vol(self, price_df):
        """
        Calculate rolling realized volatility for each symbol and window.

        Args:
            price_df (DataFrame): [date, symbol, close]
        Returns:
            DataFrame: [date, symbol, rv_{n} for each window]
        """
        df = price_df.copy()
        df = df.sort_values(['symbol', 'date'])
        
        # Compute log returns
        df['log_ret'] = df.groupby('symbol')['close'].pct_change().apply(lambda r: np.log(1 + r) if pd.notna(r) else np.nan)
        
        rv_frames = []
        for window in self.realized_windows:
            col = f'rv_{window}'
            # annualize: sqrt(252) * std of log returns
            df[col] = df.groupby('symbol')['log_ret'].rolling(window).std().reset_index(level=0, drop=True) * (252**0.5)
            rv_frames.append(df[['date', 'symbol', col]])
        
        # Merge all windows
        if rv_frames:
            rv_df = rv_frames[0]
            for other in rv_frames[1:]:
                rv_df = rv_df.merge(other, on=['date', 'symbol'])
            return rv_df
        else:
            return pd.DataFrame()

    def compute_momentum(self, price_df):
        """
        Calculate total-return momentum for each symbol and window.

        Args:
            price_df (DataFrame): [date, symbol, close]
        Returns:
            DataFrame: [date, symbol, mom_{n} for each window]
        """
        df = price_df.copy()
        df = df.sort_values(['symbol', 'date'])
        
        mom_frames = []
        for window in self.momentum_windows:
            col = f'mom_{window}'
            df[col] = df.groupby('symbol')['close'].pct_change(periods=window)
            mom_frames.append(df[['date', 'symbol', col]])
        
        # Merge all windows
        if mom_frames:
            mom_df = mom_frames[0]
            for other in mom_frames[1:]:
                mom_df = mom_df.merge(other, on=['date', 'symbol'])
            return mom_df
        else:
            return pd.DataFrame()

    def merge_signals(self, signals_df, price_df):
        """
        Enrich today's signals with realized vol and momentum metrics.

        Args:
            signals_df (DataFrame): [run_date, Symbol, ...]
            price_df (DataFrame): historical prices [date, symbol, close]
        Returns:
            DataFrame: signals_df with appended metrics for each run_date
        """
        if signals_df.empty or price_df.empty:
            return signals_df
            
        date = signals_df['run_date'].iloc[0]
        
        # Filter price history up to the run_date
        # Handle both 'symbol' and 'Symbol' column names
        symbol_col = 'symbol' if 'symbol' in price_df.columns else 'Symbol'
        hist = price_df[price_df['date'] <= date][['date', symbol_col, 'close']]
        hist = hist.rename(columns={symbol_col: 'symbol'})
        
        if hist.empty:
            print(f"No price history found for date {date}")
            return signals_df
        
        # Compute metrics
        rv = self.compute_realized_vol(hist)
        mom = self.compute_momentum(hist)
        
        # Get only metrics at run_date (or latest available date if run_date not found)
        available_dates = rv['date'].unique() if not rv.empty else []
        if available_dates:
            latest_date = max(available_dates)
            if latest_date != date:
                print(f"Using metrics from {latest_date} (run_date {date} not found)")
            rv_today = rv[rv['date'] == latest_date].drop(columns=['date']) if not rv.empty else pd.DataFrame()
            mom_today = mom[mom['date'] == latest_date].drop(columns=['date']) if not mom.empty else pd.DataFrame()
        else:
            rv_today = pd.DataFrame()
            mom_today = pd.DataFrame()
        
        # Merge into signals
        merged = signals_df.copy()
        
        if not rv_today.empty:
            merged = merged.merge(rv_today, left_on='Symbol', right_on='symbol', how='left')
            merged = merged.drop(columns=['symbol'])
        
        if not mom_today.empty:
            merged = merged.merge(mom_today, left_on='Symbol', right_on='symbol', how='left', suffixes=('', '_mom'))
            merged = merged.drop(columns=['symbol'])
        
        return merged 