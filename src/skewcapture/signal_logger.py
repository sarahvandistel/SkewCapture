"""
Signal logging functionality for daily snapshots.
"""

import os
import yaml
import pandas as pd
from datetime import datetime, date
from pathlib import Path
from typing import Dict, List


class SignalLogger:
    """
    Logs daily screener output into a rolling CSV for forward-testing.
    """
    
    def __init__(self, cfg):
        # cfg should include: raw_signals_dir, log_file_path
        self.raw_dir = Path(cfg.get('raw_signals_dir', 'data/raw'))
        self.log_path = Path(cfg.get('signal_log_csv', 'data/raw/all_signals_log.csv'))
        self.raw_dir.mkdir(parents=True, exist_ok=True)
        self.log_path.parent.mkdir(parents=True, exist_ok=True)

    def load_today_signals(self, date_str):
        """Read today's raw screener CSV."""
        # Try to find the Barchart CSV file for the given date
        date_obj = datetime.strptime(date_str, '%Y-%m-%d')
        barchart_filename = f"stocks-screener-skewcapture-screener-{date_obj.strftime('%m-%d-%Y')}.csv"
        barchart_path = Path('data/barchart') / barchart_filename
        
        if barchart_path.exists():
            print(f"Loading Barchart screener data from {barchart_path}")
            return pd.read_csv(barchart_path)
        else:
            # Fallback to signals file if Barchart file doesn't exist
            file_path = self.raw_dir / f"signals_{date_str.replace('-', '')}.csv"
            if file_path.exists():
                print(f"Loading signals data from {file_path}")
                return pd.read_csv(file_path)
            else:
                raise FileNotFoundError(f"No screener data found for {date_str}")

    def annotate_signals(self, df, date_str):
        """Add metadata (run_date, timestamp)."""
        df['run_date'] = pd.to_datetime(date_str)
        df['run_timestamp'] = datetime.utcnow()
        
        # Add any core metrics if they exist in the data
        # These columns might be present in Barchart data
        core_metrics = ['IV_short', 'IV_long', 'skew_z', 'momentum']
        for metric in core_metrics:
            if metric not in df.columns:
                df[metric] = None  # Placeholder for metrics not in current data
        
        return df

    def append_to_log(self, df):
        """Append today's annotated signals to the master log."""
        if not self.log_path.exists():
            df.to_csv(self.log_path, index=False)
            print(f"Created new signal log at {self.log_path}")
        else:
            df.to_csv(self.log_path, mode='a', header=False, index=False)
            print(f"Appended to existing signal log at {self.log_path}")

    def log(self, date_str):
        """Full pipeline: load, annotate, and append."""
        df = self.load_today_signals(date_str)
        df = self.annotate_signals(df, date_str)
        self.append_to_log(df)
        print(f"Logged {len(df)} signals for {date_str} to {self.log_path}")
        
        return df 