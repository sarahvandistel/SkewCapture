"""
Main runner for scheduling and orchestrating SkewCapture.
"""

import argparse
import os
import pandas as pd
import schedule
import time
from datetime import date, datetime
from typing import Optional
from pathlib import Path

try:
    from .config import Config
    from .data_fetcher import DataFetcher
    from .signal_logger import SignalLogger
    from .analyzer import Analyzer
except ImportError:
    # When running as script
    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).parent.parent))
    from skewcapture.config import Config
    from skewcapture.data_fetcher import DataFetcher
    from skewcapture.signal_logger import SignalLogger
    from skewcapture.analyzer import Analyzer


def run_pipeline(date_str):
    """
    Orchestrates the daily SkewCapture tasks:
    1. Logs raw signals
    2. Fetches price history
    3. Enriches with realized vol & momentum
    4. Saves enriched snapshot
    """
    # Load configuration
    config = Config()
    cfg = config.config

    print(f"Starting SkewCapture pipeline for {date_str}")
    
    # 1. Log raw signals
    print("Step 1: Logging raw signals...")
    sig_logger = SignalLogger(cfg)
    signals_df = sig_logger.log(date_str)

    # 2. Load today's signals from the log
    print("Step 2: Loading today's signals...")
    log_file = Path(cfg.get('signal_log_csv', 'data/raw/all_signals_log.csv'))
    if log_file.exists():
        all_signals_df = pd.read_csv(log_file, parse_dates=['run_date'])
        todays_signals = all_signals_df[all_signals_df['run_date'] == pd.to_datetime(date_str)]
        print(f"Found {len(todays_signals)} signals for {date_str}")
    else:
        print(f"Signal log not found: {log_file}")
        todays_signals = signals_df  # Use the signals we just logged

    # 3. Fetch price history
    print("Step 3: Fetching price history...")
    fetcher = DataFetcher(cfg)
    tickers = todays_signals['Symbol'].dropna().unique().tolist()
    print(f"Fetching price data for {len(tickers)} tickers...")
    
    # For now, use existing price file if available
    price_file = Path('data/raw/price_history_2025-08-04.csv')
    if price_file.exists():
        price_df = pd.read_csv(price_file)
        price_df['date'] = pd.to_datetime(price_df['date'])
        print(f"Loaded existing price data: {len(price_df)} records")
    else:
        # TODO: Implement actual price fetching
        print("No existing price data found. Price fetching not yet implemented.")
        price_df = pd.DataFrame()
    
    fetcher.disconnect()

    # 4. Enrich signals with analytics
    print("Step 4: Enriching signals with analytics...")
    analyzer = Analyzer(cfg)
    if not price_df.empty:
        enriched = analyzer.merge_signals(todays_signals, price_df)
        print(f"Enriched {len(enriched)} signals with realized vol and momentum metrics")
    else:
        enriched = todays_signals
        print("No price data available, using raw signals")

    # 5. Save enriched signals
    print("Step 5: Saving enriched signals...")
    out_dir = Path(cfg.get('data.processed_dir', 'data/processed'))
    out_dir.mkdir(parents=True, exist_ok=True)
    out_file = out_dir / f"enriched_signals_{date_str.replace('-', '')}.csv"
    enriched.to_csv(out_file, index=False)
    print(f"Enriched signals saved to {out_file}")
    
    print(f"✅ SkewCapture pipeline completed for {date_str}")


class Runner:
    """Main orchestrator for SkewCapture."""
    
    def __init__(self, config_path: Optional[str] = None):
        self.config = Config(config_path)
        self.data_fetcher = DataFetcher(self.config.config)
        self.signal_logger = SignalLogger(self.config.config)
        self.analyzer = Analyzer(self.config.config)
    
    def run_daily_snapshot(self, target_date: Optional[date] = None):
        """Run daily signal snapshot with analysis."""
        if target_date is None:
            target_date = date.today()
        
        date_str = target_date.strftime('%Y-%m-%d')
        run_pipeline(date_str)
    
    def schedule_daily_run(self):
        """Schedule daily run at configured time."""
        snapshot_time = self.config.get('signals.snapshot_time', '03:53')
        schedule.every().day.at(snapshot_time).do(self.run_daily_snapshot)
        
        print(f"Scheduled daily run at {snapshot_time}")
        
        while True:
            schedule.run_pending()
            time.sleep(60)
    
    def run_once(self, target_date: Optional[date] = None):
        """Run once for a specific date."""
        self.run_daily_snapshot(target_date)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Run SkewCapture daily pipeline'
    )
    parser.add_argument(
        '--date', required=True,
        help='Snapshot date in YYYY-MM-DD format'
    )
    args = parser.parse_args()
    run_pipeline(args.date) 