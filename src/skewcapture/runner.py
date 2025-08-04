"""
Main runner for scheduling and orchestrating SkewCapture.
"""

import schedule
import time
from datetime import date, datetime
from typing import Optional

from .config import Config
from .data_fetcher import DataFetcher
from .signal_logger import SignalLogger
from .analyzer import Analyzer
import pandas as pd
from pathlib import Path


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
        
        # Step 1: Log signals
        print(f"Logging signals for {date_str}...")
        signals_df = self.signal_logger.log(date_str)
        
        # Step 2: Load price history
        print("Loading price history...")
        price_file = Path('data/raw/price_history_2025-08-04.csv')  # For now, use existing file
        if price_file.exists():
            price_df = pd.read_csv(price_file)
            price_df['date'] = pd.to_datetime(price_df['date'])
            
            # Step 3: Analyze and enrich signals
            print("Computing realized vol and momentum metrics...")
            enriched_signals = self.analyzer.merge_signals(signals_df, price_df)
            
            # Step 4: Save enriched signals
            processed_dir = Path('data/processed')
            processed_dir.mkdir(exist_ok=True)
            
            output_file = processed_dir / f"enriched_signals_{date_str.replace('-', '')}.csv"
            enriched_signals.to_csv(output_file, index=False)
            print(f"Saved enriched signals to {output_file}")
            
            # Print summary
            print(f"Enriched {len(enriched_signals)} signals with realized vol and momentum metrics")
            
        else:
            print(f"Price history file not found: {price_file}")
        
        print(f"Completed daily snapshot for {target_date}")
    
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