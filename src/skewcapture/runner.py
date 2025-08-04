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


class Runner:
    """Main orchestrator for SkewCapture."""
    
    def __init__(self, config_path: Optional[str] = None):
        self.config = Config(config_path)
        self.data_fetcher = DataFetcher(self.config)
        self.signal_logger = SignalLogger(self.config)
        self.analyzer = Analyzer(self.config)
    
    def run_daily_snapshot(self, target_date: Optional[date] = None):
        """Run daily signal snapshot."""
        if target_date is None:
            target_date = date.today()
        
        # TODO: Implement daily snapshot logic
        print(f"Running daily snapshot for {target_date}")
    
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