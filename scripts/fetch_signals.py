#!/usr/bin/env python3
"""
CLI script for fetching daily signals.
"""

import argparse
import sys
from datetime import date
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from skewcapture.runner import Runner


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(description='Fetch daily signals for SkewCapture')
    parser.add_argument('--date', type=str, help='Target date (YYYY-MM-DD)')
    parser.add_argument('--config', type=str, help='Path to config file')
    parser.add_argument('--schedule', action='store_true', help='Run in scheduled mode')
    
    args = parser.parse_args()
    
    # Parse date if provided
    target_date = None
    if args.date:
        try:
            target_date = date.fromisoformat(args.date)
        except ValueError:
            print(f"Invalid date format: {args.date}. Use YYYY-MM-DD")
            sys.exit(1)
    
    # Initialize runner
    runner = Runner(args.config)
    
    if args.schedule:
        print("Starting scheduled mode...")
        runner.schedule_daily_run()
    else:
        print(f"Running one-time fetch for {target_date or 'today'}")
        runner.run_once(target_date)


if __name__ == '__main__':
    main() 