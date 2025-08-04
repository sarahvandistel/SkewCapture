#!/usr/bin/env python3
"""
CLI script for fetching daily signals.
"""

import argparse
import yaml
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from skewcapture.signal_logger import SignalLogger


def main(date):
    """Main CLI entry point."""
    # Load configuration
    with open('config/config.yml') as f:
        cfg = yaml.safe_load(f)

    # Initialize logger and process today's signals
    logger = SignalLogger(cfg)
    logger.log(date)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Process and log daily screener signals'
    )
    parser.add_argument(
        '--date', required=True,
        help='Snapshot date in YYYY-MM-DD format'
    )
    args = parser.parse_args()
    main(args.date) 