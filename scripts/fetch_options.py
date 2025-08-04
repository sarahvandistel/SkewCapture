#!/usr/bin/env python3
"""
Script to fetch options data from Databento for filtered tickers.
"""

import argparse
import sys
import pandas as pd
from pathlib import Path
from datetime import datetime

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from skewcapture.options_fetcher import OptionsFetcher
from skewcapture.config import Config


def main():
    """Main CLI entry point."""
    parser = argparse.ArgumentParser(
        description='Fetch options data from Databento for filtered tickers'
    )
    parser.add_argument(
        '--date', required=True,
        help='Date in YYYY-MM-DD format'
    )
    parser.add_argument(
        '--enriched-signals', 
        default='data/processed/enriched_signals_20250804.csv',
        help='Path to enriched signals CSV file'
    )
    parser.add_argument(
        '--max-symbols', type=int, default=10,
        help='Maximum number of symbols to fetch options for (default: 10)'
    )
    
    args = parser.parse_args()
    
    # Load configuration
    config = Config()
    cfg = config.config
    
    # Load enriched signals
    signals_file = Path(args.enriched_signals)
    if not signals_file.exists():
        print(f"Enriched signals file not found: {signals_file}")
        sys.exit(1)
    
    print(f"Loading enriched signals from {signals_file}")
    signals_df = pd.read_csv(signals_file)
    
    # Get unique symbols
    symbols = signals_df['Symbol'].dropna().unique().tolist()
    print(f"Found {len(symbols)} unique symbols")
    
    # Limit to max symbols for testing
    if len(symbols) > args.max_symbols:
        symbols = symbols[:args.max_symbols]
        print(f"Limited to first {len(symbols)} symbols for testing")
    
    print(f"Symbols to fetch options for: {symbols}")
    
    # Parse date
    target_date = datetime.strptime(args.date, '%Y-%m-%d').date()
    
    # Initialize options fetcher
    try:
        fetcher = OptionsFetcher(cfg)
        
        # Fetch options data
        print(f"Fetching options data for {len(symbols)} symbols on {target_date}")
        results = fetcher.fetch_options_data(symbols, target_date)
        
        # Print summary
        definitions_df = results['definitions']
        chain_df = results['chain_data']
        
        print(f"\n✅ Options data fetch completed:")
        print(f"   - Option definitions: {len(definitions_df)} records")
        print(f"   - Option chain data: {len(chain_df)} records")
        
        if not definitions_df.empty:
            print(f"   - Unique underlyings in definitions: {definitions_df['underlying'].nunique()}")
            if 'ts_recv' in definitions_df.columns:
                print(f"   - Date range: {definitions_df['ts_recv'].min()} to {definitions_df['ts_recv'].max()}")
        
        if not chain_df.empty:
            print(f"   - Unique underlyings in chain data: {chain_df['underlying'].nunique()}")
            if 'ts_recv' in chain_df.columns:
                print(f"   - Date range: {chain_df['ts_recv'].min()} to {chain_df['ts_recv'].max()}")
        
        fetcher.close()
        
    except Exception as e:
        print(f"Error fetching options data: {e}")
        sys.exit(1)


if __name__ == '__main__':
    main() 