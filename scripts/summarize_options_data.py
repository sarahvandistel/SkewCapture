#!/usr/bin/env python3
"""
Script to summarize enriched options data.
"""

import pandas as pd
from pathlib import Path
import sys

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from skewcapture.options_fetcher import OptionsFetcher


def summarize_options_data():
    """Summarize the enriched options data."""
    
    # Check if enriched files exist
    chain_file = Path('data/raw/option_chain_data_20240809.csv')
    definitions_file = Path('data/raw/option_definitions_20240809.csv')
    
    print("📊 Options Data Summary")
    print("=" * 50)
    
    if chain_file.exists():
        print(f"\n🔗 Option Chain Data: {chain_file}")
        df_chain = pd.read_csv(chain_file)
        
        print(f"   - Total records: {len(df_chain):,}")
        print(f"   - Unique underlyings: {df_chain['underlying'].nunique()}")
        print(f"   - Call options: {len(df_chain[df_chain['option_type'] == 'C']):,}")
        print(f"   - Put options: {len(df_chain[df_chain['option_type'] == 'P']):,}")
        
        # Strike price statistics
        if 'strike_price_parsed' in df_chain.columns:
            print(f"   - Strike price range: ${df_chain['strike_price_parsed'].min():.1f} - ${df_chain['strike_price_parsed'].max():.1f}")
        
        # Days to expiry statistics
        if 'days_to_expiry' in df_chain.columns:
            print(f"   - Days to expiry range: {df_chain['days_to_expiry'].min()} - {df_chain['days_to_expiry'].max()} days")
        
        # Sample data
        print(f"\n   📋 Sample enriched records:")
        sample_cols = ['symbol', 'underlying', 'expiry_date', 'option_type', 'strike_price_parsed', 'days_to_expiry']
        available_cols = [col for col in sample_cols if col in df_chain.columns]
        print(df_chain[available_cols].head(3).to_string(index=False))
    
    if definitions_file.exists():
        print(f"\n📋 Option Definitions: {definitions_file}")
        df_def = pd.read_csv(definitions_file)
        
        print(f"   - Total records: {len(df_def):,}")
        print(f"   - Unique underlyings: {df_def['underlying'].nunique()}")
        print(f"   - Call options: {len(df_def[df_def['option_type'] == 'C']):,}")
        print(f"   - Put options: {len(df_def[df_def['option_type'] == 'P']):,}")
        
        # Sample data
        print(f"\n   📋 Sample definitions:")
        sample_cols = ['symbol', 'underlying', 'expiry_date', 'option_type', 'strike_price_parsed', 'days_to_expiry']
        available_cols = [col for col in sample_cols if col in df_def.columns]
        print(df_def[available_cols].head(3).to_string(index=False))
    
    print(f"\n✅ Enriched data ready for analysis!")


if __name__ == '__main__':
    summarize_options_data() 