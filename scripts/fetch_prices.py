import argparse
import yaml
import pandas as pd
import sys
from pathlib import Path

# Add src to path
sys.path.insert(0, str(Path(__file__).parent.parent / 'src'))

from skewcapture.data_fetcher import DataFetcher

def main(date, tickers_csv, output_path):
    # Load configuration
    with open('config/config.yml') as f:
        cfg = yaml.safe_load(f)

    # Read tickers from CSV
    tickers_df = pd.read_csv(tickers_csv)
    tickers = tickers_df['Symbol'].dropna().unique().tolist()

    # Fetch historical prices
    fetcher = DataFetcher(cfg)
    price_data = fetcher.fetch_price_data(tickers)
    fetcher.disconnect()

    # Save to CSV
    out_file = f"{output_path}/price_history_{date}.csv"
    price_data.to_csv(out_file, index=False)
    print(f"Saved price data for {len(tickers)} symbols to {out_file}")

if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Fetch historical prices for ticker list')
    parser.add_argument('--date', required=True, help='Snapshot date YYYY-MM-DD')
    parser.add_argument('--tickers-csv', default='data/raw/signals_{date}.csv',
                        help='Path to CSV with Symbol column')
    parser.add_argument('--output-path', default='data/raw',
                        help='Directory to save price history')
    args = parser.parse_args()
    main(args.date, args.tickers_csv, args.output_path)