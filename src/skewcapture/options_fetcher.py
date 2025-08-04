"""
Options data fetching from Databento for SkewCapture.
"""

import databento as db
import pandas as pd
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import List, Dict, Optional
import logging

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class OptionsFetcher:
    """
    Fetches options data from Databento for given underlying symbols.
    """
    
    def __init__(self, cfg):
        self.cfg = cfg
        # Handle both nested and flat config structures
        if 'databento' in cfg and 'api_key' in cfg['databento']:
            self.api_key = cfg['databento']['api_key']
        else:
            self.api_key = cfg.get('databento.api_key')
        
        if not self.api_key:
            raise ValueError("Databento API key not found in config")
        
        self.client = db.Historical(self.api_key)
        self.output_dir = Path(cfg.get('data.output_dir', 'data/raw'))
        self.output_dir.mkdir(parents=True, exist_ok=True)
    
    def get_option_definitions(self, symbols: List[str], target_date: date) -> pd.DataFrame:
        """
        Get option definitions for given underlying symbols.
        
        Args:
            symbols: List of underlying symbols (e.g., ['AAPL', 'MSFT'])
            target_date: Date to fetch definitions for
            
        Returns:
            DataFrame with option definitions
        """
        logger.info(f"Fetching option definitions for {len(symbols)} symbols on {target_date}")
        
        # Convert symbols to parent format for Databento
        parent_symbols = [f"{symbol}.OPT" for symbol in symbols]
        
        try:
            # Get definition data for the target date
            data = self.client.timeseries.get_range(
                dataset="OPRA.PILLAR",
                schema="definition",
                stype_in="parent",
                symbols=parent_symbols,
                start=target_date.strftime('%Y-%m-%d'),
                end=(target_date + timedelta(days=1)).strftime('%Y-%m-%d'),
            )
            
            df = data.to_df()
            
            if df.empty:
                logger.warning(f"No option definitions found for {symbols} on {target_date}")
                return pd.DataFrame()
            
            # Filter for options only (exclude the underlying stock entries)
            df = df[df['strike_price'].notna()]
            
            # Add underlying symbol column
            df['underlying'] = df['symbol'].str.split().str[0]
            
            logger.info(f"Retrieved {len(df)} option definitions")
            return df
            
        except Exception as e:
            logger.error(f"Error fetching option definitions: {e}")
            return pd.DataFrame()
    
    def get_option_chain_data(self, symbols: List[str], target_date: date, 
                             schema: str = "cbbo-1m") -> pd.DataFrame:
        """
        Get option chain CBBO-1m data at market close (8:00 PM UTC) for given underlying symbols.
        
        Args:
            symbols: List of underlying symbols
            target_date: Date to fetch data for
            schema: Databento schema (default: "cbbo-1m" for 1-minute consolidated BBO)
            
        Returns:
            DataFrame with option chain CBBO data at market close
        """
        logger.info(f"Fetching option chain CBBO-1m data for {len(symbols)} symbols on {target_date}")
        
        # Convert symbols to parent format
        parent_symbols = [f"{symbol}.OPT" for symbol in symbols]
        
        try:
            # Get option chain CBBO data at market close (8:00 PM UTC = 4:00 PM ET)
            # We'll get a 1-minute window around 8:00 PM UTC
            data = self.client.timeseries.get_range(
                dataset="OPRA.PILLAR",
                schema=schema,
                stype_in="parent",
                symbols=parent_symbols,
                start=f"{target_date.strftime('%Y-%m-%d')}T20:00:00Z",
                end=f"{target_date.strftime('%Y-%m-%d')}T20:01:00Z",
            )
            
            df = data.to_df()
            
            if df.empty:
                logger.warning(f"No option chain CBBO data found for {symbols} on {target_date}")
                return pd.DataFrame()
            
            # Add underlying symbol column
            df['underlying'] = df['symbol'].str.split().str[0]
            
            logger.info(f"Retrieved {len(df)} option chain CBBO records")
            return df
            
        except Exception as e:
            logger.error(f"Error fetching option chain CBBO data: {e}")
            return pd.DataFrame()
    
    def fetch_options_data(self, symbols: List[str], target_date: date) -> Dict[str, pd.DataFrame]:
        """
        Fetch both option definitions and chain data for given symbols.
        
        Args:
            symbols: List of underlying symbols
            target_date: Date to fetch data for
            
        Returns:
            Dictionary with 'definitions' and 'chain_data' DataFrames
        """
        logger.info(f"Starting options data fetch for {len(symbols)} symbols on {target_date}")
        
        # Get option definitions
        definitions_df = self.get_option_definitions(symbols, target_date)
        
        # Get option chain data
        chain_df = self.get_option_chain_data(symbols, target_date)
        
        # Save to files
        date_str = target_date.strftime('%Y%m%d')
        
        if not definitions_df.empty:
            definitions_file = self.output_dir / f"option_definitions_{date_str}.csv"
            definitions_df.to_csv(definitions_file, index=False)
            logger.info(f"Saved option definitions to {definitions_file}")
        
        if not chain_df.empty:
            chain_file = self.output_dir / f"option_chain_data_{date_str}.csv"
            chain_df.to_csv(chain_file, index=False)
            logger.info(f"Saved option chain data to {chain_file}")
        
        return {
            'definitions': definitions_df,
            'chain_data': chain_df,
            'date': target_date
        }
    
    def close(self):
        """Close the Databento client."""
        if hasattr(self.client, 'close'):
            self.client.close() 