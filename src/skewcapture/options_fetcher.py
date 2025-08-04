"""
Options data fetching from Databento for SkewCapture.
"""

import databento as db
import pandas as pd
from datetime import datetime, date, timedelta
from pathlib import Path
from typing import List, Dict, Optional
import logging
import re

# Set up logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class OptionsFetcher:
    """
    Fetches options data from Databento for given underlying symbols.
    """
    
    @staticmethod
    def parse_option_symbol(symbol: str) -> Dict[str, any]:
        """
        Parse option symbol to extract expiry date, option type, and strike price.
        
        Args:
            symbol: Option symbol (e.g., 'ALGN  240823C00165000')
            
        Returns:
            Dictionary with 'expiry_date', 'option_type', 'strike_price'
        """
        try:
            # Remove leading spaces and get the option part (after the underlying)
            # Format: UNDERLYING  YYMMDDTYPEXXXXXXX
            # Example: ALGN  240823C00165000 -> 240823C00165000
            
            # Find the option part (after the underlying ticker)
            option_part = symbol.strip()
            
            # Extract expiry date (YYMMDD format)
            expiry_str = option_part[6:12]  # YYMMDD
            year = 2000 + int(expiry_str[:2])  # Convert YY to YYYY
            month = int(expiry_str[2:4])
            day = int(expiry_str[4:6])
            expiry_date = date(year, month, day)
            
            # Extract option type (C or P)
            option_type = option_part[12]  # C or P
            
            # Extract strike price (multiplied by 1000)
            strike_str = option_part[13:]  # XXXXXXX
            strike_price = float(strike_str) / 1000.0  # Convert back to actual strike
            
            return {
                'expiry_date': expiry_date,
                'option_type': option_type,
                'strike_price': strike_price
            }
            
        except Exception as e:
            logger.warning(f"Could not parse option symbol '{symbol}': {e}")
            return {
                'expiry_date': None,
                'option_type': None,
                'strike_price': None
            }
    
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
    
    def enrich_options_data(self, df: pd.DataFrame, target_date: date) -> pd.DataFrame:
        """
        Enrich options data with parsed fields from symbol.
        
        Args:
            df: DataFrame with options data including 'symbol' column
            target_date: Reference date for calculating days to expiry
            
        Returns:
            DataFrame with additional columns: expiry_date, option_type, strike_price, days_to_expiry
        """
        if df.empty or 'symbol' not in df.columns:
            return df
        
        logger.info("Enriching options data with parsed fields...")
        
        # Parse each symbol
        parsed_data = []
        for symbol in df['symbol']:
            parsed = self.parse_option_symbol(symbol)
            parsed_data.append(parsed)
        
        # Create DataFrame from parsed data
        parsed_df = pd.DataFrame(parsed_data)
        
        # Add parsed columns to original DataFrame
        df = df.copy()  # Avoid SettingWithCopyWarning
        df['expiry_date'] = parsed_df['expiry_date']
        df['option_type'] = parsed_df['option_type']
        df['strike_price_parsed'] = parsed_df['strike_price']
        
        # Add days to expiry
        if 'expiry_date' in df.columns:
            # Convert to datetime for calculation
            df['expiry_date'] = pd.to_datetime(df['expiry_date'])
            target_datetime = pd.to_datetime(target_date)
            df['days_to_expiry'] = (df['expiry_date'] - target_datetime).dt.days
        
        logger.info(f"Enriched {len(df)} options records with parsed fields")
        return df
    
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
        
        # Enrich both DataFrames with parsed fields
        if not definitions_df.empty:
            definitions_df = self.enrich_options_data(definitions_df, target_date)
        
        if not chain_df.empty:
            chain_df = self.enrich_options_data(chain_df, target_date)
        
        # Save to files
        date_str = target_date.strftime('%Y%m%d')
        
        if not definitions_df.empty:
            definitions_file = self.output_dir / f"option_definitions_{date_str}.csv"
            definitions_df.to_csv(definitions_file, index=False)
            logger.info(f"Saved enriched option definitions to {definitions_file}")
        
        if not chain_df.empty:
            chain_file = self.output_dir / f"option_chain_data_{date_str}.csv"
            chain_df.to_csv(chain_file, index=False)
            logger.info(f"Saved enriched option chain data to {chain_file}")
        
        return {
            'definitions': definitions_df,
            'chain_data': chain_df,
            'date': target_date
        }
    
    def close(self):
        """Close the Databento client."""
        if hasattr(self.client, 'close'):
            self.client.close() 