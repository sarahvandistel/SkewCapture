import pandas as pd
from ib_async import IB, Stock, util

class DataFetcher:
    """
    Fetches historical price data for underlyings using Interactive Brokers via ib_async.
    """
    def __init__(self, cfg):
        # cfg should include keys: ib_host, ib_port, ib_client_id, ib_exchange, ib_currency
        self.cfg = cfg
        self.ib = IB()
        self.ib.connect(
            cfg['ib_host'],
            cfg['ib_port'],
            clientId=cfg['ib_client_id']
        )

    def fetch_price_data(self, tickers, duration_str=None):
        """
        Fetch daily historical data for a list of tickers.

        Args:
            tickers (List[str]): List of stock symbols.
            duration_str (str): IB duration string, e.g., '3 Y'.
                                 If None, uses cfg['historical_duration'].

        Returns:
            pd.DataFrame: Combined DataFrame with columns [date, open, high, low, close, volume, average, symbol].
        """
        duration = duration_str or self.cfg.get('historical_duration', '3 Y')
        all_data = []

        for symbol in tickers:
            # Clean up symbol for IB compatibility
            clean_symbol = symbol.replace('.', ' ')  # Replace periods with spaces
            if (clean_symbol.strip() == '' or 
                'Downloaded from Barchart.com' in clean_symbol or
                'Downloaded from Barchart com' in clean_symbol):
                continue  # Skip empty symbols or footer lines
                
            contract = Stock(
                clean_symbol,
                self.cfg.get('ib_exchange', 'SMART'),
                self.cfg.get('ib_currency', 'USD')
            )
            try:
                bars = self.ib.reqHistoricalData(
                    contract,
                    endDateTime='',
                    durationStr=duration,
                    barSizeSetting='1 day',
                    whatToShow='TRADES',
                    useRTH=True
                )
                df = util.df(bars) if bars else pd.DataFrame()
                if df is not None and not df.empty:
                    df = df.rename(columns={
                        'date': 'date',
                        'open': 'open',
                        'high': 'high',
                        'low': 'low',
                        'close': 'close',
                        'volume': 'volume',
                        'average': 'average'
                    })
                    df['symbol'] = symbol
                    all_data.append(df)
            except Exception as e:
                print(f"Error fetching data for {symbol}: {e}")
                continue

        if all_data:
            result = pd.concat(all_data, ignore_index=True)
            result['date'] = pd.to_datetime(result['date'])
            return result
        else:
            return pd.DataFrame()

    def disconnect(self):
        """Disconnect from IB client."""
        self.ib.disconnect()