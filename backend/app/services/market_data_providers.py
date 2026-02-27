"""
Market Data Provider implementations for factor proxy data.
Supports Stooq (primary), FRED (macro), and fallbacks.
"""
import pandas as pd
import numpy as np
import requests
from abc import ABC, abstractmethod
from typing import Optional, Dict, List, Tuple
from datetime import date, datetime, timedelta
from io import StringIO
import time
import logging

logger = logging.getLogger(__name__)


class MarketDataProvider(ABC):
    """Abstract base class for market data providers"""

    @abstractmethod
    def fetch_series(
        self,
        symbol: str,
        start_date: date,
        end_date: date
    ) -> Optional[pd.DataFrame]:
        """
        Fetch historical data for a symbol.
        Returns DataFrame with columns: date, close (or value), daily_return
        """
        pass

    @abstractmethod
    def get_source_name(self) -> str:
        """Return the source identifier"""
        pass


class StooqProvider(MarketDataProvider):
    """
    Fetch historical prices from Stooq.
    Free, no API key required.
    URL format: https://stooq.com/q/d/l/?s={symbol}.us&d1={start}&d2={end}
    """

    BASE_URL = "https://stooq.com/q/d/l/"
    MAX_RETRIES = 3
    RETRY_DELAY = 2  # seconds

    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })

    def get_source_name(self) -> str:
        return "stooq"

    def fetch_series(
        self,
        symbol: str,
        start_date: date,
        end_date: date
    ) -> Optional[pd.DataFrame]:
        """Fetch historical prices from Stooq"""
        # Format dates for Stooq API (YYYYMMDD)
        d1 = start_date.strftime('%Y%m%d')
        d2 = end_date.strftime('%Y%m%d')

        # Stooq uses .US suffix for US stocks/ETFs
        stooq_symbol = f"{symbol}.US"

        url = f"{self.BASE_URL}?s={stooq_symbol}&d1={d1}&d2={d2}"

        for attempt in range(self.MAX_RETRIES):
            try:
                response = self.session.get(url, timeout=30)
                response.raise_for_status()

                # Parse CSV response
                csv_data = response.text

                if not csv_data or 'No data' in csv_data or len(csv_data) < 50:
                    logger.warning(f"No data returned from Stooq for {symbol}")
                    return None

                df = pd.read_csv(StringIO(csv_data))

                # Stooq returns columns: Date,Open,High,Low,Close,Volume
                if 'Date' not in df.columns or 'Close' not in df.columns:
                    logger.warning(f"Unexpected Stooq response format for {symbol}")
                    return None

                # Rename and select columns
                df = df.rename(columns={'Date': 'date', 'Close': 'close'})
                df['date'] = pd.to_datetime(df['date']).dt.date
                df = df[['date', 'close']].sort_values('date')

                # Compute daily returns
                df['daily_return'] = df['close'].pct_change()

                # Validate data - filter extreme outliers (>50% daily move)
                df = df[df['daily_return'].abs() <= 0.5]

                logger.info(f"Fetched {len(df)} rows from Stooq for {symbol}")
                return df

            except requests.RequestException as e:
                logger.warning(f"Stooq request failed for {symbol} (attempt {attempt + 1}): {e}")
                if attempt < self.MAX_RETRIES - 1:
                    time.sleep(self.RETRY_DELAY * (2 ** attempt))  # Exponential backoff
                continue

            except Exception as e:
                logger.error(f"Error parsing Stooq data for {symbol}: {e}")
                return None

        return None


class FREDProvider(MarketDataProvider):
    """
    Fetch macro data from FRED (Federal Reserve Economic Data).
    Free API with key from https://fred.stlouisfed.org/docs/api/api_key.html
    """

    BASE_URL = "https://api.stlouisfed.org/fred/series/observations"
    MAX_RETRIES = 3
    RETRY_DELAY = 2

    # Common FRED series IDs
    SERIES_MAP = {
        'RF': 'DGS3MO',      # 3-Month Treasury Rate (risk-free proxy)
        'RF_1M': 'DGS1MO',   # 1-Month Treasury
        'TERM_SPREAD': 'T10Y2Y',  # 10Y-2Y Treasury Spread
        'CREDIT_SPREAD': 'BAA10Y',  # BAA Corporate Bond Spread
        '10Y': 'DGS10',      # 10-Year Treasury
        '2Y': 'DGS2',        # 2-Year Treasury
        'VIX': 'VIXCLS',     # VIX Index
    }

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key
        self.session = requests.Session()

    def get_source_name(self) -> str:
        return "fred"

    def fetch_series(
        self,
        symbol: str,
        start_date: date,
        end_date: date
    ) -> Optional[pd.DataFrame]:
        """Fetch series from FRED"""
        if not self.api_key:
            logger.warning("FRED API key not configured")
            return None

        # Map symbol to FRED series ID
        series_id = self.SERIES_MAP.get(symbol, symbol)

        params = {
            'series_id': series_id,
            'api_key': self.api_key,
            'file_type': 'json',
            'observation_start': start_date.strftime('%Y-%m-%d'),
            'observation_end': end_date.strftime('%Y-%m-%d'),
        }

        for attempt in range(self.MAX_RETRIES):
            try:
                response = self.session.get(self.BASE_URL, params=params, timeout=30)
                response.raise_for_status()

                data = response.json()
                observations = data.get('observations', [])

                if not observations:
                    logger.warning(f"No observations from FRED for {symbol}")
                    return None

                # Convert to DataFrame
                records = []
                for obs in observations:
                    if obs['value'] != '.':  # FRED uses '.' for missing
                        records.append({
                            'date': datetime.strptime(obs['date'], '%Y-%m-%d').date(),
                            'value': float(obs['value'])
                        })

                df = pd.DataFrame(records)
                df = df.sort_values('date')

                # For rates, convert from percentage to decimal
                if symbol in ['RF', 'RF_1M', '10Y', '2Y']:
                    df['value'] = df['value'] / 100

                # Compute daily change as "return" for rates
                df['daily_return'] = df['value'].diff()
                df['close'] = df['value']  # Alias for consistency

                logger.info(f"Fetched {len(df)} rows from FRED for {symbol}")
                return df

            except requests.RequestException as e:
                logger.warning(f"FRED request failed for {symbol} (attempt {attempt + 1}): {e}")
                if attempt < self.MAX_RETRIES - 1:
                    time.sleep(self.RETRY_DELAY * (2 ** attempt))
                continue

            except Exception as e:
                logger.error(f"Error parsing FRED data for {symbol}: {e}")
                return None

        return None


class YFinanceProvider(MarketDataProvider):
    """
    Fallback provider using yfinance.
    Use sparingly due to rate limits.
    """

    def __init__(self):
        self._yf = None

    def _get_yfinance(self):
        if self._yf is None:
            try:
                import yfinance as yf
                self._yf = yf
            except ImportError:
                logger.error("yfinance not installed")
                return None
        return self._yf

    def get_source_name(self) -> str:
        return "yfinance"

    def fetch_series(
        self,
        symbol: str,
        start_date: date,
        end_date: date
    ) -> Optional[pd.DataFrame]:
        """Fetch from yfinance as fallback"""
        yf = self._get_yfinance()
        if yf is None:
            return None

        try:
            ticker = yf.Ticker(symbol)
            # Add buffer day for yfinance quirks
            df = ticker.history(
                start=start_date.strftime('%Y-%m-%d'),
                end=(end_date + timedelta(days=1)).strftime('%Y-%m-%d'),
                auto_adjust=True
            )

            if df.empty:
                logger.warning(f"No data from yfinance for {symbol}")
                return None

            df = df.reset_index()
            df = df.rename(columns={'Date': 'date', 'Close': 'close'})
            df['date'] = pd.to_datetime(df['date']).dt.date
            df = df[['date', 'close']].sort_values('date')

            # Compute daily returns
            df['daily_return'] = df['close'].pct_change()

            # Validate - filter extreme outliers
            df = df[df['daily_return'].abs() <= 0.5]

            logger.info(f"Fetched {len(df)} rows from yfinance for {symbol}")

            # Rate limit protection
            time.sleep(0.5)

            return df

        except Exception as e:
            logger.error(f"Error fetching from yfinance for {symbol}: {e}")
            return None


class TiingoFactorProvider(MarketDataProvider):
    """
    Provider using Tiingo API for factor ETF data.
    Primary provider for US ETFs with reliable, enterprise-grade data.
    Uses direct HTTP requests for better error handling on empty responses.
    """

    BASE_URL = "https://api.tiingo.com/tiingo/daily"

    def __init__(self, api_key: Optional[str] = None):
        self._api_key = api_key
        self._session = None
        self._initialized = False

    def _get_session(self):
        if self._session is None:
            if not self._api_key:
                # Try to get from settings
                try:
                    from app.core.config import settings
                    self._api_key = settings.TIINGO_API_KEY
                    if self._api_key:
                        logger.info("TiingoFactorProvider: Got API key from settings")
                except Exception as e:
                    logger.warning(f"TiingoFactorProvider: Failed to get API key from settings: {e}")

            if self._api_key:
                self._session = requests.Session()
                self._session.headers.update({
                    'Content-Type': 'application/json',
                    'Authorization': f'Token {self._api_key}'
                })
                self._initialized = True
                logger.info("TiingoFactorProvider: Session initialized successfully")
            else:
                logger.warning("TiingoFactorProvider: No API key available")
        return self._session

    def get_source_name(self) -> str:
        return "tiingo"

    def fetch_series(
        self,
        symbol: str,
        start_date: date,
        end_date: date
    ) -> Optional[pd.DataFrame]:
        """Fetch historical prices from Tiingo using direct HTTP requests"""
        session = self._get_session()
        if session is None:
            logger.warning(f"TiingoFactorProvider: Session not configured, cannot fetch {symbol}")
            return None

        try:
            url = f"{self.BASE_URL}/{symbol}/prices"
            params = {
                'startDate': start_date.strftime('%Y-%m-%d'),
                'endDate': end_date.strftime('%Y-%m-%d'),
            }

            logger.info(f"TiingoFactorProvider: Fetching {symbol} from {start_date} to {end_date}")

            response = session.get(url, params=params, timeout=30)
            response.raise_for_status()

            data = response.json()

            if not data:
                logger.warning(f"TiingoFactorProvider: No data returned for {symbol} ({start_date} to {end_date})")
                return None

            df = pd.DataFrame(data)

            if df.empty:
                logger.warning(f"TiingoFactorProvider: Empty dataframe for {symbol}")
                return None

            logger.info(f"TiingoFactorProvider: Raw response for {symbol}: {len(df)} rows, columns: {list(df.columns)}")

            # Use adjClose if available, otherwise close
            close_col = 'adjClose' if 'adjClose' in df.columns else 'close'
            if close_col not in df.columns:
                logger.warning(f"TiingoFactorProvider: No close price column found for {symbol}, columns: {list(df.columns)}")
                return None

            # Select and rename columns
            df = df[['date', close_col]].copy()
            df = df.rename(columns={close_col: 'close'})
            df['date'] = pd.to_datetime(df['date']).dt.date
            df = df.sort_values('date')

            # Compute daily returns
            df['daily_return'] = df['close'].pct_change()

            # Validate - filter extreme outliers (>50% daily move)
            df = df[df['daily_return'].abs() <= 0.5]

            logger.info(f"TiingoFactorProvider: Fetched {len(df)} rows for {symbol}")
            return df

        except requests.exceptions.HTTPError as e:
            if e.response.status_code == 404:
                logger.warning(f"TiingoFactorProvider: Symbol {symbol} not found (404)")
            else:
                logger.error(f"TiingoFactorProvider: HTTP error for {symbol}: {e}")
            return None
        except Exception as e:
            logger.error(f"TiingoFactorProvider: Error fetching {symbol}: {e}", exc_info=True)
            return None


class DataProviderManager:
    """
    Manages multiple data providers with fallback logic.

    Tiingo is the primary and preferred source for all ETF data.
    Fallbacks (Stooq, yfinance) are only tried for broader date ranges
    when Tiingo fails with an error (not just empty data).
    """

    # Minimum date range to trigger fallback providers
    # Short ranges with no data are likely holidays - don't retry
    MIN_DAYS_FOR_FALLBACK = 5

    def __init__(self, fred_api_key: Optional[str] = None, tiingo_api_key: Optional[str] = None):
        self.providers = {
            'tiingo': TiingoFactorProvider(api_key=tiingo_api_key),
            'stooq': StooqProvider(),
            'fred': FREDProvider(api_key=fred_api_key),
            'yfinance': YFinanceProvider(),
        }

        # Tiingo is primary; only fall back for broader ranges if it errors
        self.primary_provider = 'tiingo'
        self.fallback_providers = ['stooq', 'yfinance']
        # Default provider for macro/rates
        self.macro_provider = 'fred'

    def fetch_etf_data(
        self,
        symbol: str,
        start_date: date,
        end_date: date
    ) -> Tuple[Optional[pd.DataFrame], str]:
        """
        Fetch ETF data with smart fallback logic.

        Strategy:
        1. Always try Tiingo first
        2. If Tiingo returns empty data for a small date range (< 5 days),
           assume it's holidays and return empty (don't waste time on fallbacks)
        3. Only try fallbacks for larger date ranges or actual errors
        """
        date_range_days = (end_date - start_date).days

        # Try primary provider (Tiingo)
        primary = self.providers.get(self.primary_provider)
        if primary:
            try:
                df = primary.fetch_series(symbol, start_date, end_date)
                if df is not None and len(df) > 0:
                    return df, self.primary_provider

                # Tiingo returned empty - check if we should try fallbacks
                if date_range_days < self.MIN_DAYS_FOR_FALLBACK:
                    # Small date range with no data = likely holiday, don't retry
                    logger.debug(f"No data for {symbol} ({start_date} to {end_date}), "
                                f"small range - assuming holiday, skipping fallbacks")
                    return None, ''

            except Exception as e:
                logger.warning(f"Primary provider error for {symbol}: {e}")
                # Fall through to try fallbacks

        # Only try fallbacks for larger date ranges
        if date_range_days >= self.MIN_DAYS_FOR_FALLBACK:
            for provider_name in self.fallback_providers:
                provider = self.providers.get(provider_name)
                if provider:
                    try:
                        df = provider.fetch_series(symbol, start_date, end_date)
                        if df is not None and len(df) > 0:
                            logger.info(f"Fallback provider {provider_name} succeeded for {symbol}")
                            return df, provider_name
                    except Exception as e:
                        logger.warning(f"Fallback provider {provider_name} failed for {symbol}: {e}")
                        continue

        return None, ''

    def fetch_macro_data(
        self,
        symbol: str,
        start_date: date,
        end_date: date
    ) -> Tuple[Optional[pd.DataFrame], str]:
        """Fetch macro/rate data from FRED"""
        provider = self.providers.get(self.macro_provider)
        if provider:
            df = provider.fetch_series(symbol, start_date, end_date)
            if df is not None:
                return df, self.macro_provider
        return None, ''
