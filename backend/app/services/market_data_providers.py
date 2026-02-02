"""
Market data providers for fetching EOD prices.
Primary: Tiingo
Fallback: Stooq, yfinance
"""
import httpx
import yfinance as yf
import pandas as pd
from typing import Optional, Dict, List
from datetime import date, timedelta
from abc import ABC, abstractmethod
import asyncio
import logging

from app.core.config import settings

logger = logging.getLogger(__name__)


class MarketDataProviderBase(ABC):
    """Abstract base class for market data providers"""

    @abstractmethod
    async def fetch_prices(
        self,
        symbol: str,
        start_date: date,
        end_date: date
    ) -> Optional[pd.DataFrame]:
        """
        Fetch EOD prices for a symbol.

        Returns DataFrame with columns: date, close
        Or None if fetch failed.
        """
        pass

    @property
    @abstractmethod
    def name(self) -> str:
        """Provider name for logging"""
        pass


class TiingoProvider(MarketDataProviderBase):
    """Tiingo market data provider - primary source"""

    BASE_URL = "https://api.tiingo.com/tiingo/daily"
    IEX_URL = "https://api.tiingo.com/iex"

    def __init__(self, api_key: Optional[str] = None):
        self.api_key = api_key or settings.TIINGO_API_KEY
        if not self.api_key:
            logger.warning("Tiingo API key not configured")

    @property
    def name(self) -> str:
        return "tiingo"

    def _normalize_symbol(self, symbol: str) -> str:
        """Normalize symbol for Tiingo API"""
        # Tiingo uses standard US symbols without suffixes
        # Remove any .US suffix if present
        if symbol.endswith('.US'):
            symbol = symbol[:-3]
        # Handle BRK.B -> BRK-B conversion
        symbol = symbol.replace('.', '-')
        return symbol.upper()

    async def fetch_prices(
        self,
        symbol: str,
        start_date: date,
        end_date: date
    ) -> Optional[pd.DataFrame]:
        """Fetch EOD prices from Tiingo"""
        if not self.api_key:
            logger.warning("Tiingo API key not set, skipping Tiingo provider")
            return None

        normalized_symbol = self._normalize_symbol(symbol)

        try:
            url = f"{self.BASE_URL}/{normalized_symbol}/prices"
            params = {
                "startDate": start_date.strftime("%Y-%m-%d"),
                "endDate": end_date.strftime("%Y-%m-%d"),
                "token": self.api_key,
                "format": "json"
            }

            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(url, params=params)

                if response.status_code == 404:
                    logger.warning(f"Tiingo: Symbol {normalized_symbol} not found")
                    return None

                if response.status_code == 401:
                    logger.error("Tiingo API key is invalid")
                    return None

                response.raise_for_status()
                data = response.json()

                if not data:
                    logger.info(f"Fetched 0 rows from Tiingo for {symbol}")
                    return None

                # Convert to DataFrame
                df = pd.DataFrame(data)

                if df.empty or 'date' not in df.columns:
                    logger.warning(f"No Tiingo data returned for {symbol}")
                    return None

                # Use adjClose if available (accounts for splits/dividends), else close
                close_col = 'adjClose' if 'adjClose' in df.columns else 'close'

                df['date'] = pd.to_datetime(df['date']).dt.date
                df = df[['date', close_col]].rename(columns={close_col: 'close'})
                df = df.dropna()

                logger.info(f"Fetched {len(df)} rows from Tiingo for {symbol}")
                return df

        except httpx.HTTPStatusError as e:
            logger.warning(f"Tiingo HTTP error for {symbol}: {e.response.status_code}")
            return None
        except Exception as e:
            logger.warning(f"Tiingo fetch failed for {symbol}: {e}")
            return None

    async def fetch_multiple(
        self,
        symbols: List[str],
        start_date: date,
        end_date: date
    ) -> Dict[str, pd.DataFrame]:
        """Fetch prices for multiple symbols efficiently"""
        results = {}

        # Tiingo doesn't have a bulk endpoint, fetch sequentially with rate limiting
        for symbol in symbols:
            df = await self.fetch_prices(symbol, start_date, end_date)
            if df is not None and not df.empty:
                results[symbol] = df
            await asyncio.sleep(0.2)  # Rate limit: ~5 requests/sec

        return results


class StooqProvider(MarketDataProviderBase):
    """Stooq market data provider - fallback"""

    BASE_URL = "https://stooq.com/q/d/l/"

    @property
    def name(self) -> str:
        return "stooq"

    def _normalize_symbol(self, symbol: str, is_index: bool = False) -> str:
        """Convert symbol to Stooq format"""
        # Stooq uses .US suffix for US stocks
        if is_index:
            # Index symbols (^DJI, ^GSPC)
            return symbol

        # Handle BRK.B -> BRK-B.US
        symbol = symbol.replace('.', '-')

        # Add .US suffix if not already present
        if not any(symbol.endswith(suffix) for suffix in ['.US', '.UK', '.DE']):
            symbol = f"{symbol}.US"

        return symbol

    async def fetch_prices(
        self,
        symbol: str,
        start_date: date,
        end_date: date
    ) -> Optional[pd.DataFrame]:
        """Fetch EOD prices from Stooq"""
        is_index = symbol.startswith('^')
        provider_symbol = self._normalize_symbol(symbol, is_index)

        try:
            params = {
                's': provider_symbol,
                'd1': start_date.strftime('%Y%m%d'),
                'd2': end_date.strftime('%Y%m%d'),
                'i': 'd'  # daily
            }

            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(self.BASE_URL, params=params)
                response.raise_for_status()

                # Parse CSV response
                df = pd.read_csv(
                    pd.io.common.StringIO(response.text),
                    parse_dates=['Date']
                )

                if df.empty or 'Close' not in df.columns:
                    logger.warning(f"No data returned from Stooq for {symbol}")
                    return None

                df = df.rename(columns={'Date': 'date', 'Close': 'close'})
                df['date'] = pd.to_datetime(df['date']).dt.date
                df = df[['date', 'close']].dropna()

                logger.info(f"Fetched {len(df)} rows from Stooq for {symbol}")
                return df

        except Exception as e:
            logger.warning(f"Stooq fetch failed for {symbol}: {e}")
            return None


class YFinanceProvider(MarketDataProviderBase):
    """yfinance market data provider - last resort fallback"""

    @property
    def name(self) -> str:
        return "yfinance"

    async def fetch_prices(
        self,
        symbol: str,
        start_date: date,
        end_date: date
    ) -> Optional[pd.DataFrame]:
        """Fetch EOD prices from yfinance"""
        if not settings.ENABLE_YFINANCE_FALLBACK:
            return None

        try:
            # yfinance is sync, run in executor
            loop = asyncio.get_event_loop()
            df = await loop.run_in_executor(
                None,
                self._fetch_sync,
                symbol,
                start_date,
                end_date
            )
            return df

        except Exception as e:
            logger.warning(f"yfinance fetch failed for {symbol}: {e}")
            return None

    def _fetch_sync(
        self,
        symbol: str,
        start_date: date,
        end_date: date
    ) -> Optional[pd.DataFrame]:
        """Synchronous fetch for yfinance"""
        try:
            ticker = yf.Ticker(symbol)
            df = ticker.history(start=start_date, end=end_date + timedelta(days=1))

            if df.empty:
                logger.warning(f"No data from yfinance for {symbol}")
                return None

            df = df.reset_index()
            df = df.rename(columns={'Date': 'date', 'Close': 'close'})
            df['date'] = pd.to_datetime(df['date']).dt.date
            df = df[['date', 'close']].dropna()

            logger.info(f"Fetched {len(df)} rows from yfinance for {symbol}")
            return df

        except Exception as e:
            logger.warning(f"yfinance sync fetch failed for {symbol}: {e}")
            return None


class MarketDataAggregator:
    """
    Aggregates multiple market data providers with fallback logic.
    Primary: Tiingo
    Fallbacks: Stooq, yfinance
    """

    def __init__(self, tiingo_api_key: Optional[str] = None):
        self.providers: List[MarketDataProviderBase] = [
            TiingoProvider(tiingo_api_key),
            StooqProvider(),
            YFinanceProvider(),
        ]
        self.rate_limit_delay = 0.3  # seconds between requests

    async def fetch_prices(
        self,
        symbol: str,
        start_date: date,
        end_date: date
    ) -> tuple[Optional[pd.DataFrame], str]:
        """
        Fetch prices using provider cascade.

        Returns:
            Tuple of (DataFrame or None, source provider name)
        """
        for provider in self.providers:
            try:
                df = await provider.fetch_prices(symbol, start_date, end_date)
                if df is not None and not df.empty:
                    return df, provider.name
            except Exception as e:
                logger.warning(f"{provider.name} failed for {symbol}: {e}")
                continue

        logger.warning(f"All providers failed for {symbol}")
        return None, "none"

    async def fetch_multiple(
        self,
        symbols: List[str],
        start_date: date,
        end_date: date
    ) -> Dict[str, tuple[pd.DataFrame, str]]:
        """
        Fetch prices for multiple symbols.

        Returns:
            Dict mapping symbol to (DataFrame, source) tuple
        """
        results = {}

        for symbol in symbols:
            df, source = await self.fetch_prices(symbol, start_date, end_date)
            if df is not None:
                results[symbol] = (df, source)
            await asyncio.sleep(self.rate_limit_delay)

        return results


# Singleton instance for convenience
_aggregator: Optional[MarketDataAggregator] = None

def get_market_data_aggregator() -> MarketDataAggregator:
    """Get or create the market data aggregator singleton"""
    global _aggregator
    if _aggregator is None:
        _aggregator = MarketDataAggregator()
    return _aggregator


async def fetch_prices_with_fallback(
    symbol: str,
    start_date: date,
    end_date: date
) -> tuple[Optional[pd.DataFrame], str]:
    """
    Convenience function to fetch prices with automatic fallback.

    Returns:
        Tuple of (DataFrame or None, source provider name)
    """
    aggregator = get_market_data_aggregator()
    return await aggregator.fetch_prices(symbol, start_date, end_date)
