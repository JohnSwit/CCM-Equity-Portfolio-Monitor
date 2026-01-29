import httpx
import yfinance as yf
import pandas as pd
from tiingo import TiingoClient
from typing import Optional, Dict, List
from datetime import datetime, date, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_
import asyncio
import time
from app.models import Security, PricesEOD, BenchmarkDefinition, BenchmarkLevel
from app.core.config import settings
import logging

logger = logging.getLogger(__name__)


class MarketDataProvider:
    """Fetches market data from Tiingo (primary) and yfinance (fallback)"""

    def __init__(self, db: Session):
        self.db = db
        self.rate_limit_delay = 0.2  # seconds between requests (Tiingo is more generous)
        self._tiingo_client = None

    @property
    def tiingo_client(self) -> Optional[TiingoClient]:
        """Lazy initialization of Tiingo client"""
        if self._tiingo_client is None and settings.TIINGO_API_KEY:
            config = {
                'api_key': settings.TIINGO_API_KEY,
                'session': True  # Reuse HTTP session for performance
            }
            self._tiingo_client = TiingoClient(config)
        return self._tiingo_client

    def normalize_symbol(self, symbol: str) -> str:
        """Normalize symbol for Tiingo API"""
        # Tiingo uses standard ticker symbols
        # Handle special cases like BRK.B -> BRK-B
        if '.' in symbol and not symbol.startswith('^'):
            symbol = symbol.replace('.', '-')
        return symbol.upper()

    def get_benchmark_tiingo_symbol(self, benchmark_code: str, provider_symbol: str) -> str:
        """Map benchmark codes to Tiingo-compatible symbols"""
        # Tiingo uses ETF symbols for major indexes
        benchmark_map = {
            'INDU': 'DIA',   # Dow Jones -> SPDR Dow Jones ETF
            '^DJI': 'DIA',
            'SPX': 'SPY',    # S&P 500 -> SPDR S&P 500 ETF
            '^SPX': 'SPY',
            'SPY.US': 'SPY',
            'QQQ.US': 'QQQ',
            'DIA.US': 'DIA',
        }

        # First check if provider_symbol has a mapping
        if provider_symbol in benchmark_map:
            return benchmark_map[provider_symbol]

        # Check benchmark code
        if benchmark_code in benchmark_map:
            return benchmark_map[benchmark_code]

        # Strip .US suffix if present
        clean_symbol = provider_symbol.replace('.US', '')
        return clean_symbol

    def fetch_tiingo_prices(
        self,
        symbol: str,
        start_date: date,
        end_date: date
    ) -> Optional[pd.DataFrame]:
        """Fetch prices from Tiingo EOD API"""
        logger.info(f"fetch_tiingo_prices called for {symbol}")

        if not self.tiingo_client:
            logger.warning(f"Tiingo client not configured for {symbol} - API key present: {bool(settings.TIINGO_API_KEY)}")
            return None

        try:
            normalized_symbol = self.normalize_symbol(symbol)
            logger.info(f"Fetching Tiingo prices for {normalized_symbol} from {start_date} to {end_date}")

            # Tiingo returns data as list of dicts or DataFrame
            price_data = self.tiingo_client.get_ticker_price(
                normalized_symbol,
                startDate=start_date.strftime('%Y-%m-%d'),
                endDate=end_date.strftime('%Y-%m-%d'),
                frequency='daily'
            )

            if not price_data:
                logger.warning(f"No Tiingo data returned for {symbol}")
                return None

            # Convert to DataFrame
            df = pd.DataFrame(price_data)
            logger.info(f"Tiingo raw response for {symbol}: {len(df)} rows, columns: {list(df.columns)}")

            if df.empty or 'adjClose' not in df.columns:
                logger.warning(f"Tiingo returned empty or invalid data for {symbol}")
                return None

            # Use adjusted close for accuracy (accounts for splits/dividends)
            # Select only the columns we need to avoid duplicate 'close' columns
            df = df[['date', 'adjClose']].copy()
            df = df.rename(columns={'adjClose': 'close'})

            # Parse date - Tiingo returns ISO format strings
            df['date'] = pd.to_datetime(df['date']).dt.date
            df = df.dropna()

            logger.info(f"Tiingo returned {len(df)} price records for {symbol}")
            return df

        except Exception as e:
            logger.error(f"Tiingo fetch failed for {symbol}: {e}", exc_info=True)
            return None

    def fetch_yfinance_prices(
        self,
        symbol: str,
        start_date: date,
        end_date: date
    ) -> Optional[pd.DataFrame]:
        """Fetch prices from yfinance as fallback"""
        if not settings.ENABLE_YFINANCE_FALLBACK:
            return None

        try:
            logger.info(f"Falling back to yfinance for {symbol}")
            ticker = yf.Ticker(symbol)
            df = ticker.history(start=start_date, end=end_date)

            if df.empty:
                return None

            df = df.reset_index()
            df = df.rename(columns={'Date': 'date', 'Close': 'close'})
            df['date'] = pd.to_datetime(df['date']).dt.date
            df = df[['date', 'close']].dropna()

            logger.info(f"yfinance returned {len(df)} price records for {symbol}")
            return df

        except Exception as e:
            logger.warning(f"yfinance fetch failed for {symbol}: {e}")
            return None

    async def fetch_and_store_prices(
        self,
        security_id: int,
        symbol: str,
        start_date: date,
        end_date: date,
        force_refresh: bool = False
    ) -> int:
        """Fetch and store prices for a security"""
        # Try Tiingo first (primary source)
        df = self.fetch_tiingo_prices(symbol, start_date, end_date)
        source = 'tiingo'

        # Fallback to yfinance if Tiingo fails
        if df is None or df.empty:
            df = self.fetch_yfinance_prices(symbol, start_date, end_date)
            source = 'yfinance'

        if df is None or df.empty:
            logger.warning(f"No prices fetched for {symbol}")
            return 0

        # If force refresh, delete existing prices in this date range first
        if force_refresh:
            deleted = self.db.query(PricesEOD).filter(
                and_(
                    PricesEOD.security_id == security_id,
                    PricesEOD.date >= start_date,
                    PricesEOD.date <= end_date
                )
            ).delete(synchronize_session=False)
            logger.info(f"Force refresh: deleted {deleted} existing prices for {symbol}")

        # Store prices (update existing if different, insert if not exists)
        count = 0
        for _, row in df.iterrows():
            price_value = float(row['close'])
            existing = self.db.query(PricesEOD).filter(
                and_(
                    PricesEOD.security_id == security_id,
                    PricesEOD.date == row['date']
                )
            ).first()

            if existing:
                # Update if price changed
                if existing.close != price_value:
                    existing.close = price_value
                    existing.source = source
                    count += 1
            else:
                price = PricesEOD(
                    security_id=security_id,
                    date=row['date'],
                    close=price_value,
                    source=source
                )
                self.db.add(price)
                count += 1

        self.db.commit()
        logger.info(f"Stored {count} prices for {symbol} from {source}")
        return count

    def fetch_tiingo_benchmark_prices(
        self,
        tiingo_symbol: str,
        start_date: date,
        end_date: date
    ) -> Optional[pd.DataFrame]:
        """Fetch benchmark prices from Tiingo using appropriate ETF symbol"""
        if not self.tiingo_client:
            logger.warning("Tiingo client not configured (missing API key)")
            return None

        try:
            logger.info(f"Fetching Tiingo benchmark prices for {tiingo_symbol}")

            price_data = self.tiingo_client.get_ticker_price(
                tiingo_symbol,
                startDate=start_date.strftime('%Y-%m-%d'),
                endDate=end_date.strftime('%Y-%m-%d'),
                frequency='daily'
            )

            if not price_data:
                return None

            df = pd.DataFrame(price_data)

            if df.empty or 'adjClose' not in df.columns:
                return None

            # Select only the columns we need to avoid duplicate 'close' columns
            df = df[['date', 'adjClose']].copy()
            df = df.rename(columns={'adjClose': 'close'})
            df['date'] = pd.to_datetime(df['date']).dt.date
            df = df.dropna()

            logger.info(f"Tiingo returned {len(df)} benchmark records for {tiingo_symbol}")
            return df

        except Exception as e:
            logger.warning(f"Tiingo benchmark fetch failed for {tiingo_symbol}: {e}")
            return None

    async def fetch_and_store_benchmark_prices(
        self,
        benchmark_code: str,
        provider_symbol: str,
        start_date: date,
        end_date: date
    ) -> int:
        """Fetch and store benchmark prices"""
        # Map to Tiingo-compatible symbol
        tiingo_symbol = self.get_benchmark_tiingo_symbol(benchmark_code, provider_symbol)

        # Try Tiingo first
        df = self.fetch_tiingo_benchmark_prices(tiingo_symbol, start_date, end_date)

        if df is None or df.empty:
            # Fallback to yfinance with benchmark code
            df = self.fetch_yfinance_prices(benchmark_code, start_date, end_date)

        if df is None or df.empty:
            logger.warning(f"No prices fetched for benchmark {benchmark_code}")
            return 0

        # Store benchmark levels
        count = 0
        for _, row in df.iterrows():
            existing = self.db.query(BenchmarkLevel).filter(
                and_(
                    BenchmarkLevel.code == benchmark_code,
                    BenchmarkLevel.date == row['date']
                )
            ).first()

            if not existing:
                level = BenchmarkLevel(
                    code=benchmark_code,
                    date=row['date'],
                    level=float(row['close'])
                )
                self.db.add(level)
                count += 1

        self.db.commit()
        logger.info(f"Stored {count} levels for benchmark {benchmark_code}")
        return count

    def get_missing_price_dates(
        self,
        security_id: int,
        start_date: date,
        end_date: date
    ) -> List[date]:
        """Get dates with missing prices"""
        existing_dates = self.db.query(PricesEOD.date).filter(
            and_(
                PricesEOD.security_id == security_id,
                PricesEOD.date >= start_date,
                PricesEOD.date <= end_date
            )
        ).all()

        existing_dates_set = {d[0] for d in existing_dates}

        # Generate all dates in range
        all_dates = pd.date_range(start=start_date, end=end_date, freq='D')
        all_dates_set = {d.date() for d in all_dates}

        missing = sorted(all_dates_set - existing_dates_set)
        return missing

    async def update_security_prices(
        self,
        security_id: int,
        symbol: str,
        force_refresh: bool = False
    ) -> int:
        """Update prices for a security (fill missing dates)"""
        from app.models import Transaction

        logger.info(f"update_security_prices called for {symbol} (id={security_id})")

        first_txn = self.db.query(Transaction).filter(
            Transaction.security_id == security_id
        ).order_by(Transaction.trade_date).first()

        if not first_txn:
            logger.warning(f"No transactions found for {symbol} (id={security_id})")
            return 0

        start_date = first_txn.trade_date
        end_date = date.today()
        logger.info(f"Date range for {symbol}: {start_date} to {end_date}")

        # Fetch and store
        count = await self.fetch_and_store_prices(
            security_id, symbol, start_date, end_date, force_refresh=force_refresh
        )

        # Rate limiting
        await asyncio.sleep(self.rate_limit_delay)

        return count

    async def update_all_security_prices(self, force_refresh: bool = False) -> Dict[str, int]:
        """Update prices for all securities with transactions"""
        from app.models import Transaction

        # Log Tiingo client status
        logger.info(f"Tiingo API key configured: {bool(settings.TIINGO_API_KEY)}")
        logger.info(f"Tiingo client initialized: {self.tiingo_client is not None}")
        if force_refresh:
            logger.info("Force refresh enabled - will delete and re-fetch all prices")

        # Get all securities with transactions
        securities = self.db.query(Security).join(Transaction).distinct().all()

        logger.info(f"Found {len(securities)} securities with transactions to update")

        results = {
            'total_securities': len(securities),
            'updated': 0,
            'failed': 0
        }

        for security in securities:
            try:
                logger.info(f"Processing security: {security.symbol} (id={security.id})")
                count = await self.update_security_prices(
                    security.id, security.symbol, force_refresh=force_refresh
                )
                logger.info(f"Updated {count} prices for {security.symbol}")
                if count > 0:
                    results['updated'] += 1
            except Exception as e:
                logger.error(f"Failed to update {security.symbol}: {e}")
                results['failed'] += 1

        return results

    async def update_factor_etf_prices(self) -> Dict[str, int]:
        """Update prices for factor ETFs used in STYLE7 analysis"""
        # Factor ETFs from FactorsEngine
        FACTOR_ETFS = ['SPY', 'IWM', 'IVE', 'IVW', 'QUAL', 'SPLV', 'MTUM']

        logger.info(f"Updating factor ETF prices for: {FACTOR_ETFS}")

        results = {
            'total_etfs': len(FACTOR_ETFS),
            'updated': 0,
            'failed': 0
        }

        # Use 5-year lookback for factor analysis
        end_date = date.today()
        start_date = end_date - timedelta(days=5*365)

        for symbol in FACTOR_ETFS:
            try:
                # Find or create the security
                security = self.db.query(Security).filter(
                    Security.symbol == symbol
                ).first()

                if not security:
                    from app.models import AssetClass
                    security = Security(
                        symbol=symbol,
                        asset_name=f"{symbol} ETF",
                        asset_class=AssetClass.ETF
                    )
                    self.db.add(security)
                    self.db.flush()
                    logger.info(f"Created security for factor ETF: {symbol}")

                # Fetch prices from Tiingo
                df = self.fetch_tiingo_prices(symbol, start_date, end_date)

                if df is None or df.empty:
                    logger.warning(f"No Tiingo data for factor ETF {symbol}")
                    results['failed'] += 1
                    continue

                # Store prices
                count = 0
                for _, row in df.iterrows():
                    existing = self.db.query(PricesEOD).filter(
                        and_(
                            PricesEOD.security_id == security.id,
                            PricesEOD.date == row['date']
                        )
                    ).first()

                    if not existing:
                        price = PricesEOD(
                            security_id=security.id,
                            date=row['date'],
                            close=float(row['close']),
                            source='tiingo'
                        )
                        self.db.add(price)
                        count += 1

                self.db.commit()
                logger.info(f"Stored {count} prices for factor ETF {symbol}")

                if count > 0:
                    results['updated'] += 1

                await asyncio.sleep(self.rate_limit_delay)

            except Exception as e:
                logger.error(f"Failed to update factor ETF {symbol}: {e}")
                results['failed'] += 1

        return results

    async def update_benchmark_prices(self) -> Dict[str, int]:
        """Update prices for all benchmarks"""
        benchmarks = self.db.query(BenchmarkDefinition).all()

        results = {
            'total_benchmarks': len(benchmarks),
            'updated': 0,
            'failed': 0
        }

        # Use a reasonable lookback (e.g., 5 years)
        end_date = date.today()
        start_date = end_date - timedelta(days=5*365)

        for benchmark in benchmarks:
            try:
                count = await self.fetch_and_store_benchmark_prices(
                    benchmark.code,
                    benchmark.provider_symbol,
                    start_date,
                    end_date
                )
                if count > 0:
                    results['updated'] += 1
                await asyncio.sleep(self.rate_limit_delay)
            except Exception as e:
                logger.error(f"Failed to update benchmark {benchmark.code}: {e}")
                results['failed'] += 1

        return results
