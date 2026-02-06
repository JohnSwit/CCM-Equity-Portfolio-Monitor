"""
Market data service for fetching and storing security and benchmark prices.
Uses Tiingo as primary source with yfinance fallback.
"""
import pandas as pd
from tiingo import TiingoClient
from typing import Optional, Dict, List
from datetime import datetime, date, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_, func
import asyncio
import logging

from app.models import Security, PricesEOD, BenchmarkDefinition, BenchmarkLevel
from app.core.config import settings

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
            try:
                logger.info(f"Initializing TiingoClient with API key: {settings.TIINGO_API_KEY[:8]}...")
                config = {
                    'api_key': settings.TIINGO_API_KEY,
                    'session': True  # Reuse HTTP session for performance
                }
                self._tiingo_client = TiingoClient(config)
                logger.info("TiingoClient initialized successfully")
            except Exception as e:
                logger.error(f"Failed to initialize TiingoClient: {e}", exc_info=True)
                return None
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
        """Fetch and store prices for a security - optimized to only fetch missing dates"""
        source = 'tiingo'

        # If force refresh, delete existing and fetch all
        if force_refresh:
            deleted = self.db.query(PricesEOD).filter(
                and_(
                    PricesEOD.security_id == security_id,
                    PricesEOD.date >= start_date,
                    PricesEOD.date <= end_date
                )
            ).delete(synchronize_session=False)
            self.db.commit()
            logger.info(f"Force refresh: deleted {deleted} existing prices for {symbol}")
            fetch_start = start_date
            fetch_end = end_date
        else:
            # Find the latest price date we have for this security
            latest_price = self.db.query(func.max(PricesEOD.date)).filter(
                PricesEOD.security_id == security_id
            ).scalar()

            if latest_price and latest_price >= end_date:
                # Already have all data up to end_date
                logger.debug(f"Prices for {symbol} already up to date (latest: {latest_price})")
                return 0

            # Only fetch from after our latest price
            if latest_price:
                fetch_start = latest_price + timedelta(days=1)
            else:
                fetch_start = start_date
            fetch_end = end_date

            if fetch_start > fetch_end:
                logger.debug(f"No new dates to fetch for {symbol} (latest: {latest_price})")
                return 0

        logger.info(f"Fetching prices for {symbol}: {fetch_start} to {fetch_end}")

        # Fetch only missing dates from Tiingo
        df = self.fetch_tiingo_prices(symbol, fetch_start, fetch_end)

        # Fallback to yfinance if Tiingo fails
        if df is None or df.empty:
            df = self.fetch_yfinance_prices(symbol, fetch_start, fetch_end)
            source = 'yfinance'

        if df is None or df.empty:
            # Only warn if we're missing historical data, not just today's data
            today = date.today()
            if fetch_start < today:
                logger.warning(f"No prices fetched for {symbol} ({fetch_start} to {fetch_end})")
            else:
                logger.debug(f"No new market data yet for {symbol} (requested: {fetch_start})")
            return 0

        # Get existing dates to avoid duplicates (in case of overlaps)
        existing_dates = set(
            row[0] for row in self.db.query(PricesEOD.date).filter(
                and_(
                    PricesEOD.security_id == security_id,
                    PricesEOD.date >= fetch_start,
                    PricesEOD.date <= fetch_end
                )
            ).all()
        )

        # Bulk insert only new prices
        new_prices = []
        for _, row in df.iterrows():
            if row['date'] not in existing_dates:
                new_prices.append(PricesEOD(
                    security_id=security_id,
                    date=row['date'],
                    close=float(row['close']),
                    source=source
                ))

        if new_prices:
            self.db.bulk_save_objects(new_prices)
            self.db.commit()

        logger.info(f"Stored {len(new_prices)} new prices for {symbol} from {source}")
        return len(new_prices)

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
        end_date: date,
        force_refresh: bool = False
    ) -> int:
        """Fetch and store benchmark prices - optimized to only fetch missing dates"""
        # Map to Tiingo-compatible symbol
        tiingo_symbol = self.get_benchmark_tiingo_symbol(benchmark_code, provider_symbol)
        source = 'tiingo'

        # Check what data we already have (incremental update)
        if not force_refresh:
            latest_level = self.db.query(func.max(BenchmarkLevel.date)).filter(
                BenchmarkLevel.code == benchmark_code
            ).scalar()

            if latest_level and latest_level >= end_date:
                logger.debug(f"Benchmark {benchmark_code} already up to date (latest: {latest_level})")
                return 0

            # Only fetch from after our latest data
            if latest_level:
                fetch_start = latest_level + timedelta(days=1)
            else:
                fetch_start = start_date

            if fetch_start > end_date:
                logger.debug(f"No new dates to fetch for benchmark {benchmark_code}")
                return 0
        else:
            fetch_start = start_date

        logger.info(f"Fetching benchmark {benchmark_code} prices: {fetch_start} to {end_date}")

        # Try Tiingo first
        df = self.fetch_tiingo_benchmark_prices(tiingo_symbol, fetch_start, end_date)

        if df is None or df.empty:
            # Fallback to yfinance with benchmark code
            df = self.fetch_yfinance_prices(benchmark_code, fetch_start, end_date)
            source = 'yfinance'

        if df is None or df.empty:
            today = date.today()
            if fetch_start < today:
                logger.warning(f"No prices fetched for benchmark {benchmark_code}")
            else:
                logger.debug(f"No new market data yet for benchmark {benchmark_code}")
            return 0

        # Get existing dates to avoid duplicates
        existing_dates = set(
            row[0] for row in self.db.query(BenchmarkLevel.date).filter(
                and_(
                    BenchmarkLevel.code == benchmark_code,
                    BenchmarkLevel.date >= fetch_start,
                    BenchmarkLevel.date <= end_date
                )
            ).all()
        )

        # Bulk insert only new levels
        new_levels = []
        for _, row in df.iterrows():
            if row['date'] not in existing_dates:
                new_levels.append(BenchmarkLevel(
                    code=benchmark_code,
                    date=row['date'],
                    level=float(row['close'])
                ))

        if new_levels:
            self.db.bulk_save_objects(new_levels)
            self.db.commit()

        if len(new_levels) > 0:
            logger.info(f"Stored {len(new_levels)} levels for benchmark {benchmark_code} from {source}")
        return len(new_levels)

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

    async def update_all_security_prices(
        self,
        force_refresh: bool = False,
        max_concurrent: int = 10
    ) -> Dict[str, int]:
        """
        Update prices for all securities with transactions.

        Uses parallel fetching with semaphore to control concurrency.
        This significantly speeds up the process while respecting rate limits.
        """
        from app.models import Transaction

        # Log Tiingo client status
        logger.info(f"Tiingo API key configured: {bool(settings.TIINGO_API_KEY)}")
        logger.info(f"Tiingo client initialized: {self.tiingo_client is not None}")
        if force_refresh:
            logger.info("Force refresh enabled - will delete and re-fetch all prices")

        # Get all securities with transactions
        securities = self.db.query(Security).join(Transaction).distinct().all()

        logger.info(f"Found {len(securities)} securities with transactions to update")
        logger.info(f"Using parallel fetching with max_concurrent={max_concurrent}")

        results = {
            'total_securities': len(securities),
            'updated': 0,
            'failed': 0,
            'skipped': 0
        }

        # Create semaphore for concurrency control
        semaphore = asyncio.Semaphore(max_concurrent)

        async def fetch_with_semaphore(security):
            async with semaphore:
                try:
                    count = await self.update_security_prices(
                        security.id, security.symbol, force_refresh=force_refresh
                    )
                    return (security.symbol, count, None)
                except Exception as e:
                    logger.error(f"Failed to update {security.symbol}: {e}")
                    return (security.symbol, 0, str(e))

        # Process all securities in parallel (controlled by semaphore)
        tasks = [fetch_with_semaphore(security) for security in securities]
        fetch_results = await asyncio.gather(*tasks, return_exceptions=True)

        # Aggregate results
        for result in fetch_results:
            if isinstance(result, Exception):
                results['failed'] += 1
            elif result[2] is not None:  # Error case
                results['failed'] += 1
            elif result[1] > 0:
                results['updated'] += 1
            else:
                results['skipped'] += 1

        logger.info(f"Price update complete: {results['updated']} updated, {results['skipped']} skipped, {results['failed']} failed")
        return results

    async def update_benchmark_prices(self, start_date: Optional[date] = None) -> Dict[str, int]:
        """Update prices for all benchmarks"""
        benchmarks = self.db.query(BenchmarkDefinition).all()

        results = {
            'total_benchmarks': len(benchmarks),
            'updated': 0,
            'failed': 0
        }

        end_date = date.today()

        # If no start_date provided, determine from earliest transaction or default to 25 years
        if start_date is None:
            from app.models import Transaction
            earliest_txn = self.db.query(func.min(Transaction.trade_date)).scalar()
            if earliest_txn:
                # Go back a bit before earliest transaction for proper benchmark comparison
                start_date = earliest_txn - timedelta(days=30)
            else:
                # Default to 25 years for comprehensive history
                start_date = end_date - timedelta(days=25*365)

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

    async def update_factor_etf_prices(self, start_date: Optional[date] = None) -> Dict[str, int]:
        """Update prices for factor analysis ETFs"""
        # Factor ETFs used in STYLE7 analysis
        factor_etfs = ['SPY', 'IWM', 'IVE', 'IVW', 'QUAL', 'SPLV', 'MTUM']

        # Additional ETFs that might be used
        additional_etfs = ['QQQ', 'USMV', 'INDU']

        all_etfs = factor_etfs + additional_etfs

        results = {
            'total_etfs': len(all_etfs),
            'updated': 0,
            'failed': 0
        }

        end_date = date.today()

        # If no start_date provided, determine from earliest transaction or default to 25 years
        if start_date is None:
            from app.models import Transaction
            earliest_txn = self.db.query(func.min(Transaction.trade_date)).scalar()
            if earliest_txn:
                start_date = earliest_txn - timedelta(days=30)
            else:
                start_date = end_date - timedelta(days=25*365)

        for symbol in all_etfs:
            try:
                # Get or create security record
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

                count = await self.fetch_and_store_prices(
                    security.id, symbol, start_date, end_date
                )
                if count > 0:
                    results['updated'] += 1
                    logger.info(f"Updated {count} prices for factor ETF {symbol}")

                await asyncio.sleep(self.rate_limit_delay)

            except Exception as e:
                logger.error(f"Failed to update factor ETF {symbol}: {e}")
                results['failed'] += 1

        self.db.commit()
        return results
