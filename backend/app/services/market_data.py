import httpx
import yfinance as yf
import pandas as pd
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
    """Fetches market data from Stooq (primary) and yfinance (fallback)"""

    def __init__(self, db: Session):
        self.db = db
        self.stooq_base_url = "https://stooq.com/q/d/l/"
        self.rate_limit_delay = 0.5  # seconds between requests

    def get_provider_symbol(self, symbol: str, is_benchmark: bool = False) -> str:
        """Map internal symbol to provider symbol"""
        # Handle benchmark special cases
        if is_benchmark:
            benchmark_map = {
                'INDU': '^DJI',  # Dow Jones
                'SPX': '^SPX',   # S&P 500
            }
            if symbol in benchmark_map:
                return benchmark_map[symbol]

        # Convert BRK.B format to BRK-B
        if '.' in symbol:
            symbol = symbol.replace('.', '-')

        # For Stooq, US equities need .US suffix
        if not symbol.startswith('^'):
            return f"{symbol}.US"

        return symbol

    async def fetch_stooq_prices(
        self,
        symbol: str,
        start_date: date,
        end_date: date
    ) -> Optional[pd.DataFrame]:
        """Fetch prices from Stooq"""
        try:
            provider_symbol = self.get_provider_symbol(symbol)

            # Stooq URL format: ?s=SYMBOL&d1=YYYYMMDD&d2=YYYYMMDD&i=d
            params = {
                's': provider_symbol,
                'd1': start_date.strftime('%Y%m%d'),
                'd2': end_date.strftime('%Y%m%d'),
                'i': 'd'  # daily
            }

            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(self.stooq_base_url, params=params)
                response.raise_for_status()

                # Parse CSV
                df = pd.read_csv(
                    pd.io.common.StringIO(response.text),
                    parse_dates=['Date']
                )

                if df.empty or 'Close' not in df.columns:
                    return None

                df = df.rename(columns={'Date': 'date', 'Close': 'close'})
                df = df[['date', 'close']].dropna()

                return df

        except Exception as e:
            logger.warning(f"Stooq fetch failed for {symbol}: {e}")
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
            ticker = yf.Ticker(symbol)
            df = ticker.history(start=start_date, end=end_date)

            if df.empty:
                return None

            df = df.reset_index()
            df = df.rename(columns={'Date': 'date', 'Close': 'close'})
            df['date'] = pd.to_datetime(df['date']).dt.date
            df = df[['date', 'close']].dropna()

            return df

        except Exception as e:
            logger.warning(f"yfinance fetch failed for {symbol}: {e}")
            return None

    async def fetch_and_store_prices(
        self,
        security_id: int,
        symbol: str,
        start_date: date,
        end_date: date
    ) -> int:
        """Fetch and store prices for a security"""
        # Try Stooq first
        df = await self.fetch_stooq_prices(symbol, start_date, end_date)
        source = 'stooq'

        # Fallback to yfinance if Stooq fails
        if df is None or df.empty:
            df = self.fetch_yfinance_prices(symbol, start_date, end_date)
            source = 'yfinance'

        if df is None or df.empty:
            logger.warning(f"No prices fetched for {symbol}")
            return 0

        # Store prices
        count = 0
        for _, row in df.iterrows():
            existing = self.db.query(PricesEOD).filter(
                and_(
                    PricesEOD.security_id == security_id,
                    PricesEOD.date == row['date']
                )
            ).first()

            if not existing:
                price = PricesEOD(
                    security_id=security_id,
                    date=row['date'],
                    close=float(row['close']),
                    source=source
                )
                self.db.add(price)
                count += 1

        self.db.commit()
        logger.info(f"Stored {count} prices for {symbol} from {source}")
        return count

    async def fetch_and_store_benchmark_prices(
        self,
        benchmark_code: str,
        provider_symbol: str,
        start_date: date,
        end_date: date
    ) -> int:
        """Fetch and store benchmark prices"""
        # For benchmarks, use the provider symbol directly
        df = await self.fetch_stooq_prices(provider_symbol, start_date, end_date)

        if df is None or df.empty:
            # Try yfinance with original code
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

    async def update_security_prices(self, security_id: int, symbol: str) -> int:
        """Update prices for a security (fill missing dates)"""
        # Get date range from transactions
        from app.models import Transaction

        first_txn = self.db.query(Transaction).filter(
            Transaction.security_id == security_id
        ).order_by(Transaction.trade_date).first()

        if not first_txn:
            return 0

        start_date = first_txn.trade_date
        end_date = date.today()

        # Fetch and store
        count = await self.fetch_and_store_prices(
            security_id, symbol, start_date, end_date
        )

        # Rate limiting
        await asyncio.sleep(self.rate_limit_delay)

        return count

    async def update_all_security_prices(self) -> Dict[str, int]:
        """Update prices for all securities with transactions"""
        from app.models import Transaction

        # Get all securities with transactions
        securities = self.db.query(Security).join(Transaction).distinct().all()

        results = {
            'total_securities': len(securities),
            'updated': 0,
            'failed': 0
        }

        for security in securities:
            try:
                count = await self.update_security_prices(security.id, security.symbol)
                if count > 0:
                    results['updated'] += 1
            except Exception as e:
                logger.error(f"Failed to update {security.symbol}: {e}")
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
