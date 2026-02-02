"""
Market data service for fetching and storing security and benchmark prices.
Uses market_data_providers for the actual data fetching with Tiingo as primary source.
"""
import pandas as pd
from typing import Optional, Dict, List
from datetime import datetime, date, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_
import asyncio
import logging

from app.models import Security, PricesEOD, BenchmarkDefinition, BenchmarkLevel
from app.services.market_data_providers import (
    MarketDataAggregator,
    fetch_prices_with_fallback,
)
from app.core.config import settings

logger = logging.getLogger(__name__)


class MarketDataProvider:
    """Service for fetching and storing market data"""

    def __init__(self, db: Session):
        self.db = db
        self.aggregator = MarketDataAggregator()
        self.rate_limit_delay = 0.3  # seconds between requests

    async def fetch_and_store_prices(
        self,
        security_id: int,
        symbol: str,
        start_date: date,
        end_date: date
    ) -> int:
        """Fetch and store prices for a security"""
        df, source = await self.aggregator.fetch_prices(symbol, start_date, end_date)

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
        if count > 0:
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
        # For benchmarks, try the provider symbol first, then the code itself
        df, source = await self.aggregator.fetch_prices(provider_symbol, start_date, end_date)

        if df is None or df.empty:
            # Try with the benchmark code directly
            df, source = await self.aggregator.fetch_prices(benchmark_code, start_date, end_date)

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
        if count > 0:
            logger.info(f"Stored {count} levels for benchmark {benchmark_code} from {source}")
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

    async def update_factor_etf_prices(self) -> Dict[str, int]:
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
        start_date = end_date - timedelta(days=5*365)

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
