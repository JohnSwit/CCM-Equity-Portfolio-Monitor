"""
UpdateOrchestrator - Coordinates incremental data updates with dependency-aware computation.

Key features:
1. Incremental price fetching (only missing dates)
2. Smart provider selection with coverage tracking
3. Dependency-aware analytics computation
4. Parallel processing where possible
5. Comprehensive observability and metrics
"""
import asyncio
import time
import logging
from datetime import datetime, date, timedelta
from typing import Dict, List, Any, Optional, Tuple, Set
from sqlalchemy.orm import Session
from sqlalchemy import and_, func, or_
from dataclasses import dataclass, field

from app.core.database import SessionLocal
from app.models import (
    Account, Group, Security, Transaction, PricesEOD,
    PositionsEOD, ReturnsEOD, PortfolioValueEOD, RiskEOD,
    BenchmarkLevel, BenchmarkDefinition, FactorRegression, ViewType,
    InceptionPosition, AccountInception
)
from app.models.update_tracking import (
    TickerProviderCoverage, DataUpdateState, ComputationDependency,
    UpdateJobRun, DataProviderStatus, ComputationStatus,
    compute_positions_input_hash, compute_returns_input_hash,
    compute_risk_input_hash, compute_factors_input_hash
)

logger = logging.getLogger(__name__)


@dataclass
class UpdateMetrics:
    """Tracks metrics for an update run"""
    started_at: datetime = field(default_factory=datetime.utcnow)
    completed_at: Optional[datetime] = None

    # Counts
    tickers_processed: int = 0
    tickers_updated: int = 0
    tickers_failed: int = 0
    tickers_skipped: int = 0
    rows_inserted: int = 0
    api_calls_made: int = 0
    cache_hits: int = 0

    # Timing (ms)
    fetch_duration_ms: int = 0
    compute_duration_ms: int = 0
    db_write_duration_ms: int = 0

    # Errors/warnings
    errors: List[Dict[str, Any]] = field(default_factory=list)
    warnings: List[Dict[str, Any]] = field(default_factory=list)

    # Provider stats
    provider_stats: Dict[str, Dict[str, int]] = field(default_factory=dict)

    def add_error(self, entity: str, error: str, provider: str = None):
        self.errors.append({
            'entity': entity,
            'error': str(error),
            'provider': provider,
            'timestamp': datetime.utcnow().isoformat()
        })

    def add_warning(self, entity: str, warning: str):
        self.warnings.append({
            'entity': entity,
            'warning': warning,
            'timestamp': datetime.utcnow().isoformat()
        })

    def to_dict(self) -> Dict[str, Any]:
        return {
            'started_at': self.started_at.isoformat(),
            'completed_at': self.completed_at.isoformat() if self.completed_at else None,
            'duration_seconds': (self.completed_at - self.started_at).total_seconds() if self.completed_at else None,
            'tickers_processed': self.tickers_processed,
            'tickers_updated': self.tickers_updated,
            'tickers_failed': self.tickers_failed,
            'tickers_skipped': self.tickers_skipped,
            'rows_inserted': self.rows_inserted,
            'api_calls_made': self.api_calls_made,
            'cache_hits': self.cache_hits,
            'fetch_duration_ms': self.fetch_duration_ms,
            'compute_duration_ms': self.compute_duration_ms,
            'db_write_duration_ms': self.db_write_duration_ms,
            'error_count': len(self.errors),
            'warning_count': len(self.warnings),
            'provider_stats': self.provider_stats,
        }


class ProviderManager:
    """
    Manages data providers with coverage tracking and smart fallback.
    Provider priority: tiingo > stooq > yfinance
    """

    PROVIDER_PRIORITY = ['tiingo', 'stooq', 'yfinance']
    FAILURE_BACKOFF_HOURS = 24  # Don't retry failed provider for this long

    def __init__(self, db: Session):
        self.db = db
        self._coverage_cache: Dict[str, Dict[str, DataProviderStatus]] = {}

    def get_best_provider(self, symbol: str) -> Optional[str]:
        """Get the best working provider for a symbol"""
        # Check cache first
        if symbol in self._coverage_cache:
            for provider in self.PROVIDER_PRIORITY:
                if self._coverage_cache[symbol].get(provider) == DataProviderStatus.ACTIVE:
                    return provider
            return None

        # Query database
        coverages = self.db.query(TickerProviderCoverage).filter(
            TickerProviderCoverage.symbol == symbol
        ).all()

        symbol_coverage = {}
        for cov in coverages:
            symbol_coverage[cov.provider] = cov.status
            # Check if failed provider should be retried
            if cov.status == DataProviderStatus.FAILED and cov.last_failure:
                hours_since_failure = (datetime.utcnow() - cov.last_failure).total_seconds() / 3600
                if hours_since_failure >= self.FAILURE_BACKOFF_HOURS:
                    symbol_coverage[cov.provider] = DataProviderStatus.UNKNOWN

        self._coverage_cache[symbol] = symbol_coverage

        # Return best active provider
        for provider in self.PROVIDER_PRIORITY:
            status = symbol_coverage.get(provider, DataProviderStatus.UNKNOWN)
            if status in [DataProviderStatus.ACTIVE, DataProviderStatus.UNKNOWN]:
                return provider

        return None

    def get_providers_to_try(self, symbol: str) -> List[str]:
        """Get ordered list of providers to try for a symbol"""
        providers = []
        symbol_coverage = self._coverage_cache.get(symbol, {})

        for provider in self.PROVIDER_PRIORITY:
            status = symbol_coverage.get(provider, DataProviderStatus.UNKNOWN)
            if status == DataProviderStatus.NOT_SUPPORTED:
                continue
            if status == DataProviderStatus.FAILED:
                # Check if enough time has passed
                cov = self.db.query(TickerProviderCoverage).filter(
                    and_(
                        TickerProviderCoverage.symbol == symbol,
                        TickerProviderCoverage.provider == provider
                    )
                ).first()
                if cov and cov.last_failure:
                    hours_since_failure = (datetime.utcnow() - cov.last_failure).total_seconds() / 3600
                    if hours_since_failure < self.FAILURE_BACKOFF_HOURS:
                        continue
            providers.append(provider)

        return providers if providers else self.PROVIDER_PRIORITY[:1]  # At least try primary

    def record_success(self, symbol: str, provider: str, records_fetched: int):
        """Record a successful fetch"""
        coverage = self._get_or_create_coverage(symbol, provider)
        coverage.status = DataProviderStatus.ACTIVE
        coverage.last_success = datetime.utcnow()
        coverage.failure_count = 0
        coverage.records_fetched += records_fetched
        coverage.last_error = None
        self.db.commit()

        # Update cache
        if symbol not in self._coverage_cache:
            self._coverage_cache[symbol] = {}
        self._coverage_cache[symbol][provider] = DataProviderStatus.ACTIVE

    def record_failure(self, symbol: str, provider: str, error: str):
        """Record a failed fetch"""
        coverage = self._get_or_create_coverage(symbol, provider)
        coverage.failure_count += 1
        coverage.last_failure = datetime.utcnow()
        coverage.last_error = error[:500]  # Truncate error

        # Mark as not_supported after multiple failures, otherwise just failed
        if coverage.failure_count >= 3:
            coverage.status = DataProviderStatus.NOT_SUPPORTED
        else:
            coverage.status = DataProviderStatus.FAILED

        self.db.commit()

        # Update cache
        if symbol not in self._coverage_cache:
            self._coverage_cache[symbol] = {}
        self._coverage_cache[symbol][provider] = coverage.status

    def _get_or_create_coverage(self, symbol: str, provider: str) -> TickerProviderCoverage:
        """Get or create coverage record"""
        coverage = self.db.query(TickerProviderCoverage).filter(
            and_(
                TickerProviderCoverage.symbol == symbol,
                TickerProviderCoverage.provider == provider
            )
        ).first()

        if not coverage:
            coverage = TickerProviderCoverage(
                symbol=symbol,
                provider=provider,
                status=DataProviderStatus.UNKNOWN
            )
            self.db.add(coverage)
            self.db.flush()

        return coverage


class DependencyTracker:
    """
    Tracks computation dependencies and determines what needs recomputation.
    """

    def __init__(self, db: Session):
        self.db = db
        self._hash_cache: Dict[Tuple[str, str, int], str] = {}

    def get_or_create_dependency(
        self,
        computation_type: str,
        view_type: str,
        view_id: int
    ) -> ComputationDependency:
        """Get or create a computation dependency record"""
        key = (computation_type, view_type, view_id)

        dep = self.db.query(ComputationDependency).filter(
            and_(
                ComputationDependency.computation_type == computation_type,
                ComputationDependency.view_type == view_type,
                ComputationDependency.view_id == view_id
            )
        ).first()

        if not dep:
            dep = ComputationDependency(
                computation_type=computation_type,
                view_type=view_type,
                view_id=view_id,
                input_hash='',
                status=ComputationStatus.PENDING
            )
            self.db.add(dep)
            self.db.flush()

        return dep

    def needs_recomputation(
        self,
        computation_type: str,
        view_type: str,
        view_id: int,
        new_input_hash: str
    ) -> bool:
        """Check if computation needs to be rerun based on input hash"""
        dep = self.get_or_create_dependency(computation_type, view_type, view_id)

        if dep.input_hash != new_input_hash:
            return True

        if dep.status in [ComputationStatus.PENDING, ComputationStatus.FAILED]:
            return True

        return False

    def mark_started(
        self,
        computation_type: str,
        view_type: str,
        view_id: int,
        input_hash: str
    ):
        """Mark a computation as started"""
        dep = self.get_or_create_dependency(computation_type, view_type, view_id)
        dep.status = ComputationStatus.RUNNING
        dep.input_hash = input_hash
        self.db.commit()

    def mark_completed(
        self,
        computation_type: str,
        view_type: str,
        view_id: int,
        duration_ms: int,
        output_hash: str = None
    ):
        """Mark a computation as completed"""
        dep = self.get_or_create_dependency(computation_type, view_type, view_id)
        dep.status = ComputationStatus.COMPLETED
        dep.last_computed = datetime.utcnow()
        dep.compute_duration_ms = duration_ms
        dep.output_hash = output_hash
        dep.error_message = None
        self.db.commit()

    def mark_failed(
        self,
        computation_type: str,
        view_type: str,
        view_id: int,
        error: str
    ):
        """Mark a computation as failed"""
        dep = self.get_or_create_dependency(computation_type, view_type, view_id)
        dep.status = ComputationStatus.FAILED
        dep.error_message = error[:1000]
        self.db.commit()

    def mark_skipped(
        self,
        computation_type: str,
        view_type: str,
        view_id: int
    ):
        """Mark a computation as skipped (inputs unchanged)"""
        dep = self.get_or_create_dependency(computation_type, view_type, view_id)
        dep.status = ComputationStatus.SKIPPED
        self.db.commit()


class UpdateOrchestrator:
    """
    Main orchestrator for incremental data updates.
    Coordinates market data fetching and analytics computation.
    """

    # Configuration
    BATCH_SIZE = 50  # Number of tickers to process in parallel
    MAX_CONCURRENT_REQUESTS = 10  # Max concurrent API calls
    RATE_LIMIT_DELAY = 0.1  # Seconds between API calls

    def __init__(self, db: Session):
        self.db = db
        self.provider_manager = ProviderManager(db)
        self.dependency_tracker = DependencyTracker(db)
        self.metrics = UpdateMetrics()

    async def run_full_update(self, force_refresh: bool = False) -> UpdateMetrics:
        """
        Run a full incremental update:
        1. Fetch missing market data
        2. Recompute analytics only where inputs changed
        """
        self.metrics = UpdateMetrics()
        job_run = self._create_job_run('full')

        try:
            logger.info("=" * 60)
            logger.info("Starting full incremental update")
            logger.info("=" * 60)

            # Phase 1: Market Data
            logger.info("\n--- Phase 1: Market Data ---")
            await self._update_market_data(force_refresh)

            # Phase 2: Analytics (dependency-aware)
            logger.info("\n--- Phase 2: Analytics ---")
            await self._update_analytics()

            self.metrics.completed_at = datetime.utcnow()
            job_run.status = 'completed'

            logger.info("\n" + "=" * 60)
            logger.info("Update completed successfully")
            self._log_summary()
            logger.info("=" * 60)

        except Exception as e:
            logger.error(f"Update failed: {e}", exc_info=True)
            job_run.status = 'failed'
            self.metrics.add_error('orchestrator', str(e))
            raise
        finally:
            self._finalize_job_run(job_run)

        return self.metrics

    async def run_market_data_update(self, force_refresh: bool = False) -> UpdateMetrics:
        """Run only market data update (no analytics)"""
        self.metrics = UpdateMetrics()
        job_run = self._create_job_run('market_data')

        try:
            logger.info("Starting market data update")
            await self._update_market_data(force_refresh)
            self.metrics.completed_at = datetime.utcnow()
            job_run.status = 'completed'
            self._log_summary()
        except Exception as e:
            logger.error(f"Market data update failed: {e}", exc_info=True)
            job_run.status = 'failed'
            self.metrics.add_error('orchestrator', str(e))
            raise
        finally:
            self._finalize_job_run(job_run)

        return self.metrics

    async def run_analytics_update(self) -> UpdateMetrics:
        """Run only analytics update (no data fetching)"""
        self.metrics = UpdateMetrics()
        job_run = self._create_job_run('analytics')

        try:
            logger.info("Starting analytics update")
            await self._update_analytics()
            self.metrics.completed_at = datetime.utcnow()
            job_run.status = 'completed'
            self._log_summary()
        except Exception as e:
            logger.error(f"Analytics update failed: {e}", exc_info=True)
            job_run.status = 'failed'
            self.metrics.add_error('orchestrator', str(e))
            raise
        finally:
            self._finalize_job_run(job_run)

        return self.metrics

    async def _update_market_data(self, force_refresh: bool = False):
        """Update all market data incrementally"""
        start_time = time.time()

        # Get securities that need price updates
        securities = self._get_securities_needing_update()
        logger.info(f"Found {len(securities)} securities to check for updates")

        # Process in batches
        for i in range(0, len(securities), self.BATCH_SIZE):
            batch = securities[i:i + self.BATCH_SIZE]
            await self._process_security_batch(batch, force_refresh)

        # Update benchmarks
        await self._update_benchmarks()

        # Update factor ETFs
        await self._update_factor_etfs()

        self.metrics.fetch_duration_ms = int((time.time() - start_time) * 1000)

    async def _process_security_batch(
        self,
        securities: List[Security],
        force_refresh: bool = False
    ):
        """Process a batch of securities concurrently"""
        from app.services.market_data import MarketDataProvider

        market_data = MarketDataProvider(self.db)

        semaphore = asyncio.Semaphore(self.MAX_CONCURRENT_REQUESTS)

        async def process_one(security: Security):
            async with semaphore:
                await self._update_security_prices(market_data, security, force_refresh)

        tasks = [process_one(s) for s in securities]
        await asyncio.gather(*tasks, return_exceptions=True)

    async def _update_security_prices(
        self,
        market_data: 'MarketDataProvider',
        security: Security,
        force_refresh: bool = False
    ):
        """Update prices for a single security with smart provider selection"""
        symbol = security.symbol
        self.metrics.tickers_processed += 1

        try:
            # Check what dates we need
            update_state = self._get_update_state('security_price', symbol)
            today = date.today()

            if not force_refresh and update_state.last_update_date:
                if update_state.last_update_date >= today - timedelta(days=1):
                    # Already up to date
                    self.metrics.tickers_skipped += 1
                    self.metrics.cache_hits += 1
                    return

            # Get date range to fetch
            first_txn = self.db.query(Transaction).filter(
                Transaction.security_id == security.id
            ).order_by(Transaction.trade_date).first()

            if not first_txn:
                self.metrics.tickers_skipped += 1
                return

            start_date = first_txn.trade_date
            if not force_refresh and update_state.last_update_date:
                start_date = max(start_date, update_state.last_update_date + timedelta(days=1))

            if start_date > today:
                self.metrics.tickers_skipped += 1
                return

            # Fetch using smart provider selection
            providers = self.provider_manager.get_providers_to_try(symbol)
            success = False

            for provider in providers:
                try:
                    self.metrics.api_calls_made += 1
                    count = await market_data.fetch_and_store_prices(
                        security.id, symbol, start_date, today, force_refresh
                    )

                    if count > 0:
                        self.provider_manager.record_success(symbol, provider, count)
                        self._update_state_success('security_price', symbol, today)
                        self.metrics.tickers_updated += 1
                        self.metrics.rows_inserted += count
                        success = True
                        break
                    elif count == 0:
                        # No new data but no error - might be up to date
                        self._update_state_success('security_price', symbol, today)
                        self.metrics.tickers_skipped += 1
                        success = True
                        break

                except Exception as e:
                    self.provider_manager.record_failure(symbol, provider, str(e))
                    continue

            if not success:
                self.metrics.tickers_failed += 1
                self.metrics.add_error(symbol, "All providers failed")

            # Rate limiting
            await asyncio.sleep(self.RATE_LIMIT_DELAY)

        except Exception as e:
            logger.error(f"Error updating {symbol}: {e}")
            self.metrics.tickers_failed += 1
            self.metrics.add_error(symbol, str(e))

    async def _update_benchmarks(self):
        """Update benchmark price data"""
        from app.services.market_data import MarketDataProvider

        market_data = MarketDataProvider(self.db)
        benchmarks = self.db.query(BenchmarkDefinition).all()

        end_date = date.today()

        # Get start date from earliest transaction to match portfolio history
        earliest_txn = self.db.query(func.min(Transaction.trade_date)).scalar()
        if earliest_txn:
            start_date = earliest_txn - timedelta(days=30)
        else:
            start_date = end_date - timedelta(days=25*365)  # Default 25 years

        for benchmark in benchmarks:
            try:
                # Check if we need update
                latest = self.db.query(func.max(BenchmarkLevel.date)).filter(
                    BenchmarkLevel.code == benchmark.code
                ).scalar()

                if latest and latest >= end_date - timedelta(days=1):
                    logger.debug(f"Benchmark {benchmark.code} already up to date")
                    continue

                fetch_start = latest + timedelta(days=1) if latest else start_date

                self.metrics.api_calls_made += 1
                count = await market_data.fetch_and_store_benchmark_prices(
                    benchmark.code, benchmark.provider_symbol, fetch_start, end_date
                )

                if count > 0:
                    self.metrics.rows_inserted += count
                    logger.info(f"Updated {count} benchmark levels for {benchmark.code}")

                await asyncio.sleep(self.RATE_LIMIT_DELAY)

            except Exception as e:
                logger.error(f"Failed to update benchmark {benchmark.code}: {e}")
                self.metrics.add_error(f"benchmark:{benchmark.code}", str(e))

    async def _update_factor_etfs(self):
        """Update factor ETF prices"""
        from app.services.market_data import MarketDataProvider
        from app.models import AssetClass

        market_data = MarketDataProvider(self.db)
        factor_etfs = ['SPY', 'IWM', 'IVE', 'IVW', 'QUAL', 'SPLV', 'MTUM', 'QQQ']

        end_date = date.today()

        # Get start date from earliest transaction to match portfolio history
        earliest_txn = self.db.query(func.min(Transaction.trade_date)).scalar()
        if earliest_txn:
            start_date = earliest_txn - timedelta(days=30)
        else:
            start_date = end_date - timedelta(days=25*365)  # Default 25 years

        for symbol in factor_etfs:
            try:
                # Get or create security
                security = self.db.query(Security).filter(
                    Security.symbol == symbol
                ).first()

                if not security:
                    security = Security(
                        symbol=symbol,
                        asset_name=f"{symbol} ETF",
                        asset_class=AssetClass.ETF
                    )
                    self.db.add(security)
                    self.db.flush()

                # Check if we need update
                latest = self.db.query(func.max(PricesEOD.date)).filter(
                    PricesEOD.security_id == security.id
                ).scalar()

                if latest and latest >= end_date - timedelta(days=1):
                    continue

                fetch_start = latest + timedelta(days=1) if latest else start_date

                self.metrics.api_calls_made += 1
                count = await market_data.fetch_and_store_prices(
                    security.id, symbol, fetch_start, end_date
                )

                if count > 0:
                    self.metrics.rows_inserted += count

                await asyncio.sleep(self.RATE_LIMIT_DELAY)

            except Exception as e:
                logger.error(f"Failed to update factor ETF {symbol}: {e}")
                self.metrics.add_error(f"factor_etf:{symbol}", str(e))

    async def _update_analytics(self, use_batch_service: bool = True):
        """
        Update analytics with dependency awareness.

        Args:
            use_batch_service: Use optimized BatchAnalyticsService for bulk operations (default True)
        """
        start_time = time.time()
        from sqlalchemy import or_
        from app.models import AccountInception

        # Get accounts that have transactions OR inception data (skip truly orphaned accounts)
        accounts_with_txns = self.db.query(Transaction.account_id).distinct().subquery()
        accounts_with_inception = self.db.query(AccountInception.account_id).distinct().subquery()
        accounts = self.db.query(Account).filter(
            or_(
                Account.id.in_(accounts_with_txns),
                Account.id.in_(accounts_with_inception)
            )
        ).all()

        groups = self.db.query(Group).all()
        as_of_date = date.today()

        logger.info(f"Processing analytics for {len(accounts)} accounts with transactions or inception")

        # Seed inception prices into PricesEOD before any computation.
        # Without prices on the inception date, portfolio value = $0 and returns break.
        self._seed_inception_prices()

        if use_batch_service:
            # Use optimized batch service for positions, values, and returns
            from app.services.analytics_batch import BatchAnalyticsService

            logger.info("Using BatchAnalyticsService for optimized analytics computation")

            batch_service = BatchAnalyticsService(self.db)
            batch_result = batch_service.run_full_analytics()
            logger.info(f"Batch analytics result: {batch_result}")

            # Compute group rollups
            from app.services.groups import GroupsEngine
            groups_engine = GroupsEngine(self.db)
            groups_results = groups_engine.compute_all_groups()
            logger.info(f"Groups computed: {groups_results}")

            # Compute benchmark returns
            from app.services.benchmarks import BenchmarksEngine
            benchmarks_engine = BenchmarksEngine(self.db)
            benchmarks_engine.ensure_default_benchmarks()
            benchmark_results = benchmarks_engine.compute_all_benchmark_returns()
            logger.info(f"Benchmarks computed: {benchmark_results}")

            # Compute benchmark metrics for all views
            for account in accounts:
                for benchmark_code in ['SPY', 'QQQ', 'INDU']:
                    try:
                        benchmarks_engine.compute_benchmark_metrics(
                            ViewType.ACCOUNT, account.id, benchmark_code, as_of_date
                        )
                    except Exception:
                        pass

            for group in groups:
                for benchmark_code in ['SPY', 'QQQ', 'INDU']:
                    try:
                        benchmarks_engine.compute_benchmark_metrics(
                            ViewType.GROUP, group.id, benchmark_code, as_of_date
                        )
                    except Exception:
                        pass

            # Compute factor returns and regressions
            from app.services.factors import FactorsEngine
            factors_engine = FactorsEngine(self.db)
            factors_engine.ensure_style7_factor_set()
            factor_returns_count = factors_engine.compute_factor_returns()
            logger.info(f"Factor returns computed: {factor_returns_count}")

            for account in accounts:
                try:
                    factors_engine.compute_factor_regression(ViewType.ACCOUNT, account.id, as_of_date)
                except Exception:
                    pass

            for group in groups:
                try:
                    factors_engine.compute_factor_regression(ViewType.GROUP, group.id, as_of_date)
                except Exception:
                    pass

            # Compute risk metrics
            from app.services.risk import RiskEngine
            risk_engine = RiskEngine(self.db)
            risk_results = risk_engine.compute_all_risk_metrics(as_of_date)
            logger.info(f"Risk metrics computed: {risk_results}")
        else:
            # Legacy path: process accounts one by one with dependency tracking
            for account in accounts:
                await self._compute_account_analytics(account, as_of_date)

            # Process groups (after accounts)
            for group in groups:
                await self._compute_group_analytics(group, as_of_date)

        self.metrics.compute_duration_ms = int((time.time() - start_time) * 1000)

    async def _compute_account_analytics(self, account: Account, as_of_date: date):
        """Compute analytics for an account if inputs changed"""
        from app.services.positions import PositionsEngine
        from app.services.returns import ReturnsEngine
        from app.services.risk import RiskEngine
        from app.services.factors import FactorsEngine
        from app.services.benchmarks import BenchmarksEngine
        from app.models import AccountInception

        view_type = 'account'
        view_id = account.id

        try:
            # 1. Positions (depends on transactions and/or inception data)
            transaction_ids = [t.id for t in self.db.query(Transaction.id).filter(
                Transaction.account_id == account.id
            ).all()]

            # Check if account has inception data
            has_inception = self.db.query(AccountInception).filter(
                AccountInception.account_id == account.id
            ).first() is not None

            if not transaction_ids and not has_inception:
                return  # No transactions and no inception data, nothing to compute

            last_txn_date = self.db.query(func.max(Transaction.trade_date)).filter(
                Transaction.account_id == account.id
            ).scalar()

            # Include inception in hash to trigger recomputation when inception changes
            inception_id = None
            if has_inception:
                inception = self.db.query(AccountInception).filter(
                    AccountInception.account_id == account.id
                ).first()
                if inception:
                    inception_id = inception.id

            positions_hash = compute_positions_input_hash(
                account.id, transaction_ids, last_txn_date, inception_id
            )

            # Always compute positions for accounts with inception data to ensure inception
            # securities are included (inception data doesn't change often so this is fine)
            needs_recompute = has_inception or self.dependency_tracker.needs_recomputation(
                'positions', view_type, view_id, positions_hash
            )

            if needs_recompute:
                start = time.time()
                self.dependency_tracker.mark_started('positions', view_type, view_id, positions_hash)

                positions_engine = PositionsEngine(self.db)
                positions_engine.build_positions_for_account(account.id)

                duration = int((time.time() - start) * 1000)
                self.dependency_tracker.mark_completed('positions', view_type, view_id, duration)
                logger.info(f"Recomputed positions for account {account.id} ({duration}ms)")
            else:
                self.dependency_tracker.mark_skipped('positions', view_type, view_id)

            # 2. Returns (depends on positions + prices)
            prices_last_date = self.db.query(func.max(PricesEOD.date)).scalar() or date.today()
            positions_dep = self.dependency_tracker.get_or_create_dependency(
                'positions', view_type, view_id
            )

            returns_hash = compute_returns_input_hash(
                view_type, view_id, positions_dep.output_hash or positions_hash, prices_last_date
            )

            if self.dependency_tracker.needs_recomputation(
                'returns', view_type, view_id, returns_hash
            ):
                start = time.time()
                self.dependency_tracker.mark_started('returns', view_type, view_id, returns_hash)

                returns_engine = ReturnsEngine(self.db)
                returns_engine.compute_portfolio_values_for_account(account.id)
                returns_engine.compute_returns_for_account(account.id)

                duration = int((time.time() - start) * 1000)
                self.dependency_tracker.mark_completed('returns', view_type, view_id, duration)
                logger.info(f"Recomputed returns for account {account.id} ({duration}ms)")
            else:
                self.dependency_tracker.mark_skipped('returns', view_type, view_id)

            # 3. Risk (depends on returns)
            returns_dep = self.dependency_tracker.get_or_create_dependency(
                'returns', view_type, view_id
            )
            risk_hash = compute_risk_input_hash(
                view_type, view_id, returns_dep.output_hash or returns_hash, as_of_date
            )

            if self.dependency_tracker.needs_recomputation(
                'risk', view_type, view_id, risk_hash
            ):
                start = time.time()
                self.dependency_tracker.mark_started('risk', view_type, view_id, risk_hash)

                risk_engine = RiskEngine(self.db)
                risk_engine.compute_risk_for_view(ViewType.ACCOUNT, account.id, as_of_date)

                duration = int((time.time() - start) * 1000)
                self.dependency_tracker.mark_completed('risk', view_type, view_id, duration)
                logger.info(f"Recomputed risk for account {account.id} ({duration}ms)")
            else:
                self.dependency_tracker.mark_skipped('risk', view_type, view_id)

            # 4. Factor regressions (depends on returns + factor prices)
            factor_prices_last = self.db.query(func.max(PricesEOD.date)).join(Security).filter(
                Security.symbol.in_(['SPY', 'IWM', 'IVE', 'IVW', 'QUAL', 'SPLV', 'MTUM'])
            ).scalar() or date.today()

            factors_hash = compute_factors_input_hash(
                view_type, view_id, returns_dep.output_hash or returns_hash,
                factor_prices_last, 'STYLE7'
            )

            if self.dependency_tracker.needs_recomputation(
                'factors', view_type, view_id, factors_hash
            ):
                start = time.time()
                self.dependency_tracker.mark_started('factors', view_type, view_id, factors_hash)

                factors_engine = FactorsEngine(self.db)
                factors_engine.compute_factor_regression(ViewType.ACCOUNT, account.id, as_of_date)

                duration = int((time.time() - start) * 1000)
                self.dependency_tracker.mark_completed('factors', view_type, view_id, duration)
                logger.info(f"Recomputed factors for account {account.id} ({duration}ms)")
            else:
                self.dependency_tracker.mark_skipped('factors', view_type, view_id)

            # 5. Benchmark metrics
            benchmarks_engine = BenchmarksEngine(self.db)
            for benchmark_code in ['SPY', 'QQQ', 'INDU']:
                try:
                    benchmarks_engine.compute_benchmark_metrics(
                        ViewType.ACCOUNT, account.id, benchmark_code, as_of_date
                    )
                except Exception as e:
                    logger.error(f"Benchmark metrics failed for account {account.id}: {e}")

        except Exception as e:
            logger.error(f"Analytics failed for account {account.id}: {e}", exc_info=True)
            self.metrics.add_error(f"account:{account.id}", str(e))

    async def _compute_group_analytics(self, group: Group, as_of_date: date):
        """Compute analytics for a group"""
        from app.services.groups import GroupsEngine
        from app.services.risk import RiskEngine
        from app.services.factors import FactorsEngine
        from app.services.benchmarks import BenchmarksEngine

        try:
            # Groups always need recomputation after account updates
            # (simplified - could add more sophisticated dependency tracking)
            groups_engine = GroupsEngine(self.db)
            groups_engine.compute_group(group.id)

            risk_engine = RiskEngine(self.db)
            risk_engine.compute_risk_for_view(ViewType.GROUP, group.id, as_of_date)

            factors_engine = FactorsEngine(self.db)
            factors_engine.compute_factor_regression(ViewType.GROUP, group.id, as_of_date)

            benchmarks_engine = BenchmarksEngine(self.db)
            for benchmark_code in ['SPY', 'QQQ', 'INDU']:
                try:
                    benchmarks_engine.compute_benchmark_metrics(
                        ViewType.GROUP, group.id, benchmark_code, as_of_date
                    )
                except Exception:
                    pass

        except Exception as e:
            logger.error(f"Analytics failed for group {group.id}: {e}")
            self.metrics.add_error(f"group:{group.id}", str(e))

    def _seed_inception_prices(self):
        """
        Ensure PricesEOD records exist for all inception positions on their inception date.
        Without this, portfolio value on inception date = $0 and returns break.
        """
        inception_data = self.db.query(
            InceptionPosition.security_id,
            InceptionPosition.price,
            AccountInception.inception_date
        ).join(
            AccountInception, InceptionPosition.inception_id == AccountInception.id
        ).filter(
            InceptionPosition.price > 0
        ).all()

        if not inception_data:
            return

        price_map = {}
        for security_id, price, inception_date in inception_data:
            key = (security_id, inception_date)
            if key not in price_map:
                price_map[key] = price

        created = 0
        for (security_id, inception_date), price in price_map.items():
            existing = self.db.query(PricesEOD.id).filter(
                and_(
                    PricesEOD.security_id == security_id,
                    PricesEOD.date == inception_date
                )
            ).first()

            if not existing:
                price_eod = PricesEOD(
                    security_id=security_id,
                    date=inception_date,
                    close=price,
                    source='inception'
                )
                self.db.add(price_eod)
                created += 1

        if created > 0:
            self.db.commit()
            logger.info(f"Seeded {created} inception prices into PricesEOD")

    def _get_securities_needing_update(self) -> List[Security]:
        """Get list of securities that need price updates (from transactions or inception)"""
        # Get securities with transactions
        txn_security_ids = set(
            r[0] for r in self.db.query(Transaction.security_id).distinct().all()
            if r[0] is not None
        )

        # Get securities with inception positions
        inception_security_ids = set(
            r[0] for r in self.db.query(InceptionPosition.security_id).distinct().all()
        )

        # Combine both sets
        all_security_ids = txn_security_ids.union(inception_security_ids)

        return self.db.query(Security).filter(Security.id.in_(all_security_ids)).all()

    def _get_update_state(self, entity_type: str, entity_id: str) -> DataUpdateState:
        """Get or create update state for an entity"""
        state = self.db.query(DataUpdateState).filter(
            and_(
                DataUpdateState.entity_type == entity_type,
                DataUpdateState.entity_id == entity_id
            )
        ).first()

        if not state:
            state = DataUpdateState(
                entity_type=entity_type,
                entity_id=entity_id
            )
            self.db.add(state)
            self.db.flush()

        return state

    def _update_state_success(self, entity_type: str, entity_id: str, update_date: date):
        """Update state after successful fetch"""
        state = self._get_update_state(entity_type, entity_id)
        state.last_update_date = update_date
        state.last_update_timestamp = datetime.utcnow()
        self.db.commit()

    def _create_job_run(self, job_type: str) -> UpdateJobRun:
        """Create a new job run record"""
        job_run = UpdateJobRun(
            job_type=job_type,
            started_at=self.metrics.started_at
        )
        self.db.add(job_run)
        self.db.commit()
        return job_run

    def _finalize_job_run(self, job_run: UpdateJobRun):
        """Finalize job run with metrics"""
        job_run.completed_at = datetime.utcnow()
        job_run.tickers_processed = self.metrics.tickers_processed
        job_run.tickers_updated = self.metrics.tickers_updated
        job_run.tickers_failed = self.metrics.tickers_failed
        job_run.tickers_skipped = self.metrics.tickers_skipped
        job_run.rows_inserted = self.metrics.rows_inserted
        job_run.api_calls_made = self.metrics.api_calls_made
        job_run.cache_hits = self.metrics.cache_hits
        job_run.fetch_duration_ms = self.metrics.fetch_duration_ms
        job_run.compute_duration_ms = self.metrics.compute_duration_ms
        job_run.errors_json = self.metrics.errors
        job_run.warnings_json = self.metrics.warnings
        job_run.summary_json = self.metrics.to_dict()
        self.db.commit()

    def _log_summary(self):
        """Log update summary"""
        logger.info("\n=== Update Summary ===")
        logger.info(f"Tickers processed: {self.metrics.tickers_processed}")
        logger.info(f"  Updated: {self.metrics.tickers_updated}")
        logger.info(f"  Skipped (cached): {self.metrics.tickers_skipped}")
        logger.info(f"  Failed: {self.metrics.tickers_failed}")
        logger.info(f"Rows inserted: {self.metrics.rows_inserted}")
        logger.info(f"API calls: {self.metrics.api_calls_made}")
        logger.info(f"Cache hits: {self.metrics.cache_hits}")
        logger.info(f"Fetch time: {self.metrics.fetch_duration_ms}ms")
        logger.info(f"Compute time: {self.metrics.compute_duration_ms}ms")
        if self.metrics.errors:
            logger.warning(f"Errors: {len(self.metrics.errors)}")
            for err in self.metrics.errors[:5]:  # Show first 5
                logger.warning(f"  - {err['entity']}: {err['error']}")
