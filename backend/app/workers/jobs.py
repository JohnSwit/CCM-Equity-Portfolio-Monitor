import asyncio
from sqlalchemy.orm import Session
from datetime import date
from app.core.database import SessionLocal
from app.services.market_data import MarketDataProvider
from app.services.positions import PositionsEngine
from app.services.returns import ReturnsEngine
from app.services.groups import GroupsEngine
from app.services.benchmarks import BenchmarksEngine
from app.services.baskets import BasketsEngine
from app.services.factors import FactorsEngine
from app.services.risk import RiskEngine
from app.services.data_sourcing import BenchmarkService, ClassificationService
from app.models import (
    Account, Group, ViewType, Transaction,
    PositionsEOD, PortfolioValueEOD, ReturnsEOD, RiskEOD,
    BenchmarkMetric, FactorRegression
)
from app.models.sector_models import BenchmarkConstituent, SectorClassification
from sqlalchemy import func
from datetime import date, datetime, timedelta
import logging

logger = logging.getLogger(__name__)

# Flag to enable new incremental update system
USE_INCREMENTAL_UPDATES = True

# Cache freshness threshold in hours - skip refresh if data is newer than this
DATA_FRESHNESS_HOURS = 12


def is_benchmark_data_fresh(db: Session, benchmark_code: str = "SP500") -> bool:
    """
    Check if benchmark constituent data is fresh AND has adequate data.
    Returns True if data is recent AND has enough constituents.
    """
    latest_update = db.query(func.max(BenchmarkConstituent.updated_at)).filter(
        BenchmarkConstituent.benchmark_code == benchmark_code
    ).scalar()

    if not latest_update:
        logger.info(f"No benchmark data found for {benchmark_code} - needs refresh")
        return False

    age_hours = (datetime.utcnow() - latest_update).total_seconds() / 3600

    if age_hours >= DATA_FRESHNESS_HOURS:
        logger.info(f"Benchmark {benchmark_code} data is stale ({age_hours:.1f}h old) - will refresh")
        return False

    # Check we have adequate constituent count (S&P 500 should have ~500)
    constituent_count = db.query(func.count(BenchmarkConstituent.id)).filter(
        BenchmarkConstituent.benchmark_code == benchmark_code
    ).scalar() or 0

    if constituent_count < 400:  # S&P 500 should have ~500
        logger.info(f"Benchmark {benchmark_code} has too few constituents ({constituent_count}) - will refresh")
        return False

    logger.info(f"Benchmark {benchmark_code} data is fresh ({age_hours:.1f}h old, {constituent_count} constituents) - skipping refresh")
    return True


def is_classification_data_fresh(db: Session) -> bool:
    """
    Check if classification data is fresh AND complete.
    Returns True only if data is recent AND covers most securities.
    """
    from app.models import Security

    latest_update = db.query(func.max(SectorClassification.updated_at)).scalar()

    if not latest_update:
        logger.info("No classification data found - needs refresh")
        return False

    age_hours = (datetime.utcnow() - latest_update).total_seconds() / 3600

    if age_hours >= DATA_FRESHNESS_HOURS:
        logger.info(f"Classification data is stale ({age_hours:.1f}h old) - will refresh")
        return False

    # Also check coverage - ensure we have classifications for most securities
    total_securities = db.query(func.count(Security.id)).scalar() or 0
    classified_securities = db.query(func.count(SectorClassification.id)).scalar() or 0

    if total_securities > 0:
        coverage = classified_securities / total_securities
        if coverage < 0.5:  # Less than 50% coverage
            logger.info(f"Classification coverage too low ({coverage:.1%}) - will refresh")
            return False

    logger.info(f"Classification data is fresh ({age_hours:.1f}h old, {classified_securities}/{total_securities} securities) - skipping refresh")
    return True


def clear_analytics_for_accounts_without_transactions(db: Session):
    """
    Clear all analytics data for accounts that have no transactions.
    This handles the case where all transactions for an account were deleted.
    """
    logger.info("Clearing analytics for accounts without transactions...")

    # Get all account IDs
    all_account_ids = [acc.id for acc in db.query(Account.id).all()]

    # Get account IDs that have transactions
    accounts_with_txns = set([
        txn.account_id for txn in db.query(Transaction.account_id).distinct().all()
    ])

    # Find accounts with no transactions
    accounts_to_clear = [acc_id for acc_id in all_account_ids if acc_id not in accounts_with_txns]

    if not accounts_to_clear:
        logger.info("No accounts without transactions found")
        return

    logger.info(f"Clearing analytics for {len(accounts_to_clear)} accounts without transactions")

    # Clear positions
    deleted_positions = db.query(PositionsEOD).filter(
        PositionsEOD.account_id.in_(accounts_to_clear)
    ).delete(synchronize_session=False)
    logger.info(f"Deleted {deleted_positions} positions")

    # Clear portfolio values
    deleted_values = db.query(PortfolioValueEOD).filter(
        PortfolioValueEOD.view_type == ViewType.ACCOUNT,
        PortfolioValueEOD.view_id.in_(accounts_to_clear)
    ).delete(synchronize_session=False)
    logger.info(f"Deleted {deleted_values} portfolio values")

    # Clear returns
    deleted_returns = db.query(ReturnsEOD).filter(
        ReturnsEOD.view_type == ViewType.ACCOUNT,
        ReturnsEOD.view_id.in_(accounts_to_clear)
    ).delete(synchronize_session=False)
    logger.info(f"Deleted {deleted_returns} returns")

    # Clear risk metrics
    deleted_risk = db.query(RiskEOD).filter(
        RiskEOD.view_type == ViewType.ACCOUNT,
        RiskEOD.view_id.in_(accounts_to_clear)
    ).delete(synchronize_session=False)
    logger.info(f"Deleted {deleted_risk} risk metrics")

    # Clear benchmark metrics
    deleted_benchmarks = db.query(BenchmarkMetric).filter(
        BenchmarkMetric.view_type == ViewType.ACCOUNT,
        BenchmarkMetric.view_id.in_(accounts_to_clear)
    ).delete(synchronize_session=False)
    logger.info(f"Deleted {deleted_benchmarks} benchmark metrics")

    # Clear factor regressions
    deleted_factors = db.query(FactorRegression).filter(
        FactorRegression.view_type == ViewType.ACCOUNT,
        FactorRegression.view_id.in_(accounts_to_clear)
    ).delete(synchronize_session=False)
    logger.info(f"Deleted {deleted_factors} factor regressions")

    db.commit()
    logger.info("Analytics cleared successfully")


def clear_analytics_for_account(db: Session, account_id: int):
    """
    Clear all analytics data for a specific account.
    Use this when transactions are deleted for an account.
    """
    logger.info(f"Clearing analytics for account {account_id}...")

    # Clear positions
    db.query(PositionsEOD).filter(
        PositionsEOD.account_id == account_id
    ).delete(synchronize_session=False)

    # Clear portfolio values
    db.query(PortfolioValueEOD).filter(
        PortfolioValueEOD.view_type == ViewType.ACCOUNT,
        PortfolioValueEOD.view_id == account_id
    ).delete(synchronize_session=False)

    # Clear returns
    db.query(ReturnsEOD).filter(
        ReturnsEOD.view_type == ViewType.ACCOUNT,
        ReturnsEOD.view_id == account_id
    ).delete(synchronize_session=False)

    # Clear risk metrics
    db.query(RiskEOD).filter(
        RiskEOD.view_type == ViewType.ACCOUNT,
        RiskEOD.view_id == account_id
    ).delete(synchronize_session=False)

    # Clear benchmark metrics
    db.query(BenchmarkMetric).filter(
        BenchmarkMetric.view_type == ViewType.ACCOUNT,
        BenchmarkMetric.view_id == account_id
    ).delete(synchronize_session=False)

    # Clear factor regressions
    db.query(FactorRegression).filter(
        FactorRegression.view_type == ViewType.ACCOUNT,
        FactorRegression.view_id == account_id
    ).delete(synchronize_session=False)

    db.commit()
    logger.info(f"Analytics cleared for account {account_id}")


def clear_all_returns(db: Session):
    """
    Clear ALL returns data for fresh recomputation.
    Use this when the TWR index convention has changed.
    """
    logger.info("Clearing ALL returns data for fresh recomputation...")

    # Clear all account returns
    deleted_account_returns = db.query(ReturnsEOD).filter(
        ReturnsEOD.view_type == ViewType.ACCOUNT
    ).delete(synchronize_session=False)
    logger.info(f"Deleted {deleted_account_returns} account returns")

    # Clear all group/firm returns
    deleted_group_returns = db.query(ReturnsEOD).filter(
        ReturnsEOD.view_type.in_([ViewType.GROUP, ViewType.FIRM])
    ).delete(synchronize_session=False)
    logger.info(f"Deleted {deleted_group_returns} group/firm returns")

    db.commit()
    logger.info("All returns data cleared")


def clear_group_and_firm_analytics(db: Session):
    """
    Clear all group and firm level analytics.
    These need to be recomputed from scratch after account changes.
    """
    logger.info("Clearing group and firm analytics...")

    # Clear group/firm portfolio values
    db.query(PortfolioValueEOD).filter(
        PortfolioValueEOD.view_type.in_([ViewType.GROUP, ViewType.FIRM])
    ).delete(synchronize_session=False)

    # Clear group/firm returns
    db.query(ReturnsEOD).filter(
        ReturnsEOD.view_type.in_([ViewType.GROUP, ViewType.FIRM])
    ).delete(synchronize_session=False)

    # Clear group/firm risk metrics
    db.query(RiskEOD).filter(
        RiskEOD.view_type.in_([ViewType.GROUP, ViewType.FIRM])
    ).delete(synchronize_session=False)

    # Clear group/firm benchmark metrics
    db.query(BenchmarkMetric).filter(
        BenchmarkMetric.view_type.in_([ViewType.GROUP, ViewType.FIRM])
    ).delete(synchronize_session=False)

    # Clear group/firm factor regressions
    db.query(FactorRegression).filter(
        FactorRegression.view_type.in_([ViewType.GROUP, ViewType.FIRM])
    ).delete(synchronize_session=False)

    db.commit()
    logger.info("Group and firm analytics cleared")


async def market_data_update_job(db: Session = None):
    """
    Daily job to fetch market data and compute analytics:
    1. Fetch prices for all securities with transactions
    2. Fetch benchmark prices (SPY, QQQ, INDU)
    3. Fetch factor ETF prices
    4. Recompute all analytics (positions, returns, risk, factors)
    """
    close_db = False
    if db is None:
        db = SessionLocal()
        close_db = True

    try:
        logger.info("Starting market data update job")

        market_data = MarketDataProvider(db)

        # Update security prices
        logger.info("Updating security prices...")
        security_results = await market_data.update_all_security_prices()
        logger.info(f"Security prices updated: {security_results}")

        # Update benchmark prices
        logger.info("Updating benchmark prices...")
        benchmark_results = await market_data.update_benchmark_prices()
        logger.info(f"Benchmark prices updated: {benchmark_results}")

        # Ensure factor ETFs exist and update their prices
        logger.info("Updating factor ETF prices...")
        factors_engine = FactorsEngine(db)
        factors_engine.ensure_style7_factor_set()
        factors_engine.ensure_factor_etfs_exist()

        # Update factor ETF prices from Tiingo
        factor_etf_results = await market_data.update_factor_etf_prices()
        logger.info(f"Factor ETF prices updated: {factor_etf_results}")

        logger.info("Market data fetched successfully")

        # Now recompute analytics with the new data
        logger.info("Recomputing analytics...")
        await recompute_analytics_job(db)

        logger.info("Market data update job completed successfully")

    except Exception as e:
        logger.error(f"Market data update job failed: {e}", exc_info=True)
        raise
    finally:
        if close_db:
            db.close()


async def recompute_analytics_job(db: Session = None):
    """
    Daily job to recompute analytics:
    0. Clear old analytics for accounts without transactions and all group/firm data
    1. Build positions from transactions
    2. Compute portfolio values and returns for accounts
    3. Compute group/firm rollups
    4. Compute benchmark returns and metrics
    5. Compute basket returns
    6. Compute factor returns and regressions
    7. Compute risk metrics
    """
    close_db = False
    if db is None:
        db = SessionLocal()
        close_db = True

    try:
        logger.info("Starting analytics recomputation job")

        as_of_date = date.today()

        # 0. Clear old analytics data
        logger.info("Clearing old analytics data...")
        clear_analytics_for_accounts_without_transactions(db)
        clear_group_and_firm_analytics(db)

        # 0.5. Refresh benchmark constituents (SP500 holdings for sector comparison)
        # Only refresh if data is stale (older than DATA_FRESHNESS_HOURS)
        if not is_benchmark_data_fresh(db, "SP500"):
            logger.info("Refreshing benchmark constituents (SP500)...")
            try:
                benchmark_service = BenchmarkService(db)
                benchmark_refresh_result = await benchmark_service.refresh_benchmark("SP500")
                logger.info(f"Benchmark constituents refresh: {benchmark_refresh_result}")
            except Exception as e:
                logger.error(f"Failed to refresh benchmark constituents: {e}")

        # 0.6. Refresh security classifications
        # Only refresh if data is stale (older than DATA_FRESHNESS_HOURS)
        if not is_classification_data_fresh(db):
            logger.info("Refreshing security classifications...")
            try:
                classification_service = ClassificationService(db)
                classification_result = await classification_service.refresh_all_classifications()
                logger.info(f"Classifications refresh: {classification_result}")
            except Exception as e:
                logger.error(f"Failed to refresh classifications: {e}")

        # 1. Build positions
        logger.info("Building positions...")
        positions_engine = PositionsEngine(db)
        positions_results = positions_engine.build_positions_for_all_accounts()
        logger.info(f"Positions built: {positions_results}")

        # 2. Compute account values and returns
        logger.info("Computing account analytics...")
        returns_engine = ReturnsEngine(db)
        accounts = db.query(Account).all()

        for account in accounts:
            try:
                returns_engine.compute_portfolio_values_for_account(account.id)
                returns_engine.compute_returns_for_account(account.id)
            except Exception as e:
                logger.error(f"Failed to compute analytics for account {account.id}: {e}")

        # 3. Compute groups and firm
        logger.info("Computing group rollups...")
        groups_engine = GroupsEngine(db)
        groups_results = groups_engine.compute_all_groups()
        logger.info(f"Groups computed: {groups_results}")

        # 4. Compute benchmarks
        logger.info("Computing benchmark analytics...")
        benchmarks_engine = BenchmarksEngine(db)
        benchmarks_engine.ensure_default_benchmarks()
        benchmark_results = benchmarks_engine.compute_all_benchmark_returns()
        logger.info(f"Benchmarks computed: {benchmark_results}")

        # Compute benchmark metrics for all views
        logger.info("Computing benchmark metrics...")
        for account in accounts:
            for benchmark_code in ['SPY', 'QQQ', 'INDU']:
                try:
                    benchmarks_engine.compute_benchmark_metrics(
                        ViewType.ACCOUNT, account.id, benchmark_code, as_of_date
                    )
                except Exception as e:
                    logger.error(f"Failed benchmark metrics for account {account.id}: {e}")

        groups = db.query(Group).all()
        for group in groups:
            for benchmark_code in ['SPY', 'QQQ', 'INDU']:
                try:
                    benchmarks_engine.compute_benchmark_metrics(
                        ViewType.GROUP, group.id, benchmark_code, as_of_date
                    )
                except Exception as e:
                    logger.error(f"Failed benchmark metrics for group {group.id}: {e}")

        # 5. Compute baskets
        logger.info("Computing basket analytics...")
        baskets_engine = BasketsEngine(db)
        baskets_results = baskets_engine.compute_all_baskets()
        logger.info(f"Baskets computed: {baskets_results}")

        # 6. Compute factors
        logger.info("Computing factor analytics...")
        factors_engine = FactorsEngine(db)
        factors_engine.ensure_style7_factor_set()
        factor_returns_count = factors_engine.compute_factor_returns()
        logger.info(f"Factor returns computed: {factor_returns_count}")

        # Compute factor regressions for all views
        logger.info("Computing factor regressions...")
        for account in accounts:
            try:
                factors_engine.compute_factor_regression(
                    ViewType.ACCOUNT, account.id, as_of_date
                )
            except Exception as e:
                logger.error(f"Failed factor regression for account {account.id}: {e}")

        for group in groups:
            try:
                factors_engine.compute_factor_regression(
                    ViewType.GROUP, group.id, as_of_date
                )
            except Exception as e:
                logger.error(f"Failed factor regression for group {group.id}: {e}")

        # 7. Compute risk metrics
        logger.info("Computing risk metrics...")
        risk_engine = RiskEngine(db)
        risk_results = risk_engine.compute_all_risk_metrics(as_of_date)
        logger.info(f"Risk metrics computed: {risk_results}")

        logger.info("Analytics recomputation job completed successfully")

    except Exception as e:
        logger.error(f"Analytics recomputation job failed: {e}", exc_info=True)
        raise
    finally:
        if close_db:
            db.close()


async def force_refresh_prices_job(db: Session = None):
    """
    Force refresh all price data from Tiingo.
    Use this when prices are stale, corrupt, or need to be completely refreshed.
    This will delete existing prices and re-fetch from Tiingo.
    """
    close_db = False
    if db is None:
        db = SessionLocal()
        close_db = True

    try:
        logger.info("Starting force refresh prices job")

        market_data = MarketDataProvider(db)

        # Force refresh security prices
        logger.info("Force refreshing security prices...")
        security_results = await market_data.update_all_security_prices(force_refresh=True)
        logger.info(f"Security prices refreshed: {security_results}")

        # Force refresh factor ETF prices
        logger.info("Force refreshing factor ETF prices...")
        factor_etf_results = await market_data.update_factor_etf_prices()
        logger.info(f"Factor ETF prices refreshed: {factor_etf_results}")

        # Update benchmark prices
        logger.info("Updating benchmark prices...")
        benchmark_results = await market_data.update_benchmark_prices()
        logger.info(f"Benchmark prices updated: {benchmark_results}")

        logger.info("Force refresh prices job completed successfully")

        # Recompute analytics with fresh data
        logger.info("Recomputing analytics with fresh prices...")
        await recompute_analytics_job(db)

    except Exception as e:
        logger.error(f"Force refresh prices job failed: {e}", exc_info=True)
        raise
    finally:
        if close_db:
            db.close()


# =============================================================================
# NEW INCREMENTAL UPDATE SYSTEM
# =============================================================================

async def incremental_update_job(db: Session = None, force_refresh: bool = False):
    """
    New incremental update job using UpdateOrchestrator.

    Features:
    - Only fetches missing dates
    - Smart provider selection with coverage tracking
    - Dependency-aware analytics computation
    - Comprehensive metrics and observability

    Args:
        db: Database session (optional)
        force_refresh: If True, re-fetch all data ignoring cache

    Returns:
        UpdateMetrics with detailed statistics
    """
    from app.services.update_orchestrator import UpdateOrchestrator

    close_db = False
    if db is None:
        db = SessionLocal()
        close_db = True

    try:
        logger.info("=" * 60)
        logger.info("Starting INCREMENTAL update job")
        logger.info(f"Force refresh: {force_refresh}")
        logger.info("=" * 60)

        orchestrator = UpdateOrchestrator(db)
        metrics = await orchestrator.run_full_update(force_refresh=force_refresh)

        return metrics.to_dict()

    except Exception as e:
        logger.error(f"Incremental update job failed: {e}", exc_info=True)
        raise
    finally:
        if close_db:
            db.close()


async def incremental_market_data_job(db: Session = None, force_refresh: bool = False):
    """
    Incremental market data update only (no analytics recomputation).
    Use this for quick price updates without full analytics refresh.
    """
    from app.services.update_orchestrator import UpdateOrchestrator

    close_db = False
    if db is None:
        db = SessionLocal()
        close_db = True

    try:
        logger.info("Starting incremental market data update")

        orchestrator = UpdateOrchestrator(db)
        metrics = await orchestrator.run_market_data_update(force_refresh=force_refresh)

        return metrics.to_dict()

    except Exception as e:
        logger.error(f"Incremental market data job failed: {e}", exc_info=True)
        raise
    finally:
        if close_db:
            db.close()


async def incremental_analytics_job(db: Session = None):
    """
    Incremental analytics update only (no data fetching).
    Use this after manual data imports or price corrections.
    """
    from app.services.update_orchestrator import UpdateOrchestrator

    close_db = False
    if db is None:
        db = SessionLocal()
        close_db = True

    try:
        logger.info("Starting incremental analytics update")

        orchestrator = UpdateOrchestrator(db)
        metrics = await orchestrator.run_analytics_update()

        return metrics.to_dict()

    except Exception as e:
        logger.error(f"Incremental analytics job failed: {e}", exc_info=True)
        raise
    finally:
        if close_db:
            db.close()


async def smart_update_job(db: Session = None):
    """
    Smart update that chooses the most efficient update strategy based on state.

    - If no recent updates: full incremental update
    - If prices are stale but analytics fresh: market data only
    - If prices fresh but analytics stale: analytics only
    - If everything fresh: skip
    """
    from app.services.update_orchestrator import UpdateOrchestrator
    from app.models.update_tracking import UpdateJobRun

    close_db = False
    if db is None:
        db = SessionLocal()
        close_db = True

    try:
        logger.info("Running smart update job")

        # Check last successful run
        last_run = db.query(UpdateJobRun).filter(
            UpdateJobRun.status == 'completed'
        ).order_by(UpdateJobRun.completed_at.desc()).first()

        if last_run and last_run.completed_at:
            hours_since_last = (datetime.utcnow() - last_run.completed_at).total_seconds() / 3600

            if hours_since_last < 4:  # Less than 4 hours ago
                logger.info(f"Last update was {hours_since_last:.1f}h ago - running quick update")
                return await incremental_market_data_job(db)
            elif hours_since_last < 12:
                logger.info(f"Last update was {hours_since_last:.1f}h ago - running standard update")
                return await incremental_update_job(db)

        # Default: full update
        logger.info("Running full incremental update")
        return await incremental_update_job(db)

    except Exception as e:
        logger.error(f"Smart update job failed: {e}", exc_info=True)
        raise
    finally:
        if close_db:
            db.close()


def get_update_status(db: Session = None) -> dict:
    """
    Get current update status and statistics.

    Returns dict with:
    - last_successful_run: timestamp and metrics
    - pending_updates: count of entities needing update
    - provider_health: status of each data provider
    - computation_status: status of analytics computations
    """
    from app.models.update_tracking import (
        UpdateJobRun, DataUpdateState, TickerProviderCoverage,
        ComputationDependency, DataProviderStatus, ComputationStatus
    )

    close_db = False
    if db is None:
        db = SessionLocal()
        close_db = True

    try:
        status = {}

        # Last successful run
        last_run = db.query(UpdateJobRun).filter(
            UpdateJobRun.status == 'completed'
        ).order_by(UpdateJobRun.completed_at.desc()).first()

        if last_run:
            status['last_successful_run'] = {
                'completed_at': last_run.completed_at.isoformat() if last_run.completed_at else None,
                'tickers_updated': last_run.tickers_updated,
                'rows_inserted': last_run.rows_inserted,
                'duration_seconds': (
                    (last_run.completed_at - last_run.started_at).total_seconds()
                    if last_run.completed_at else None
                )
            }
        else:
            status['last_successful_run'] = None

        # Provider health
        provider_stats = {}
        for provider in ['tiingo', 'stooq', 'yfinance']:
            active = db.query(func.count(TickerProviderCoverage.id)).filter(
                TickerProviderCoverage.provider == provider,
                TickerProviderCoverage.status == DataProviderStatus.ACTIVE
            ).scalar() or 0

            failed = db.query(func.count(TickerProviderCoverage.id)).filter(
                TickerProviderCoverage.provider == provider,
                TickerProviderCoverage.status == DataProviderStatus.FAILED
            ).scalar() or 0

            provider_stats[provider] = {'active': active, 'failed': failed}

        status['provider_health'] = provider_stats

        # Computation status
        comp_stats = {}
        for comp_type in ['positions', 'returns', 'risk', 'factors']:
            completed = db.query(func.count(ComputationDependency.id)).filter(
                ComputationDependency.computation_type == comp_type,
                ComputationDependency.status == ComputationStatus.COMPLETED
            ).scalar() or 0

            pending = db.query(func.count(ComputationDependency.id)).filter(
                ComputationDependency.computation_type == comp_type,
                ComputationDependency.status.in_([
                    ComputationStatus.PENDING, ComputationStatus.FAILED
                ])
            ).scalar() or 0

            comp_stats[comp_type] = {'completed': completed, 'pending': pending}

        status['computation_status'] = comp_stats

        # Securities needing update
        today = date.today()
        stale_count = db.query(func.count(DataUpdateState.id)).filter(
            DataUpdateState.entity_type == 'security_price',
            or_(
                DataUpdateState.last_update_date < today - timedelta(days=1),
                DataUpdateState.last_update_date.is_(None)
            )
        ).scalar() or 0

        status['pending_price_updates'] = stale_count

        return status

    finally:
        if close_db:
            db.close()
