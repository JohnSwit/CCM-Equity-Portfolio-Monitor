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
import logging

logger = logging.getLogger(__name__)


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

        # Factor ETFs will be updated as part of security prices
        # since they're in the Security table

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
        logger.info("Refreshing benchmark constituents (SP500)...")
        try:
            benchmark_service = BenchmarkService(db)
            benchmark_refresh_result = await benchmark_service.refresh_benchmark("SP500")
            logger.info(f"Benchmark constituents refresh: {benchmark_refresh_result}")
        except Exception as e:
            logger.error(f"Failed to refresh benchmark constituents: {e}")

        # 0.6. Refresh security classifications
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
