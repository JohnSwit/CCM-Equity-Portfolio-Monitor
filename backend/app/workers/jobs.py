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
from app.models import Account, Group, ViewType
import logging

logger = logging.getLogger(__name__)


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
