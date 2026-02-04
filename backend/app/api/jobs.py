from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session
from datetime import datetime, date
from typing import Optional
import asyncio
import logging
from app.core.database import get_db
from app.api.auth import get_current_user
from app.models import User
from app.models.schemas import JobRunResponse

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/jobs", tags=["jobs"])


def get_admin_user(current_user: User = Depends(get_current_user)) -> User:
    """Verify that the current user is an admin"""
    if not current_user.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin privileges required"
        )
    return current_user


@router.post("/run", response_model=JobRunResponse)
async def run_job(
    job_name: str = Query(..., regex="^(market_data_update|recompute_analytics|force_refresh_prices|reset_returns)$"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_admin_user)
):
    """
    Trigger a job manually:
    - market_data_update: Fetch latest market data
    - recompute_analytics: Recompute positions, returns, and analytics
    - force_refresh_prices: Delete all prices and re-fetch from Tiingo (use if data is stale/corrupt)
    - reset_returns: Clear ALL returns and recompute from scratch (use after TWR index fix)
    """
    started_at = datetime.utcnow()

    try:
        if job_name == "market_data_update":
            from app.workers.jobs import market_data_update_job
            await market_data_update_job(db)
            message = "Market data update completed successfully"

        elif job_name == "recompute_analytics":
            from app.workers.jobs import recompute_analytics_job
            await recompute_analytics_job(db)
            message = "Analytics recomputation completed successfully"

        elif job_name == "force_refresh_prices":
            from app.workers.jobs import force_refresh_prices_job
            await force_refresh_prices_job(db)
            message = "Force refresh of all prices completed successfully"

        elif job_name == "reset_returns":
            from app.workers.jobs import clear_all_returns, recompute_analytics_job
            # First clear all returns data
            clear_all_returns(db)
            # Then recompute everything
            await recompute_analytics_job(db)
            message = "Returns reset and recomputed successfully"

        else:
            raise HTTPException(status_code=400, detail="Invalid job name")

        return JobRunResponse(
            status="success",
            message=message,
            started_at=started_at
        )

    except Exception as e:
        return JobRunResponse(
            status="failed",
            message=f"Job failed: {str(e)}",
            started_at=started_at
        )


# =============================================================================
# NEW INCREMENTAL UPDATE ENDPOINTS
# =============================================================================

@router.post("/incremental-update")
async def run_incremental_update(
    force_refresh: bool = Query(False, description="Force re-fetch all data ignoring cache"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_admin_user)
):
    """
    Run incremental update with smart caching and dependency tracking.

    This is the recommended update method for production use.

    Features:
    - Only fetches missing dates
    - Smart provider selection (tracks which providers work for each ticker)
    - Dependency-aware analytics (only recomputes when inputs change)
    - Comprehensive metrics and error tracking

    Args:
        force_refresh: If True, ignores cache and re-fetches all data

    Returns:
        Detailed metrics including:
        - tickers_updated/skipped/failed counts
        - rows_inserted
        - api_calls_made vs cache_hits
        - timing breakdowns
        - error list
    """
    from app.workers.jobs import incremental_update_job

    try:
        metrics = await incremental_update_job(db, force_refresh=force_refresh)
        return {
            "status": "success",
            "message": "Incremental update completed",
            "metrics": metrics
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Incremental update failed: {str(e)}"
        )


@router.post("/incremental-market-data")
async def run_incremental_market_data(
    force_refresh: bool = Query(False, description="Force re-fetch all data"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_admin_user)
):
    """
    Run market data update only (no analytics recomputation).

    Use this for:
    - Quick price updates during market hours
    - Testing data provider connectivity
    - Backfilling missing prices

    Faster than full incremental update since it skips analytics.
    """
    from app.workers.jobs import incremental_market_data_job

    try:
        metrics = await incremental_market_data_job(db, force_refresh=force_refresh)
        return {
            "status": "success",
            "message": "Market data update completed",
            "metrics": metrics
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Market data update failed: {str(e)}"
        )


@router.post("/incremental-analytics")
async def run_incremental_analytics(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_admin_user)
):
    """
    Run analytics update only (no data fetching).

    Use this after:
    - Manual price imports
    - Price corrections
    - Transaction imports

    Only recomputes analytics where inputs have changed.
    """
    from app.workers.jobs import incremental_analytics_job

    try:
        metrics = await incremental_analytics_job(db)
        return {
            "status": "success",
            "message": "Analytics update completed",
            "metrics": metrics
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Analytics update failed: {str(e)}"
        )


@router.post("/smart-update")
async def run_smart_update(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_admin_user)
):
    """
    Automatically chooses the most efficient update strategy.

    Logic:
    - If last update < 4h ago: quick market data update only
    - If last update < 12h ago: standard incremental update
    - Otherwise: full incremental update

    Use this for scheduled jobs or when you're not sure what's needed.
    """
    from app.workers.jobs import smart_update_job

    try:
        metrics = await smart_update_job(db)
        return {
            "status": "success",
            "message": "Smart update completed",
            "metrics": metrics
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Smart update failed: {str(e)}"
        )


@router.get("/update-status")
async def get_update_status(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get current update status and health metrics.

    Returns:
    - last_successful_run: when and what happened
    - provider_health: status of each data provider
    - computation_status: which analytics are up to date
    - pending_price_updates: count of stale tickers
    """
    from app.workers.jobs import get_update_status as _get_status

    try:
        status = _get_status(db)
        return {
            "status": "success",
            "data": status
        }
    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Failed to get status: {str(e)}"
        )


@router.get("/provider-coverage")
async def get_provider_coverage(
    symbol: Optional[str] = Query(None, description="Filter by symbol"),
    provider: Optional[str] = Query(None, description="Filter by provider"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get data provider coverage information.

    Shows which providers work for each ticker, failure counts, etc.
    Useful for debugging data fetch issues.
    """
    from app.models.update_tracking import TickerProviderCoverage
    from sqlalchemy import and_

    query = db.query(TickerProviderCoverage)

    if symbol:
        query = query.filter(TickerProviderCoverage.symbol == symbol.upper())
    if provider:
        query = query.filter(TickerProviderCoverage.provider == provider.lower())

    coverages = query.order_by(
        TickerProviderCoverage.symbol,
        TickerProviderCoverage.provider
    ).limit(500).all()

    return {
        "status": "success",
        "count": len(coverages),
        "data": [
            {
                "symbol": c.symbol,
                "provider": c.provider,
                "status": c.status.value if c.status else None,
                "last_success": c.last_success.isoformat() if c.last_success else None,
                "last_failure": c.last_failure.isoformat() if c.last_failure else None,
                "failure_count": c.failure_count,
                "records_fetched": c.records_fetched,
                "last_error": c.last_error[:100] if c.last_error else None
            }
            for c in coverages
        ]
    }


@router.get("/job-history")
async def get_job_history(
    limit: int = Query(20, ge=1, le=100),
    job_type: Optional[str] = Query(None, description="Filter by job type"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get recent job execution history.

    Shows timing, success/failure, metrics for each run.
    """
    from app.models.update_tracking import UpdateJobRun

    query = db.query(UpdateJobRun)

    if job_type:
        query = query.filter(UpdateJobRun.job_type == job_type)

    runs = query.order_by(UpdateJobRun.started_at.desc()).limit(limit).all()

    return {
        "status": "success",
        "count": len(runs),
        "data": [
            {
                "id": r.id,
                "job_type": r.job_type,
                "status": r.status,
                "started_at": r.started_at.isoformat() if r.started_at else None,
                "completed_at": r.completed_at.isoformat() if r.completed_at else None,
                "duration_seconds": (
                    (r.completed_at - r.started_at).total_seconds()
                    if r.completed_at and r.started_at else None
                ),
                "tickers_processed": r.tickers_processed,
                "tickers_updated": r.tickers_updated,
                "tickers_failed": r.tickers_failed,
                "rows_inserted": r.rows_inserted,
                "api_calls_made": r.api_calls_made,
                "cache_hits": r.cache_hits,
                "error_count": len(r.errors_json) if r.errors_json else 0
            }
            for r in runs
        ]
    }


# =============================================================================
# BATCH ANALYTICS ENDPOINTS (Post-Import)
# =============================================================================

@router.post("/batch-analytics")
async def run_batch_analytics(
    account_ids: Optional[str] = Query(None, description="Comma-separated account IDs (None = all)"),
    start_date: Optional[str] = Query(None, description="Start date (YYYY-MM-DD)"),
    end_date: Optional[str] = Query(None, description="End date (YYYY-MM-DD)"),
    skip_positions: bool = Query(False, description="Skip position building"),
    skip_values: bool = Query(False, description="Skip portfolio value computation"),
    skip_returns: bool = Query(False, description="Skip returns computation"),
    force_full_rebuild: bool = Query(False, description="Force full rebuild (bypass incremental skip logic)"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_admin_user)
):
    """
    Run optimized batch analytics computation.

    This is designed for large-scale post-import processing:
    - Uses bulk upsert (PostgreSQL ON CONFLICT) instead of individual queries
    - Processes in batches with periodic commits
    - Provides progress tracking
    - Memory-efficient chunked processing

    Use this after bulk transaction imports to compute all analytics.

    Args:
        account_ids: Specific accounts to process (None = all accounts)
        start_date: Start date for computation (None = from first transaction)
        end_date: End date (default: today)
        skip_positions: Skip position building step
        skip_values: Skip portfolio value computation
        skip_returns: Skip returns computation
        force_full_rebuild: Force full rebuild, bypass incremental skip (use after bug fixes)

    Returns:
        Detailed results including counts and timing
    """
    from app.services.analytics_batch import BatchAnalyticsService
    from datetime import date as date_type

    # Parse parameters
    parsed_account_ids = None
    if account_ids:
        try:
            parsed_account_ids = [int(x.strip()) for x in account_ids.split(",")]
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid account_ids format")

    parsed_start_date = None
    if start_date:
        try:
            parsed_start_date = date_type.fromisoformat(start_date)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid start_date format (use YYYY-MM-DD)")

    parsed_end_date = None
    if end_date:
        try:
            parsed_end_date = date_type.fromisoformat(end_date)
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid end_date format (use YYYY-MM-DD)")

    try:
        service = BatchAnalyticsService(db)
        result = service.run_full_analytics(
            account_ids=parsed_account_ids,
            start_date=parsed_start_date,
            end_date=parsed_end_date,
            skip_positions=skip_positions,
            skip_values=skip_values,
            skip_returns=skip_returns,
            force_full_rebuild=force_full_rebuild
        )

        return {
            "status": "success",
            "message": "Batch analytics completed",
            "result": result
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Batch analytics failed: {str(e)}"
        )


@router.post("/post-import-analytics")
async def run_post_import_analytics(
    import_job_id: Optional[str] = Query(None, description="Bulk import job ID"),
    incremental: bool = Query(True, description="Only compute from new transaction dates"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_admin_user)
):
    """
    Run analytics specifically for post-bulk-import processing.

    This determines which accounts were affected by an import and runs
    optimized batch analytics only for those accounts.

    Args:
        import_job_id: The bulk import job ID (None = all accounts)
        incremental: Only compute from earliest new transaction date

    Returns:
        Analytics computation results
    """
    from app.services.analytics_batch import PostImportAnalyticsJob

    try:
        job = PostImportAnalyticsJob(db)

        # Parse import_job_id if provided
        parsed_id = int(import_job_id) if import_job_id else None

        result = job.run_for_import(
            import_job_id=parsed_id,
            incremental=incremental
        )

        return {
            "status": "success",
            "message": "Post-import analytics completed",
            "result": result
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Post-import analytics failed: {str(e)}"
        )


@router.post("/backfill-benchmarks")
async def backfill_benchmark_data(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_admin_user)
):
    """
    Backfill historical benchmark data to match transaction history.

    The incremental update only fetches data FORWARD from the latest date.
    This endpoint fetches BACKWARD to fill historical gaps.

    Use this after importing historical transactions that predate existing benchmark data.
    """
    from app.services.market_data import MarketDataProvider
    from app.models import Transaction, BenchmarkDefinition, BenchmarkLevel
    from sqlalchemy import func
    from datetime import timedelta

    try:
        market_data = MarketDataProvider(db)

        # Get earliest transaction date
        earliest_txn = db.query(func.min(Transaction.trade_date)).scalar()
        if not earliest_txn:
            return {
                "status": "success",
                "message": "No transactions found",
                "benchmarks_updated": 0
            }

        target_start = earliest_txn - timedelta(days=30)
        end_date = date.today()

        benchmarks = db.query(BenchmarkDefinition).all()
        results = []

        for benchmark in benchmarks:
            # Get earliest existing benchmark data
            earliest_benchmark = db.query(func.min(BenchmarkLevel.date)).filter(
                BenchmarkLevel.code == benchmark.code
            ).scalar()

            if earliest_benchmark and earliest_benchmark <= target_start:
                # Already have data back to target
                results.append({
                    "code": benchmark.code,
                    "status": "skipped",
                    "reason": f"Already has data from {earliest_benchmark}"
                })
                continue

            # Need to backfill - fetch from target_start to earliest existing (or end_date if no data)
            fetch_end = earliest_benchmark - timedelta(days=1) if earliest_benchmark else end_date

            logger.info(f"Backfilling {benchmark.code}: {target_start} to {fetch_end}")

            count = await market_data.fetch_and_store_benchmark_prices(
                benchmark.code,
                benchmark.provider_symbol,
                target_start,
                fetch_end,
                force_refresh=False
            )

            results.append({
                "code": benchmark.code,
                "status": "updated",
                "rows_added": count
            })

        return {
            "status": "success",
            "message": "Benchmark backfill completed",
            "target_start_date": str(target_start),
            "benchmarks": results
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Benchmark backfill failed: {str(e)}"
        )


@router.post("/cleanup-orphaned-data")
async def cleanup_orphaned_data_endpoint(
    delete_accounts: bool = Query(False, description="Delete orphaned Account records (accounts with no transactions)"),
    delete_securities: bool = Query(False, description="Delete orphaned Security records (securities with no transactions)"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_admin_user)
):
    """
    Clean up orphaned data after transaction deletions.

    When transactions are deleted, the Account and Security records remain in the database.
    This can cause the update process to attempt to fetch data or compute analytics for
    entities that no longer have any transactions.

    This endpoint:
    1. Always clears analytics for accounts without transactions
    2. Optionally deletes the Account records themselves
    3. Optionally deletes orphaned Security records and their price data

    Note: Benchmark/Factor ETF securities (SPY, QQQ, etc.) are never deleted as they're
    needed for analytics even if no transactions reference them.

    Args:
        delete_accounts: If True, deletes Account records with no transactions
        delete_securities: If True, deletes Security records with no transactions (except ETFs)

    Returns:
        Summary of cleanup operations
    """
    from app.workers.jobs import cleanup_orphaned_data

    try:
        results = cleanup_orphaned_data(
            db,
            delete_accounts=delete_accounts,
            delete_securities=delete_securities
        )

        return {
            "status": "success",
            "message": "Orphaned data cleanup completed",
            "results": results
        }

    except Exception as e:
        raise HTTPException(
            status_code=500,
            detail=f"Cleanup failed: {str(e)}"
        )


@router.get("/debug/account-transaction-counts")
async def get_account_transaction_counts(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_admin_user)
):
    """
    Debug endpoint: Show transaction counts per account.

    Helps diagnose why accounts are not being detected as orphaned.
    """
    from app.models import Account, Transaction
    from sqlalchemy import func

    # Get all accounts with their transaction counts
    account_txn_counts = db.query(
        Account.id,
        Account.account_number,
        Account.account_name,
        func.count(Transaction.id).label('transaction_count')
    ).outerjoin(
        Transaction, Account.id == Transaction.account_id
    ).group_by(Account.id).all()

    # Get accounts with no transactions
    orphaned = [a for a in account_txn_counts if a.transaction_count == 0]
    with_txns = [a for a in account_txn_counts if a.transaction_count > 0]

    return {
        "total_accounts": len(account_txn_counts),
        "accounts_with_transactions": len(with_txns),
        "orphaned_accounts": len(orphaned),
        "orphaned_account_details": [
            {
                "id": a.id,
                "account_number": a.account_number,
                "account_name": a.account_name
            }
            for a in orphaned
        ],
        "accounts_with_transactions_details": [
            {
                "id": a.id,
                "account_number": a.account_number,
                "account_name": a.account_name,
                "transaction_count": a.transaction_count
            }
            for a in with_txns[:20]  # Show first 20
        ]
    }


@router.delete("/debug/delete-account/{account_id}")
async def force_delete_account(
    account_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_admin_user)
):
    """
    Force delete an account and ALL its associated data.

    This will delete:
    - All transactions for the account
    - All positions for the account
    - All portfolio values for the account
    - All returns for the account
    - All risk metrics for the account
    - All benchmark metrics for the account
    - All factor regressions for the account
    - The account record itself

    WARNING: This is irreversible!
    """
    from app.models import (
        Account, Transaction, PositionsEOD, PortfolioValueEOD,
        ReturnsEOD, RiskEOD, BenchmarkMetric, FactorRegression, ViewType
    )

    # Check account exists
    account = db.query(Account).filter(Account.id == account_id).first()
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")

    account_name = account.account_name
    account_number = account.account_number

    # Delete all related data
    deleted_counts = {}

    # Transactions
    deleted_counts['transactions'] = db.query(Transaction).filter(
        Transaction.account_id == account_id
    ).delete(synchronize_session=False)

    # Positions
    deleted_counts['positions'] = db.query(PositionsEOD).filter(
        PositionsEOD.account_id == account_id
    ).delete(synchronize_session=False)

    # Portfolio values
    deleted_counts['portfolio_values'] = db.query(PortfolioValueEOD).filter(
        PortfolioValueEOD.view_type == ViewType.ACCOUNT,
        PortfolioValueEOD.view_id == account_id
    ).delete(synchronize_session=False)

    # Returns
    deleted_counts['returns'] = db.query(ReturnsEOD).filter(
        ReturnsEOD.view_type == ViewType.ACCOUNT,
        ReturnsEOD.view_id == account_id
    ).delete(synchronize_session=False)

    # Risk metrics
    deleted_counts['risk'] = db.query(RiskEOD).filter(
        RiskEOD.view_type == ViewType.ACCOUNT,
        RiskEOD.view_id == account_id
    ).delete(synchronize_session=False)

    # Benchmark metrics
    deleted_counts['benchmark_metrics'] = db.query(BenchmarkMetric).filter(
        BenchmarkMetric.view_type == ViewType.ACCOUNT,
        BenchmarkMetric.view_id == account_id
    ).delete(synchronize_session=False)

    # Factor regressions
    deleted_counts['factor_regressions'] = db.query(FactorRegression).filter(
        FactorRegression.view_type == ViewType.ACCOUNT,
        FactorRegression.view_id == account_id
    ).delete(synchronize_session=False)

    # Delete the account itself
    db.delete(account)
    db.commit()

    return {
        "status": "success",
        "message": f"Deleted account '{account_name}' ({account_number})",
        "account_id": account_id,
        "deleted_counts": deleted_counts
    }
