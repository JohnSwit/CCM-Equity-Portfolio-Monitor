import asyncio
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime, date, timedelta
from app.workers.jobs import market_data_update_job, recompute_analytics_job

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


async def run_daily_jobs():
    """Run both daily jobs in sequence"""
    logger.info("=== Starting daily jobs ===")

    try:
        # First, update market data
        logger.info("Running market data update...")
        await market_data_update_job()

        # Then recompute analytics
        logger.info("Running analytics recomputation...")
        await recompute_analytics_job()

        logger.info("=== Daily jobs completed successfully ===")

    except Exception as e:
        logger.error(f"Daily jobs failed: {e}", exc_info=True)


async def check_and_run_if_stale():
    """
    Check if market data is stale (last price older than 1 business day)
    and trigger an immediate update if needed. This handles the case where
    the worker was restarted/redeployed and missed scheduled updates.
    """
    try:
        from app.core.database import SessionLocal
        from app.models import PricesEOD, ReturnsEOD
        from sqlalchemy import func

        db = SessionLocal()
        try:
            latest_price_date = db.query(func.max(PricesEOD.date)).scalar()
            latest_return_date = db.query(func.max(ReturnsEOD.date)).scalar()

            if latest_price_date is None:
                logger.info("No price data found - skipping staleness check")
                return

            today = date.today()
            days_since_prices = (today - latest_price_date).days
            days_since_returns = (today - latest_return_date).days if latest_return_date else 999

            # Consider data stale if prices OR returns are more than 1 calendar day old
            # Also trigger if returns lag behind prices (analytics didn't complete)
            prices_stale = days_since_prices > 1
            returns_lag = latest_return_date and latest_price_date and latest_return_date < latest_price_date

            if prices_stale or returns_lag:
                logger.info(
                    f"Data needs update! Prices: {latest_price_date} ({days_since_prices}d ago), "
                    f"Returns: {latest_return_date} ({days_since_returns}d ago). "
                    f"Prices stale: {prices_stale}, Returns lagging: {returns_lag}. "
                    f"Triggering immediate update..."
                )
                await run_daily_jobs()
            else:
                logger.info(
                    f"Market data is current (prices: {latest_price_date}, "
                    f"returns: {latest_return_date}). No immediate update needed."
                )
        finally:
            db.close()

    except Exception as e:
        logger.error(f"Staleness check failed: {e}", exc_info=True)


async def main():
    """Main scheduler loop"""
    logger.info("Starting Portfolio Monitor Worker")

    # Create scheduler
    scheduler = AsyncIOScheduler()

    # Schedule daily jobs to run after market close (6 PM EST = 23:00 UTC)
    # Adjust timezone as needed
    scheduler.add_job(
        run_daily_jobs,
        CronTrigger(hour=23, minute=0, timezone='UTC'),
        id='daily_jobs',
        name='Daily market data and analytics update',
        replace_existing=True
    )

    # Start scheduler
    scheduler.start()
    logger.info("Scheduler started - jobs will run daily at 23:00 UTC")

    # Check if data is stale and run update immediately if needed
    # (handles missed updates after deployment/restart)
    logger.info("Checking if market data needs immediate update...")
    await check_and_run_if_stale()

    logger.info("Press Ctrl+C to exit")

    try:
        # Keep the script running
        while True:
            await asyncio.sleep(60)
    except (KeyboardInterrupt, SystemExit):
        logger.info("Shutting down scheduler...")
        scheduler.shutdown()


if __name__ == "__main__":
    asyncio.run(main())
