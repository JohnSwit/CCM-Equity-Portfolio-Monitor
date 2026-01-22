import asyncio
import logging
from apscheduler.schedulers.asyncio import AsyncIOScheduler
from apscheduler.triggers.cron import CronTrigger
from datetime import datetime
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
