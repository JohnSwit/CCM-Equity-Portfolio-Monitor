import logging
from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from sqlalchemy import text
from app.core.config import settings
from app.core.database import init_db, get_db, engine
from app.core.security import get_password_hash
from app.models import User
from app.api import auth, imports, views, analytics, baskets, jobs, transactions, portfolio_stats, data_management, new_funds, coverage, ideas, tax, bulk_import

# Configure logging for all modules
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
# Ensure our app modules log at INFO level
logging.getLogger('app').setLevel(logging.INFO)
logging.getLogger('app.services').setLevel(logging.INFO)
logging.getLogger('app.services.market_data').setLevel(logging.INFO)
logging.getLogger('app.workers').setLevel(logging.INFO)

logger = logging.getLogger(__name__)

# Create FastAPI app
app = FastAPI(
    title="Portfolio Monitor API",
    description="Internal portfolio monitoring and analytics system",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(auth.router)
app.include_router(imports.router)
app.include_router(views.router)
app.include_router(analytics.router)
app.include_router(baskets.router)
app.include_router(jobs.router)
app.include_router(transactions.router)
app.include_router(portfolio_stats.router)
app.include_router(data_management.router)
app.include_router(new_funds.router)
app.include_router(coverage.router)
app.include_router(ideas.router)
app.include_router(tax.router)
app.include_router(bulk_import.router)


def ensure_tiingo_enum():
    """Ensure TIINGO is added to factordatasource enum in PostgreSQL.

    Note: The PostgreSQL enum uses UPPERCASE values (STOOQ, FRED, YFINANCE, ALPHAVANTAGE)
    so we must add TIINGO in uppercase to match.
    """
    try:
        with engine.connect() as conn:
            # Check if enum type exists and get current values
            result = conn.execute(text("""
                SELECT enumlabel FROM pg_enum
                JOIN pg_type ON pg_enum.enumtypid = pg_type.oid
                WHERE pg_type.typname = 'factordatasource'
            """))
            existing_values = [row[0] for row in result.fetchall()]
            logger.info(f"Existing factordatasource enum values: {existing_values}")

            # Check for both cases - the enum needs UPPERCASE TIINGO
            if 'TIINGO' not in existing_values:
                # Add TIINGO to the enum (uppercase to match existing pattern)
                conn.execute(text("ALTER TYPE factordatasource ADD VALUE IF NOT EXISTS 'TIINGO'"))
                conn.commit()
                logger.info("Added 'TIINGO' to factordatasource enum")
            else:
                logger.info("'TIINGO' already exists in factordatasource enum")
    except Exception as e:
        logger.warning(f"Could not update factordatasource enum: {e}")


def ensure_transaction_type_enum():
    """Ensure DIVIDEND_REINVEST is added to transactiontype enum in PostgreSQL."""
    try:
        with engine.connect() as conn:
            # Check if enum type exists and get current values
            result = conn.execute(text("""
                SELECT enumlabel FROM pg_enum
                JOIN pg_type ON pg_enum.enumtypid = pg_type.oid
                WHERE pg_type.typname = 'transactiontype'
            """))
            existing_values = [row[0] for row in result.fetchall()]
            logger.info(f"Existing transactiontype enum values: {existing_values}")

            # Add DIVIDEND_REINVEST if not present
            if 'DIVIDEND_REINVEST' not in existing_values:
                conn.execute(text("ALTER TYPE transactiontype ADD VALUE IF NOT EXISTS 'DIVIDEND_REINVEST'"))
                conn.commit()
                logger.info("Added 'DIVIDEND_REINVEST' to transactiontype enum")
            else:
                logger.info("'DIVIDEND_REINVEST' already exists in transactiontype enum")
    except Exception as e:
        logger.warning(f"Could not update transactiontype enum: {e}")


@app.on_event("startup")
async def startup_event():
    """Initialize database and create default admin user"""
    logger.info("=== Portfolio Monitor API Starting ===")
    logger.info(f"Tiingo API Key configured: {bool(settings.TIINGO_API_KEY)}")
    logger.info(f"yfinance fallback enabled: {settings.ENABLE_YFINANCE_FALLBACK}")

    # Create tables
    init_db()

    # Ensure enum values exist (explicit call with logging)
    ensure_tiingo_enum()
    ensure_transaction_type_enum()

    # Create default admin user if not exists
    db = next(get_db())
    try:
        admin = db.query(User).filter(User.email == "admin@example.com").first()
        if not admin:
            admin = User(
                email="admin@example.com",
                hashed_password=get_password_hash("admin"),
                full_name="Admin User",
                is_active=True,
                is_admin=True
            )
            db.add(admin)
            db.commit()
            print("Created default admin user: admin@example.com / admin")
    finally:
        db.close()


@app.get("/")
def root():
    """Root endpoint"""
    return {
        "name": "Portfolio Monitor API",
        "version": "1.0.0",
        "status": "running"
    }


@app.get("/health")
def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}
