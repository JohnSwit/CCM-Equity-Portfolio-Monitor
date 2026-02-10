import logging
from fastapi import FastAPI, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.middleware.gzip import GZipMiddleware
from fastapi.responses import ORJSONResponse
from starlette.middleware.base import BaseHTTPMiddleware
from sqlalchemy import text
from app.core.config import settings


class CacheControlMiddleware(BaseHTTPMiddleware):
    """Add Cache-Control headers to analytics responses for browser caching."""
    async def dispatch(self, request: Request, call_next):
        response = await call_next(request)
        path = request.url.path
        if request.method == "GET":
            # Analytics data rarely changes - cache for 60s
            if path.startswith("/analytics/") or path.startswith("/portfolio-stats/"):
                response.headers["Cache-Control"] = "private, max-age=60"
            # Static lists cache longer
            elif path in ("/views",):
                response.headers["Cache-Control"] = "private, max-age=120"
        return response
from app.core.database import init_db, get_db, engine
from app.core.security import get_password_hash
from app.models import User
from app.api import auth, imports, views, analytics, baskets, jobs, transactions, portfolio_stats, data_management, new_funds, coverage, ideas, tax, bulk_import, tax_lots

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

# Create FastAPI app with ORJSON for faster serialization
app = FastAPI(
    title="Portfolio Monitor API",
    description="Internal portfolio monitoring and analytics system",
    version="1.0.0",
    default_response_class=ORJSONResponse,
)

# Cache-Control headers for analytics endpoints
app.add_middleware(CacheControlMiddleware)

# GZip middleware - compress responses > 500 bytes (10-100x size reduction)
app.add_middleware(GZipMiddleware, minimum_size=500)

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
app.include_router(tax_lots.router)


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


def ensure_tax_lot_columns():
    """Add new columns to tax_lots table if they don't exist."""
    try:
        with engine.connect() as conn:
            # Check if import_log_id column exists
            result = conn.execute(text("""
                SELECT column_name FROM information_schema.columns
                WHERE table_name = 'tax_lots' AND column_name = 'import_log_id'
            """))
            if not result.fetchone():
                logger.info("Adding new columns to tax_lots table...")

                # First ensure the tax_lot_import_logs table exists
                conn.execute(text("""
                    CREATE TABLE IF NOT EXISTS tax_lot_import_logs (
                        id SERIAL PRIMARY KEY,
                        file_name VARCHAR,
                        file_hash VARCHAR,
                        status VARCHAR,
                        rows_processed INTEGER DEFAULT 0,
                        rows_imported INTEGER DEFAULT 0,
                        rows_skipped INTEGER DEFAULT 0,
                        rows_error INTEGER DEFAULT 0,
                        errors JSON,
                        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
                    )
                """))
                conn.execute(text("CREATE INDEX IF NOT EXISTS ix_tax_lot_import_logs_file_hash ON tax_lot_import_logs (file_hash)"))

                # Add new columns to tax_lots
                conn.execute(text("ALTER TABLE tax_lots ADD COLUMN IF NOT EXISTS import_log_id INTEGER REFERENCES tax_lot_import_logs(id)"))
                conn.execute(text("ALTER TABLE tax_lots ADD COLUMN IF NOT EXISTS market_value FLOAT"))
                conn.execute(text("ALTER TABLE tax_lots ADD COLUMN IF NOT EXISTS short_term_gain_loss FLOAT"))
                conn.execute(text("ALTER TABLE tax_lots ADD COLUMN IF NOT EXISTS long_term_gain_loss FLOAT"))
                conn.execute(text("ALTER TABLE tax_lots ADD COLUMN IF NOT EXISTS total_gain_loss FLOAT"))
                conn.execute(text("CREATE INDEX IF NOT EXISTS ix_tax_lots_import_log_id ON tax_lots (import_log_id)"))

                conn.commit()
                logger.info("Successfully added new columns to tax_lots table")
            else:
                logger.info("tax_lots table already has new columns")
    except Exception as e:
        logger.warning(f"Could not update tax_lots table: {e}")


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

    # Ensure tax_lots table has new columns
    ensure_tax_lot_columns()

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
