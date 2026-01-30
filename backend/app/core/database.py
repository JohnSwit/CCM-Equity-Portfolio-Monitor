from sqlalchemy import create_engine, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from app.core.config import settings
import logging

logger = logging.getLogger(__name__)

engine = create_engine(
    settings.DATABASE_URL,
    pool_pre_ping=True,
    pool_size=10,
    max_overflow=20
)

SessionLocal = sessionmaker(autocommit=False, autoflush=False, bind=engine)

Base = declarative_base()


def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


def update_enum_values():
    """Update PostgreSQL enum types with new values.

    This handles the case where new enum values are added in Python code
    but the database enum type was created before those values existed.
    """
    enum_updates = [
        # (enum_type_name, new_value)
        ("factordatasource", "tiingo"),
    ]

    with engine.connect() as conn:
        for enum_type, new_value in enum_updates:
            try:
                # Check if the enum type exists
                result = conn.execute(text(
                    "SELECT 1 FROM pg_type WHERE typname = :enum_type"
                ), {"enum_type": enum_type})

                if result.fetchone():
                    # Check if the value already exists in the enum
                    result = conn.execute(text(f"""
                        SELECT enumlabel FROM pg_enum
                        JOIN pg_type ON pg_enum.enumtypid = pg_type.oid
                        WHERE pg_type.typname = :enum_type
                    """), {"enum_type": enum_type})
                    existing_values = [row[0] for row in result.fetchall()]

                    if new_value not in existing_values:
                        # Add the new value to the enum
                        # Note: ALTER TYPE ADD VALUE cannot be parameterized, so we use f-string
                        # but validate the inputs are safe (alphanumeric only)
                        if enum_type.isalnum() and new_value.replace('_', '').isalnum():
                            conn.execute(text(
                                f"ALTER TYPE {enum_type} ADD VALUE IF NOT EXISTS '{new_value}'"
                            ))
                            conn.commit()
                            logger.info(f"Added '{new_value}' to enum type '{enum_type}'")
                    else:
                        logger.debug(f"Value '{new_value}' already exists in enum '{enum_type}'")
            except Exception as e:
                # Log but don't fail - the enum might not exist yet
                logger.debug(f"Could not update enum {enum_type}: {e}")


def init_db():
    """Initialize database - create all tables and update enums"""
    # First create all tables
    Base.metadata.create_all(bind=engine)

    # Then update any enum types with new values
    try:
        update_enum_values()
    except Exception as e:
        logger.warning(f"Could not update enum values: {e}")
