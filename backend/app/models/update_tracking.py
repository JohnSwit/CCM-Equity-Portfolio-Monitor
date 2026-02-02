"""
Models for tracking data updates, provider status, and computation dependencies.
Enables incremental updates and dependency-aware recomputation.
"""
from sqlalchemy import (
    Column, Integer, String, Float, Date, DateTime, Boolean,
    Text, JSON, ForeignKey, Index, UniqueConstraint, Enum as SQLEnum
)
from sqlalchemy.sql import func
from datetime import datetime, date
import enum
import hashlib
import json
from typing import Dict, Any, Optional, List

from app.core.database import Base


class DataProviderStatus(str, enum.Enum):
    """Status of a data provider for a specific ticker"""
    ACTIVE = "active"           # Provider works for this ticker
    FAILED = "failed"           # Provider failed (temporary)
    NOT_SUPPORTED = "not_supported"  # Provider doesn't cover this ticker
    UNKNOWN = "unknown"         # Not yet tested


class TickerProviderCoverage(Base):
    """
    Tracks which data providers work for each ticker.
    Enables smart provider selection without redundant calls.
    """
    __tablename__ = "ticker_provider_coverage"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String, nullable=False, index=True)
    provider = Column(String, nullable=False, index=True)  # tiingo, stooq, yfinance
    status = Column(SQLEnum(DataProviderStatus), default=DataProviderStatus.UNKNOWN)
    last_success = Column(DateTime)
    last_failure = Column(DateTime)
    failure_count = Column(Integer, default=0)
    last_error = Column(Text)
    records_fetched = Column(Integer, default=0)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint('symbol', 'provider', name='uq_ticker_provider'),
        Index('idx_ticker_provider_status', 'symbol', 'status'),
    )


class DataUpdateState(Base):
    """
    Tracks the update state for each data entity (security prices, benchmarks, etc.).
    Enables incremental fetching from last known state.
    """
    __tablename__ = "data_update_state"

    id = Column(Integer, primary_key=True, index=True)
    entity_type = Column(String, nullable=False, index=True)  # security_price, benchmark, factor_etf
    entity_id = Column(String, nullable=False, index=True)    # symbol or id
    last_update_date = Column(Date)                           # Last date we have data for
    last_update_timestamp = Column(DateTime)                   # When we last updated
    preferred_provider = Column(String)                        # Best provider for this entity
    update_frequency = Column(String, default='daily')         # daily, weekly, monthly
    needs_backfill = Column(Boolean, default=False)           # True if gaps detected
    backfill_start = Column(Date)                             # Start of backfill range
    metadata_json = Column(JSON)                              # Additional state info
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint('entity_type', 'entity_id', name='uq_data_update_entity'),
        Index('idx_data_update_needs_update', 'entity_type', 'last_update_date'),
    )


class ComputationStatus(str, enum.Enum):
    """Status of a computation"""
    PENDING = "pending"
    RUNNING = "running"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"     # Skipped because inputs unchanged


class ComputationDependency(Base):
    """
    Tracks computation dependencies and input hashes.
    Enables dependency-aware recomputation.
    """
    __tablename__ = "computation_dependencies"

    id = Column(Integer, primary_key=True, index=True)
    computation_type = Column(String, nullable=False, index=True)  # positions, returns, risk, factors
    view_type = Column(String, nullable=False, index=True)         # account, group, firm
    view_id = Column(Integer, nullable=False, index=True)
    input_hash = Column(String, nullable=False)                    # Hash of all inputs
    output_hash = Column(String)                                   # Hash of outputs (for verification)
    last_computed = Column(DateTime)
    status = Column(SQLEnum(ComputationStatus), default=ComputationStatus.PENDING)
    compute_duration_ms = Column(Integer)                          # Time to compute
    error_message = Column(Text)
    metadata_json = Column(JSON)                                   # Dependencies, versions, etc.
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint('computation_type', 'view_type', 'view_id', name='uq_computation_view'),
        Index('idx_computation_status', 'computation_type', 'status'),
    )


class UpdateJobRun(Base):
    """
    Tracks each update job execution for observability.
    """
    __tablename__ = "update_job_runs"

    id = Column(Integer, primary_key=True, index=True)
    job_type = Column(String, nullable=False, index=True)      # market_data, analytics, full
    started_at = Column(DateTime, nullable=False)
    completed_at = Column(DateTime)
    status = Column(String, default='running')                  # running, completed, failed

    # Metrics
    tickers_processed = Column(Integer, default=0)
    tickers_updated = Column(Integer, default=0)
    tickers_failed = Column(Integer, default=0)
    tickers_skipped = Column(Integer, default=0)
    rows_inserted = Column(Integer, default=0)
    api_calls_made = Column(Integer, default=0)
    cache_hits = Column(Integer, default=0)

    # Timing
    fetch_duration_ms = Column(Integer)
    compute_duration_ms = Column(Integer)
    db_write_duration_ms = Column(Integer)

    # Error tracking
    errors_json = Column(JSON)                                  # List of errors
    warnings_json = Column(JSON)                                # List of warnings

    # Summary
    summary_json = Column(JSON)                                 # Detailed breakdown

    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index('idx_job_run_type_date', 'job_type', 'started_at'),
    )


# Helper functions for hash computation
def compute_input_hash(inputs: Dict[str, Any]) -> str:
    """Compute a deterministic hash of inputs for change detection"""
    # Sort keys for deterministic ordering
    normalized = json.dumps(inputs, sort_keys=True, default=str)
    return hashlib.sha256(normalized.encode()).hexdigest()[:32]


def compute_positions_input_hash(
    account_id: int,
    transaction_ids: List[int],
    last_transaction_date: date
) -> str:
    """Compute input hash for positions computation"""
    inputs = {
        'account_id': account_id,
        'transaction_count': len(transaction_ids),
        'transaction_ids_hash': hashlib.md5(
            ','.join(map(str, sorted(transaction_ids))).encode()
        ).hexdigest(),
        'last_transaction_date': str(last_transaction_date),
    }
    return compute_input_hash(inputs)


def compute_returns_input_hash(
    view_type: str,
    view_id: int,
    positions_hash: str,
    prices_last_date: date
) -> str:
    """Compute input hash for returns computation"""
    inputs = {
        'view_type': view_type,
        'view_id': view_id,
        'positions_hash': positions_hash,
        'prices_last_date': str(prices_last_date),
    }
    return compute_input_hash(inputs)


def compute_risk_input_hash(
    view_type: str,
    view_id: int,
    returns_hash: str,
    as_of_date: date
) -> str:
    """Compute input hash for risk metrics computation"""
    inputs = {
        'view_type': view_type,
        'view_id': view_id,
        'returns_hash': returns_hash,
        'as_of_date': str(as_of_date),
    }
    return compute_input_hash(inputs)


def compute_factors_input_hash(
    view_type: str,
    view_id: int,
    returns_hash: str,
    factor_prices_last_date: date,
    factor_set_code: str
) -> str:
    """Compute input hash for factor regression computation"""
    inputs = {
        'view_type': view_type,
        'view_id': view_id,
        'returns_hash': returns_hash,
        'factor_prices_last_date': str(factor_prices_last_date),
        'factor_set_code': factor_set_code,
    }
    return compute_input_hash(inputs)
