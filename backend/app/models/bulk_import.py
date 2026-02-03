"""
Bulk Import Models - Track large-scale transaction imports with batching,
progress tracking, resume capability, and detailed metrics.
"""
from sqlalchemy import (
    Column, Integer, String, Float, Date, DateTime, Boolean,
    Text, JSON, ForeignKey, Index, BigInteger, Enum as SQLEnum
)
from sqlalchemy.orm import relationship
from datetime import datetime
import enum
from app.core.database import Base


class BulkImportStatus(str, enum.Enum):
    """Status of bulk import job"""
    PENDING = "pending"           # Job created, not started
    UPLOADING = "uploading"       # File being uploaded
    VALIDATING = "validating"     # File being validated
    PROCESSING = "processing"     # Actively importing
    PAUSED = "paused"             # Manually paused
    COMPLETING = "completing"     # Final cleanup/commit
    COMPLETED = "completed"       # Successfully finished
    COMPLETED_WITH_ERRORS = "completed_with_errors"  # Finished with some errors
    FAILED = "failed"             # Fatal error, cannot continue
    CANCELLED = "cancelled"       # User cancelled


class BatchStatus(str, enum.Enum):
    """Status of individual batch within import"""
    PENDING = "pending"
    PROCESSING = "processing"
    COMPLETED = "completed"
    FAILED = "failed"
    SKIPPED = "skipped"  # Skipped due to job cancellation


class BulkImportJob(Base):
    """
    Tracks a bulk import job with progress, metrics, and resume capability.

    Design:
    - File is uploaded and stored, creating the job in PENDING state
    - Job transitions to VALIDATING, then PROCESSING
    - Processing happens in batches (BulkImportBatch)
    - Each batch commits independently for fault tolerance
    - Job can be paused/resumed at batch boundaries
    - Detailed metrics track throughput and errors
    """
    __tablename__ = "bulk_import_jobs"

    id = Column(Integer, primary_key=True, index=True)

    # Job identification
    job_id = Column(String(64), unique=True, nullable=False, index=True)  # UUID for external reference
    source = Column(String(50), default="blackdiamond", nullable=False)

    # File info
    file_name = Column(String(255))
    file_hash = Column(String(64), index=True)
    file_size_bytes = Column(BigInteger)
    file_path = Column(String(500))  # Path to uploaded file for resume

    # Status tracking
    status = Column(SQLEnum(BulkImportStatus), default=BulkImportStatus.PENDING, index=True)
    status_message = Column(Text)  # Human-readable status details

    # Progress tracking
    total_rows = Column(Integer, default=0)
    rows_processed = Column(Integer, default=0)  # Includes skipped duplicates
    rows_imported = Column(Integer, default=0)   # Actually inserted
    rows_skipped = Column(Integer, default=0)    # Duplicate/unchanged
    rows_error = Column(Integer, default=0)      # Failed rows

    # Batch tracking
    total_batches = Column(Integer, default=0)
    batches_completed = Column(Integer, default=0)
    current_batch = Column(Integer, default=0)
    batch_size = Column(Integer, default=5000)  # Rows per batch

    # Resume support
    last_processed_row = Column(Integer, default=0)
    checkpoint_data = Column(JSON)  # Additional state for resume

    # Timing metrics
    created_at = Column(DateTime, default=datetime.utcnow)
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    paused_at = Column(DateTime)

    # Performance metrics
    avg_rows_per_second = Column(Float)
    estimated_completion = Column(DateTime)

    # Options
    skip_analytics = Column(Boolean, default=True)  # Don't trigger price fetches
    validate_only = Column(Boolean, default=False)  # Just validate, don't import

    # Error tracking
    errors_sample = Column(JSON)  # Sample of errors (limit to 100)
    error_file_path = Column(String(500))  # Full error log file path

    # User tracking
    created_by = Column(Integer, ForeignKey("users.id"))

    # Relationships
    batches = relationship("BulkImportBatch", back_populates="job", cascade="all, delete-orphan")

    def progress_percent(self) -> float:
        """Calculate progress percentage"""
        if self.total_rows == 0:
            return 0.0
        return (self.rows_processed / self.total_rows) * 100

    def is_resumable(self) -> bool:
        """Check if job can be resumed"""
        return self.status in [
            BulkImportStatus.PAUSED,
            BulkImportStatus.FAILED,
            BulkImportStatus.PROCESSING  # May have crashed
        ]

    def to_dict(self) -> dict:
        """Convert to dictionary for API response"""
        return {
            "job_id": self.job_id,
            "status": self.status.value if self.status else None,
            "status_message": self.status_message,
            "file_name": self.file_name,
            "total_rows": self.total_rows,
            "rows_processed": self.rows_processed,
            "rows_imported": self.rows_imported,
            "rows_skipped": self.rows_skipped,
            "rows_error": self.rows_error,
            "progress_percent": round(self.progress_percent(), 2),
            "total_batches": self.total_batches,
            "batches_completed": self.batches_completed,
            "current_batch": self.current_batch,
            "batch_size": self.batch_size,
            "created_at": self.created_at.isoformat() if self.created_at else None,
            "started_at": self.started_at.isoformat() if self.started_at else None,
            "completed_at": self.completed_at.isoformat() if self.completed_at else None,
            "estimated_completion": self.estimated_completion.isoformat() if self.estimated_completion else None,
            "avg_rows_per_second": round(self.avg_rows_per_second, 2) if self.avg_rows_per_second else None,
            "errors_sample": self.errors_sample,
            "is_resumable": self.is_resumable(),
        }


class BulkImportBatch(Base):
    """
    Tracks individual batch within a bulk import job.

    Each batch:
    - Processes a contiguous range of rows
    - Commits independently
    - Can be retried if failed
    - Tracks its own metrics
    """
    __tablename__ = "bulk_import_batches"

    id = Column(Integer, primary_key=True, index=True)

    job_id = Column(Integer, ForeignKey("bulk_import_jobs.id"), nullable=False, index=True)
    batch_number = Column(Integer, nullable=False)

    # Row range (0-indexed from file)
    start_row = Column(Integer, nullable=False)
    end_row = Column(Integer, nullable=False)

    # Status
    status = Column(SQLEnum(BatchStatus), default=BatchStatus.PENDING)
    error_message = Column(Text)

    # Metrics
    rows_in_batch = Column(Integer, default=0)
    rows_imported = Column(Integer, default=0)
    rows_skipped = Column(Integer, default=0)
    rows_error = Column(Integer, default=0)

    # Timing
    started_at = Column(DateTime)
    completed_at = Column(DateTime)
    duration_ms = Column(Integer)  # Processing time in milliseconds

    # Retry tracking
    attempt_count = Column(Integer, default=0)
    max_attempts = Column(Integer, default=3)

    # Relationship
    job = relationship("BulkImportJob", back_populates="batches")

    __table_args__ = (
        Index('idx_batch_job_number', 'job_id', 'batch_number'),
    )


class ImportedTransaction(Base):
    """
    Temporary staging table for bulk imports.

    Transactions are first inserted here, then moved to the main
    transactions table after validation. This allows for:
    - Atomic batch commits
    - Easy rollback of entire import
    - Validation without affecting live data
    """
    __tablename__ = "imported_transactions_staging"

    id = Column(Integer, primary_key=True, index=True)
    bulk_import_job_id = Column(Integer, ForeignKey("bulk_import_jobs.id"), index=True)
    batch_id = Column(Integer, ForeignKey("bulk_import_batches.id"), index=True)

    # Row tracking
    source_row_number = Column(Integer)  # Original row in CSV
    source_txn_key = Column(String(64), index=True)  # Idempotency key

    # Raw data (before transformation)
    raw_data = Column(JSON)

    # Parsed/validated data
    account_number = Column(String(50), index=True)
    account_display_name = Column(String(255))
    symbol = Column(String(20), index=True)
    asset_name = Column(String(255))
    asset_class = Column(String(50))
    trade_date = Column(Date)
    settle_date = Column(Date)
    transaction_type = Column(String(50))
    price = Column(Float)
    units = Column(Float)
    market_value = Column(Float)
    transaction_fee = Column(Float)

    # Validation status
    is_valid = Column(Boolean, default=True)
    validation_errors = Column(JSON)
    is_duplicate = Column(Boolean, default=False)

    # Final status
    was_imported = Column(Boolean, default=False)
    final_transaction_id = Column(Integer, ForeignKey("transactions.id"))

    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        Index('idx_staging_job_batch', 'bulk_import_job_id', 'batch_id'),
        Index('idx_staging_account', 'account_number'),
    )
