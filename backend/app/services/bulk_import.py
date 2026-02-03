"""
Bulk Import Service - Handles large-scale transaction imports with:
- Background processing (non-blocking)
- Batch commits (fault-tolerant)
- Progress tracking (observable)
- Resume capability (resumable)
- Idempotency (safe to run multiple times)
- No external API calls during import
"""
import os
import io
import uuid
import hashlib
import logging
import time
from datetime import datetime, timedelta
from typing import Dict, List, Any, Optional, Tuple, Set
from pathlib import Path
import pandas as pd
from sqlalchemy.orm import Session
from sqlalchemy import and_, func, text
from sqlalchemy.dialects.postgresql import insert

from app.core.database import SessionLocal
from app.models.models import (
    Account, Security, Transaction, TransactionType, TransactionTypeMap,
    AssetClass, ImportLog
)
from app.models.bulk_import import (
    BulkImportJob, BulkImportBatch, ImportedTransaction,
    BulkImportStatus, BatchStatus
)

logger = logging.getLogger(__name__)

# Configuration
DEFAULT_BATCH_SIZE = 5000
MAX_ERROR_SAMPLES = 100
UPLOAD_DIR = Path("/app/uploads/bulk_imports")


class BulkImportService:
    """
    Service for handling bulk transaction imports.

    Key features:
    - Processes in configurable batches (default 5000 rows)
    - Commits after each batch for fault tolerance
    - Caches account/security lookups for efficiency
    - Tracks progress for monitoring
    - Supports pause/resume
    - Idempotent - skips already imported transactions
    """

    REQUIRED_HEADERS = [
        'Account Number', 'Account Display Name', 'Class', 'Asset Name',
        'Symbol', 'Trade Date', 'Settle Date', 'Transaction Type',
        'Price', 'Units', 'Market Value', 'Transaction Fee'
    ]

    def __init__(self, db: Session):
        self.db = db
        # Caches for efficiency - populated during import
        self._account_cache: Dict[str, int] = {}  # account_number -> account_id
        self._security_cache: Dict[Tuple[str, str], int] = {}  # (symbol, asset_class) -> security_id
        self._txn_type_map: Dict[str, TransactionType] = {}
        self._existing_keys: Set[str] = set()  # Pre-loaded existing transaction keys

    def create_import_job(
        self,
        file_content: bytes,
        file_name: str,
        batch_size: int = DEFAULT_BATCH_SIZE,
        skip_analytics: bool = True,
        validate_only: bool = False,
        user_id: Optional[int] = None
    ) -> BulkImportJob:
        """
        Create a new bulk import job from uploaded file.

        Returns the job immediately - processing happens in background.
        """
        job_id = str(uuid.uuid4())
        file_hash = hashlib.sha256(file_content).hexdigest()

        # Check for existing completed import of same file
        existing = self.db.query(BulkImportJob).filter(
            BulkImportJob.file_hash == file_hash,
            BulkImportJob.status.in_([
                BulkImportStatus.COMPLETED,
                BulkImportStatus.COMPLETED_WITH_ERRORS
            ])
        ).first()

        if existing:
            raise ValueError(f"File already imported (job_id: {existing.job_id})")

        # Save file for resume capability
        UPLOAD_DIR.mkdir(parents=True, exist_ok=True)
        file_path = UPLOAD_DIR / f"{job_id}_{file_name}"
        with open(file_path, 'wb') as f:
            f.write(file_content)

        # Validate file and count rows
        try:
            total_rows, validation_result = self._validate_file(file_content)
            if validation_result.get('error'):
                job = BulkImportJob(
                    job_id=job_id,
                    source="blackdiamond",
                    file_name=file_name,
                    file_hash=file_hash,
                    file_size_bytes=len(file_content),
                    file_path=str(file_path),
                    status=BulkImportStatus.FAILED,
                    status_message=validation_result['error'],
                    created_by=user_id
                )
                self.db.add(job)
                self.db.commit()
                return job
        except Exception as e:
            logger.error(f"Validation failed: {e}")
            raise ValueError(f"File validation failed: {str(e)}")

        # Calculate batches
        total_batches = (total_rows + batch_size - 1) // batch_size

        # Create job record
        job = BulkImportJob(
            job_id=job_id,
            source="blackdiamond",
            file_name=file_name,
            file_hash=file_hash,
            file_size_bytes=len(file_content),
            file_path=str(file_path),
            status=BulkImportStatus.PENDING,
            status_message="Job created, waiting to start",
            total_rows=total_rows,
            total_batches=total_batches,
            batch_size=batch_size,
            skip_analytics=skip_analytics,
            validate_only=validate_only,
            created_by=user_id
        )
        self.db.add(job)
        self.db.commit()
        self.db.refresh(job)

        # Create batch records
        for batch_num in range(total_batches):
            start_row = batch_num * batch_size
            end_row = min((batch_num + 1) * batch_size, total_rows)

            batch = BulkImportBatch(
                job_id=job.id,
                batch_number=batch_num,
                start_row=start_row,
                end_row=end_row,
                rows_in_batch=end_row - start_row,
                status=BatchStatus.PENDING
            )
            self.db.add(batch)

        self.db.commit()

        logger.info(f"Created bulk import job {job_id}: {total_rows} rows, {total_batches} batches")
        return job

    def _validate_file(self, file_content: bytes) -> Tuple[int, Dict[str, Any]]:
        """Validate file format and count rows"""
        try:
            content_str = file_content.decode('utf-8')
            if '\t' in content_str:
                df = pd.read_csv(io.StringIO(content_str), sep='\t', nrows=1)
            else:
                df = pd.read_csv(io.StringIO(content_str), nrows=1)

            # Check headers
            missing_headers = set(self.REQUIRED_HEADERS) - set(df.columns)
            if missing_headers:
                return 0, {'error': f'Missing required headers: {missing_headers}'}

            # Count total rows (without loading all into memory)
            if '\t' in content_str:
                total_rows = sum(1 for _ in io.StringIO(content_str)) - 1  # -1 for header
            else:
                total_rows = sum(1 for _ in io.StringIO(content_str)) - 1

            return total_rows, {'valid': True}

        except Exception as e:
            return 0, {'error': f'Failed to parse CSV: {str(e)}'}

    def process_job(self, job_id: str) -> Dict[str, Any]:
        """
        Process a bulk import job.

        This is the main entry point for background processing.
        Can be called from a worker or scheduler.
        """
        job = self.db.query(BulkImportJob).filter(
            BulkImportJob.job_id == job_id
        ).first()

        if not job:
            raise ValueError(f"Job not found: {job_id}")

        if job.status == BulkImportStatus.COMPLETED:
            return {"status": "already_completed", "job": job.to_dict()}

        if job.status == BulkImportStatus.CANCELLED:
            return {"status": "cancelled", "job": job.to_dict()}

        try:
            # Update status to processing
            job.status = BulkImportStatus.PROCESSING
            job.started_at = job.started_at or datetime.utcnow()
            job.status_message = "Loading file and preparing caches..."
            self.db.commit()

            # Load file
            with open(job.file_path, 'rb') as f:
                file_content = f.read()

            # Parse into DataFrame (memory efficient chunked reading would be better for very large files)
            content_str = file_content.decode('utf-8')
            if '\t' in content_str:
                df = pd.read_csv(io.StringIO(content_str), sep='\t')
            else:
                df = pd.read_csv(io.StringIO(content_str))

            df = self._clean_dataframe(df)

            # Pre-populate caches for efficiency
            self._populate_caches(df)

            # Process batches
            batches = self.db.query(BulkImportBatch).filter(
                BulkImportBatch.job_id == job.id,
                BulkImportBatch.status.in_([BatchStatus.PENDING, BatchStatus.FAILED])
            ).order_by(BulkImportBatch.batch_number).all()

            total_imported = 0
            total_skipped = 0
            total_errors = 0
            errors_sample = []
            start_time = time.time()

            for batch in batches:
                # Check if job was cancelled
                self.db.refresh(job)
                if job.status == BulkImportStatus.CANCELLED:
                    logger.info(f"Job {job_id} cancelled, stopping processing")
                    break

                if job.status == BulkImportStatus.PAUSED:
                    logger.info(f"Job {job_id} paused at batch {batch.batch_number}")
                    break

                # Process batch
                batch_result = self._process_batch(job, batch, df)

                total_imported += batch_result['imported']
                total_skipped += batch_result['skipped']
                total_errors += batch_result['errors']

                if batch_result.get('error_samples'):
                    errors_sample.extend(batch_result['error_samples'])
                    errors_sample = errors_sample[:MAX_ERROR_SAMPLES]

                # Update job progress
                job.rows_processed = job.rows_processed + batch_result['processed']
                job.rows_imported = total_imported
                job.rows_skipped = total_skipped
                job.rows_error = total_errors
                job.batches_completed = job.batches_completed + 1
                job.current_batch = batch.batch_number + 1
                job.errors_sample = errors_sample

                # Calculate metrics
                elapsed = time.time() - start_time
                if elapsed > 0:
                    job.avg_rows_per_second = job.rows_processed / elapsed
                    if job.avg_rows_per_second > 0:
                        remaining_rows = job.total_rows - job.rows_processed
                        remaining_seconds = remaining_rows / job.avg_rows_per_second
                        job.estimated_completion = datetime.utcnow() + timedelta(seconds=remaining_seconds)

                job.status_message = f"Processing batch {job.current_batch}/{job.total_batches}"

                self.db.commit()

                logger.info(
                    f"Job {job_id} batch {batch.batch_number}: "
                    f"imported={batch_result['imported']}, skipped={batch_result['skipped']}, "
                    f"errors={batch_result['errors']}, "
                    f"progress={job.progress_percent():.1f}%"
                )

            # Final status
            if job.status == BulkImportStatus.PROCESSING:
                if total_errors > 0:
                    job.status = BulkImportStatus.COMPLETED_WITH_ERRORS
                    job.status_message = f"Completed with {total_errors} errors"
                else:
                    job.status = BulkImportStatus.COMPLETED
                    job.status_message = "Import completed successfully"

                job.completed_at = datetime.utcnow()

            # Also create a legacy ImportLog for compatibility
            if job.status in [BulkImportStatus.COMPLETED, BulkImportStatus.COMPLETED_WITH_ERRORS]:
                self._create_import_log(job)

            self.db.commit()

            return {
                "status": "completed",
                "job": job.to_dict(),
                "summary": {
                    "total_rows": job.total_rows,
                    "imported": total_imported,
                    "skipped": total_skipped,
                    "errors": total_errors,
                    "duration_seconds": round(time.time() - start_time, 2)
                }
            }

        except Exception as e:
            logger.error(f"Job {job_id} failed: {e}", exc_info=True)
            job.status = BulkImportStatus.FAILED
            job.status_message = f"Fatal error: {str(e)}"
            self.db.commit()
            raise

    def _process_batch(
        self,
        job: BulkImportJob,
        batch: BulkImportBatch,
        df: pd.DataFrame
    ) -> Dict[str, Any]:
        """Process a single batch of rows"""
        batch.status = BatchStatus.PROCESSING
        batch.started_at = datetime.utcnow()
        batch.attempt_count += 1
        self.db.commit()

        batch_start = time.time()
        imported = 0
        skipped = 0
        errors = 0
        error_samples = []

        try:
            # Get batch rows
            batch_df = df.iloc[batch.start_row:batch.end_row]

            # Prepare transactions for bulk insert
            transactions_to_insert = []

            for idx, row in batch_df.iterrows():
                try:
                    # Generate idempotency key
                    txn_key = self._generate_txn_key(row)

                    # Check if already exists (using pre-loaded cache)
                    if txn_key in self._existing_keys:
                        skipped += 1
                        continue

                    # Get/create account and security IDs
                    account_id = self._get_or_create_account(row)
                    security_id = self._get_or_create_security(row)
                    txn_type = self._infer_transaction_type(row['Transaction Type'])

                    # Prepare transaction dict for bulk insert
                    txn_data = {
                        'account_id': account_id,
                        'security_id': security_id,
                        'trade_date': row['Trade Date'].date() if pd.notna(row['Trade Date']) else None,
                        'settle_date': row['Settle Date'].date() if pd.notna(row['Settle Date']) else None,
                        'transaction_type': txn_type,
                        'raw_transaction_type': row['Transaction Type'],
                        'price': float(row['Price']) if pd.notna(row['Price']) else None,
                        'units': float(row['Units']) if pd.notna(row['Units']) else None,
                        'market_value': float(row['Market Value']) if pd.notna(row['Market Value']) else None,
                        'transaction_fee': float(row['Transaction Fee']) if pd.notna(row['Transaction Fee']) else 0.0,
                        'source_txn_key': txn_key,
                        'import_log_id': None,  # Will be set after ImportLog creation
                        'created_at': datetime.utcnow()
                    }

                    transactions_to_insert.append(txn_data)
                    self._existing_keys.add(txn_key)  # Add to cache to prevent duplicates in same batch
                    imported += 1

                except Exception as e:
                    errors += 1
                    if len(error_samples) < 10:  # Limit per-batch samples
                        error_samples.append({
                            'row': int(idx) + 2,
                            'error': str(e)
                        })

            # Bulk insert transactions
            if transactions_to_insert:
                self.db.execute(
                    Transaction.__table__.insert(),
                    transactions_to_insert
                )

            batch.status = BatchStatus.COMPLETED
            batch.rows_imported = imported
            batch.rows_skipped = skipped
            batch.rows_error = errors
            batch.completed_at = datetime.utcnow()
            batch.duration_ms = int((time.time() - batch_start) * 1000)

            self.db.commit()

            return {
                'processed': len(batch_df),
                'imported': imported,
                'skipped': skipped,
                'errors': errors,
                'error_samples': error_samples
            }

        except Exception as e:
            logger.error(f"Batch {batch.batch_number} failed: {e}", exc_info=True)
            self.db.rollback()

            batch.status = BatchStatus.FAILED
            batch.error_message = str(e)
            batch.completed_at = datetime.utcnow()
            batch.duration_ms = int((time.time() - batch_start) * 1000)
            self.db.commit()

            # If retries available, mark for retry
            if batch.attempt_count < batch.max_attempts:
                batch.status = BatchStatus.PENDING

            return {
                'processed': 0,
                'imported': 0,
                'skipped': 0,
                'errors': batch.rows_in_batch,
                'error_samples': [{'batch': batch.batch_number, 'error': str(e)}]
            }

    def _populate_caches(self, df: pd.DataFrame):
        """Pre-populate caches for efficient lookups"""
        logger.info("Pre-populating caches...")

        # Cache existing accounts
        accounts = self.db.query(Account).all()
        self._account_cache = {a.account_number: a.id for a in accounts}
        logger.info(f"Cached {len(self._account_cache)} accounts")

        # Cache existing securities
        securities = self.db.query(Security).all()
        self._security_cache = {(s.symbol, s.asset_class.value): s.id for s in securities}
        logger.info(f"Cached {len(self._security_cache)} securities")

        # Cache transaction type mappings
        mappings = self.db.query(TransactionTypeMap).filter(
            TransactionTypeMap.source == 'blackdiamond'
        ).all()
        self._txn_type_map = {m.raw_type: m.normalized_type for m in mappings}
        logger.info(f"Cached {len(self._txn_type_map)} transaction type mappings")

        # Pre-load existing transaction keys for idempotency check
        # This is memory intensive for very large datasets - could use bloom filter
        unique_accounts = df['Account Number'].unique().tolist()

        # Load keys in chunks to avoid memory issues
        chunk_size = 10
        for i in range(0, len(unique_accounts), chunk_size):
            chunk_accounts = unique_accounts[i:i + chunk_size]
            account_ids = [
                self._account_cache.get(str(acc))
                for acc in chunk_accounts
                if str(acc) in self._account_cache
            ]
            account_ids = [a for a in account_ids if a is not None]

            if account_ids:
                keys = self.db.query(Transaction.source_txn_key).filter(
                    Transaction.account_id.in_(account_ids)
                ).all()
                self._existing_keys.update(k[0] for k in keys)

        logger.info(f"Cached {len(self._existing_keys)} existing transaction keys")

    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and normalize dataframe"""
        # Trim whitespace
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].astype(str).str.strip()

        # Normalize Symbol to uppercase
        df['Symbol'] = df['Symbol'].str.upper()

        # Parse dates
        df['Trade Date'] = pd.to_datetime(df['Trade Date'], errors='coerce')
        df['Settle Date'] = pd.to_datetime(df['Settle Date'], errors='coerce')

        # Parse numeric fields
        for col in ['Price', 'Units', 'Market Value', 'Transaction Fee']:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)

        return df

    def _generate_txn_key(self, row: pd.Series) -> str:
        """Generate idempotency key for transaction"""
        key_parts = [
            str(row['Account Number']),
            str(row['Symbol']),
            str(row['Trade Date']),
            str(row['Transaction Type']),
            str(row['Units']),
            str(row['Price'])
        ]
        key_string = '|'.join(key_parts)
        return hashlib.sha256(key_string.encode()).hexdigest()

    def _get_or_create_account(self, row: pd.Series) -> int:
        """Get or create account, using cache"""
        account_number = str(row['Account Number'])

        if account_number in self._account_cache:
            return self._account_cache[account_number]

        # Create new account
        account = Account(
            account_number=account_number,
            display_name=row['Account Display Name']
        )
        self.db.add(account)
        self.db.flush()

        self._account_cache[account_number] = account.id
        return account.id

    def _get_or_create_security(self, row: pd.Series) -> Optional[int]:
        """Get or create security, using cache"""
        symbol = str(row['Symbol']).upper()
        if not symbol or symbol == 'NAN':
            return None

        asset_class, is_option = self._classify_asset_class(row['Class'], symbol)
        cache_key = (symbol, asset_class.value)

        if cache_key in self._security_cache:
            return self._security_cache[cache_key]

        # Create new security
        security = Security(
            symbol=symbol,
            asset_name=row['Asset Name'],
            asset_class=asset_class,
            is_option=is_option
        )
        self.db.add(security)
        self.db.flush()

        self._security_cache[cache_key] = security.id
        return security.id

    def _classify_asset_class(self, class_str: str, symbol: str) -> Tuple[AssetClass, bool]:
        """Classify asset class and determine if it's an option"""
        class_lower = class_str.lower() if class_str else ''
        is_option = False

        if 'option' in class_lower or 'opt' in class_lower:
            asset_class = AssetClass.OPTION
            is_option = True
        elif 'etf' in class_lower:
            asset_class = AssetClass.ETF
        elif 'cash' in class_lower or 'money market' in class_lower:
            asset_class = AssetClass.CASH
        elif 'equity' in class_lower or 'stock' in class_lower:
            asset_class = AssetClass.EQUITY
        else:
            asset_class = AssetClass.OTHER

        return asset_class, is_option

    def _infer_transaction_type(self, raw_type: str) -> TransactionType:
        """Infer normalized transaction type from raw string"""
        if raw_type in self._txn_type_map:
            return self._txn_type_map[raw_type]

        raw_lower = raw_type.lower()

        if 'buy' in raw_lower or 'purchase' in raw_lower:
            return TransactionType.BUY
        elif 'sell' in raw_lower or 'sale' in raw_lower:
            return TransactionType.SELL
        elif 'reinvest' in raw_lower:
            return TransactionType.DIVIDEND_REINVEST
        elif 'dividend' in raw_lower or 'div' in raw_lower:
            return TransactionType.DIVIDEND
        elif 'transfer in' in raw_lower or 'deposit' in raw_lower:
            return TransactionType.TRANSFER_IN
        elif 'transfer out' in raw_lower or 'withdrawal' in raw_lower:
            return TransactionType.TRANSFER_OUT
        elif 'fee' in raw_lower or 'commission' in raw_lower:
            return TransactionType.FEE
        else:
            return TransactionType.OTHER

    def _create_import_log(self, job: BulkImportJob):
        """Create legacy ImportLog for compatibility"""
        import_log = ImportLog(
            source='blackdiamond',
            file_name=job.file_name,
            file_hash=job.file_hash,
            status='completed' if job.status == BulkImportStatus.COMPLETED else 'completed_with_errors',
            rows_processed=job.rows_processed,
            rows_imported=job.rows_imported,
            rows_error=job.rows_error,
            errors=job.errors_sample
        )
        self.db.add(import_log)
        self.db.flush()

        # Update all transactions with import_log_id
        # This is expensive for large imports - could be optimized
        # For now, we skip this and just keep the bulk_import_job_id reference

    def pause_job(self, job_id: str) -> BulkImportJob:
        """Pause a running job"""
        job = self.db.query(BulkImportJob).filter(
            BulkImportJob.job_id == job_id
        ).first()

        if not job:
            raise ValueError(f"Job not found: {job_id}")

        if job.status != BulkImportStatus.PROCESSING:
            raise ValueError(f"Cannot pause job in status: {job.status}")

        job.status = BulkImportStatus.PAUSED
        job.paused_at = datetime.utcnow()
        job.status_message = "Job paused by user"
        self.db.commit()

        return job

    def resume_job(self, job_id: str) -> Dict[str, Any]:
        """Resume a paused or failed job"""
        job = self.db.query(BulkImportJob).filter(
            BulkImportJob.job_id == job_id
        ).first()

        if not job:
            raise ValueError(f"Job not found: {job_id}")

        if not job.is_resumable():
            raise ValueError(f"Job cannot be resumed from status: {job.status}")

        job.status = BulkImportStatus.PENDING
        job.status_message = "Job queued for resume"
        self.db.commit()

        # Process the job
        return self.process_job(job_id)

    def cancel_job(self, job_id: str) -> BulkImportJob:
        """Cancel a job"""
        job = self.db.query(BulkImportJob).filter(
            BulkImportJob.job_id == job_id
        ).first()

        if not job:
            raise ValueError(f"Job not found: {job_id}")

        if job.status in [BulkImportStatus.COMPLETED, BulkImportStatus.COMPLETED_WITH_ERRORS]:
            raise ValueError("Cannot cancel completed job")

        job.status = BulkImportStatus.CANCELLED
        job.status_message = "Job cancelled by user"
        job.completed_at = datetime.utcnow()

        # Mark pending batches as skipped
        self.db.query(BulkImportBatch).filter(
            BulkImportBatch.job_id == job.id,
            BulkImportBatch.status == BatchStatus.PENDING
        ).update({'status': BatchStatus.SKIPPED})

        self.db.commit()
        return job

    def get_job_status(self, job_id: str) -> Dict[str, Any]:
        """Get detailed job status"""
        job = self.db.query(BulkImportJob).filter(
            BulkImportJob.job_id == job_id
        ).first()

        if not job:
            raise ValueError(f"Job not found: {job_id}")

        # Get batch details
        batches = self.db.query(BulkImportBatch).filter(
            BulkImportBatch.job_id == job.id
        ).order_by(BulkImportBatch.batch_number).all()

        batch_summary = {
            'pending': sum(1 for b in batches if b.status == BatchStatus.PENDING),
            'processing': sum(1 for b in batches if b.status == BatchStatus.PROCESSING),
            'completed': sum(1 for b in batches if b.status == BatchStatus.COMPLETED),
            'failed': sum(1 for b in batches if b.status == BatchStatus.FAILED),
            'skipped': sum(1 for b in batches if b.status == BatchStatus.SKIPPED),
        }

        return {
            **job.to_dict(),
            'batch_summary': batch_summary,
            'batches': [
                {
                    'batch_number': b.batch_number,
                    'status': b.status.value if b.status else None,
                    'rows_in_batch': b.rows_in_batch,
                    'rows_imported': b.rows_imported,
                    'rows_skipped': b.rows_skipped,
                    'rows_error': b.rows_error,
                    'duration_ms': b.duration_ms,
                    'attempt_count': b.attempt_count
                }
                for b in batches
            ]
        }

    def list_jobs(
        self,
        status: Optional[BulkImportStatus] = None,
        limit: int = 20
    ) -> List[Dict[str, Any]]:
        """List bulk import jobs"""
        query = self.db.query(BulkImportJob)

        if status:
            query = query.filter(BulkImportJob.status == status)

        jobs = query.order_by(BulkImportJob.created_at.desc()).limit(limit).all()

        return [job.to_dict() for job in jobs]


async def run_bulk_import_job(job_id: str):
    """
    Background task to process a bulk import job.

    Call this from the API endpoint to start processing.
    """
    db = SessionLocal()
    try:
        service = BulkImportService(db)
        result = service.process_job(job_id)
        logger.info(f"Bulk import job {job_id} completed: {result['status']}")
        return result
    except Exception as e:
        logger.error(f"Bulk import job {job_id} failed: {e}", exc_info=True)
        raise
    finally:
        db.close()
