"""
Bulk Import API - Endpoints for large-scale transaction imports with:
- Non-blocking upload and processing
- Progress tracking
- Pause/resume/cancel capabilities
- Background job processing
"""
import asyncio
import logging
from fastapi import APIRouter, Depends, UploadFile, File, Query, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from typing import Optional

from app.core.database import get_db
from app.api.auth import get_current_user
from app.models import User
from app.models.bulk_import import BulkImportStatus
from app.services.bulk_import import BulkImportService, run_bulk_import_job

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/imports/bulk", tags=["bulk-imports"])


@router.post("/start")
async def start_bulk_import(
    background_tasks: BackgroundTasks,
    file: UploadFile = File(...),
    batch_size: int = Query(5000, ge=100, le=50000, description="Rows per batch"),
    skip_analytics: bool = Query(True, description="Skip price fetches and analytics during import"),
    validate_only: bool = Query(False, description="Only validate, don't import"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Start a bulk import job.

    The file is validated and the job is created immediately.
    Processing happens in the background - use the job_id to check status.

    Parameters:
    - file: CSV file to import
    - batch_size: Number of rows to process per batch (default 5000)
    - skip_analytics: Don't trigger price fetches or analytics recomputation (default True)
    - validate_only: Just validate the file without importing (default False)

    Returns:
    - job_id: UUID to track the import
    - status: Current job status
    - total_rows: Total rows in file
    - total_batches: Number of batches to process
    """
    # Read file content
    file_content = await file.read()

    if len(file_content) == 0:
        raise HTTPException(status_code=400, detail="Empty file")

    # Create service and job
    service = BulkImportService(db)

    try:
        job = service.create_import_job(
            file_content=file_content,
            file_name=file.filename,
            batch_size=batch_size,
            skip_analytics=skip_analytics,
            validate_only=validate_only,
            user_id=current_user.id
        )
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    # If job was created successfully (not failed during validation), queue for processing
    if job.status == BulkImportStatus.PENDING:
        # Add to background tasks for async processing
        background_tasks.add_task(process_job_background, job.job_id)

    return {
        "job_id": job.job_id,
        "status": job.status.value,
        "status_message": job.status_message,
        "total_rows": job.total_rows,
        "total_batches": job.total_batches,
        "batch_size": job.batch_size,
        "message": "Job created and queued for processing" if job.status == BulkImportStatus.PENDING else job.status_message
    }


async def process_job_background(job_id: str):
    """Background task to process the import job"""
    try:
        await run_bulk_import_job(job_id)
    except Exception as e:
        logger.error(f"Background job {job_id} failed: {e}", exc_info=True)


@router.get("/{job_id}/status")
def get_job_status(
    job_id: str,
    include_batches: bool = Query(False, description="Include per-batch details"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get detailed status of a bulk import job.

    Parameters:
    - job_id: The job UUID
    - include_batches: Include per-batch details (default False)

    Returns:
    - Complete job status including progress, metrics, and errors
    """
    service = BulkImportService(db)

    try:
        status = service.get_job_status(job_id)
    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    if not include_batches:
        status.pop('batches', None)

    return status


@router.post("/{job_id}/pause")
def pause_job(
    job_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Pause a running bulk import job.

    The job will stop at the next batch boundary and can be resumed later.
    """
    service = BulkImportService(db)

    try:
        job = service.pause_job(job_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return {
        "job_id": job.job_id,
        "status": job.status.value,
        "message": "Job paused successfully",
        "progress_percent": round(job.progress_percent(), 2),
        "rows_processed": job.rows_processed,
        "rows_imported": job.rows_imported
    }


@router.post("/{job_id}/resume")
async def resume_job(
    job_id: str,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Resume a paused or failed bulk import job.

    Processing will continue from where it left off.
    """
    service = BulkImportService(db)

    try:
        # Verify job is resumable
        status = service.get_job_status(job_id)
        if not status.get('is_resumable'):
            raise HTTPException(
                status_code=400,
                detail=f"Job cannot be resumed from status: {status.get('status')}"
            )

        # Queue for background processing
        background_tasks.add_task(resume_job_background, job_id)

    except ValueError as e:
        raise HTTPException(status_code=404, detail=str(e))

    return {
        "job_id": job_id,
        "status": "queued",
        "message": "Job queued for resume"
    }


async def resume_job_background(job_id: str):
    """Background task to resume the import job"""
    from app.core.database import SessionLocal
    db = SessionLocal()
    try:
        service = BulkImportService(db)
        result = service.resume_job(job_id)
        logger.info(f"Resume job {job_id} completed: {result.get('status')}")
    except Exception as e:
        logger.error(f"Resume job {job_id} failed: {e}", exc_info=True)
    finally:
        db.close()


@router.post("/{job_id}/cancel")
def cancel_job(
    job_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Cancel a bulk import job.

    Already imported transactions will remain in the database.
    """
    service = BulkImportService(db)

    try:
        job = service.cancel_job(job_id)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))

    return {
        "job_id": job.job_id,
        "status": job.status.value,
        "message": "Job cancelled",
        "rows_imported": job.rows_imported,
        "rows_remaining": job.total_rows - job.rows_processed
    }


@router.get("")
def list_jobs(
    status: Optional[str] = Query(None, description="Filter by status"),
    limit: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    List bulk import jobs.

    Parameters:
    - status: Optional status filter
    - limit: Maximum number of jobs to return (default 20)
    """
    service = BulkImportService(db)

    status_enum = None
    if status:
        try:
            status_enum = BulkImportStatus(status)
        except ValueError:
            raise HTTPException(status_code=400, detail=f"Invalid status: {status}")

    jobs = service.list_jobs(status=status_enum, limit=limit)
    return {"jobs": jobs, "count": len(jobs)}


@router.get("/{job_id}/errors")
def get_job_errors(
    job_id: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get detailed error information for a bulk import job.
    """
    from app.models.bulk_import import BulkImportJob, BulkImportBatch, BatchStatus

    job = db.query(BulkImportJob).filter(BulkImportJob.job_id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    # Get failed batches
    failed_batches = db.query(BulkImportBatch).filter(
        BulkImportBatch.job_id == job.id,
        BulkImportBatch.status == BatchStatus.FAILED
    ).all()

    return {
        "job_id": job.job_id,
        "total_errors": job.rows_error,
        "errors_sample": job.errors_sample or [],
        "failed_batches": [
            {
                "batch_number": b.batch_number,
                "start_row": b.start_row,
                "end_row": b.end_row,
                "error_message": b.error_message,
                "attempt_count": b.attempt_count
            }
            for b in failed_batches
        ]
    }


@router.delete("/{job_id}")
def delete_job(
    job_id: str,
    delete_transactions: bool = Query(False, description="Also delete imported transactions"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Delete a bulk import job and optionally its transactions.

    Parameters:
    - delete_transactions: Also delete transactions imported by this job (default False)
    """
    from app.models.bulk_import import BulkImportJob, BulkImportBatch
    from app.models import Transaction

    job = db.query(BulkImportJob).filter(BulkImportJob.job_id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.status == BulkImportStatus.PROCESSING:
        raise HTTPException(status_code=400, detail="Cannot delete job while processing")

    txn_count = 0
    if delete_transactions:
        # This would require tracking which transactions belong to which job
        # For now, we just return a warning
        logger.warning("Transaction deletion not implemented for bulk imports")

    # Delete batches
    db.query(BulkImportBatch).filter(BulkImportBatch.job_id == job.id).delete()

    # Delete job
    db.delete(job)
    db.commit()

    return {
        "deleted": True,
        "job_id": job_id,
        "transactions_deleted": txn_count
    }


@router.post("/{job_id}/retry-failed-batches")
async def retry_failed_batches(
    job_id: str,
    background_tasks: BackgroundTasks,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Retry failed batches in a completed-with-errors job.
    """
    from app.models.bulk_import import BulkImportJob, BulkImportBatch, BatchStatus

    job = db.query(BulkImportJob).filter(BulkImportJob.job_id == job_id).first()
    if not job:
        raise HTTPException(status_code=404, detail="Job not found")

    if job.status != BulkImportStatus.COMPLETED_WITH_ERRORS:
        raise HTTPException(
            status_code=400,
            detail=f"Can only retry failed batches for jobs with status 'completed_with_errors', got '{job.status.value}'"
        )

    # Mark failed batches as pending for retry
    failed_count = db.query(BulkImportBatch).filter(
        BulkImportBatch.job_id == job.id,
        BulkImportBatch.status == BatchStatus.FAILED
    ).update({
        'status': BatchStatus.PENDING,
        'error_message': None
    })

    if failed_count == 0:
        return {
            "job_id": job_id,
            "message": "No failed batches to retry"
        }

    # Reset job status
    job.status = BulkImportStatus.PENDING
    job.status_message = f"Retrying {failed_count} failed batches"
    db.commit()

    # Queue for processing
    background_tasks.add_task(process_job_background, job_id)

    return {
        "job_id": job_id,
        "batches_to_retry": failed_count,
        "message": "Failed batches queued for retry"
    }
