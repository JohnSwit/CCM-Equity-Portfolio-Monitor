from fastapi import APIRouter, Depends, UploadFile, File, Query, HTTPException
from sqlalchemy.orm import Session
from typing import List
from app.core.database import get_db
from app.api.auth import get_current_user
from app.models import User, ImportLog, Transaction
from app.models.schemas import ImportPreviewResponse, ImportCommitResponse
from app.services.bd_parser import BDParser, calculate_file_hash

router = APIRouter(prefix="/imports", tags=["imports"])


@router.post("/blackdiamond/transactions")
async def import_bd_transactions(
    file: UploadFile = File(...),
    mode: str = Query("preview", regex="^(preview|commit)$"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Import Black Diamond transactions CSV.
    - mode=preview: Parse and show preview with errors
    - mode=commit: Actually import the transactions
    """
    # Read file content
    file_content = await file.read()
    file_hash = calculate_file_hash(file_content)

    # Check if already imported
    if mode == "commit":
        existing = db.query(ImportLog).filter(
            ImportLog.file_hash == file_hash,
            ImportLog.status.in_(['completed', 'completed_with_errors'])
        ).first()

        if existing:
            raise HTTPException(
                status_code=400,
                detail=f"File already imported (import_log_id: {existing.id})"
            )

    # Parse CSV
    parser = BDParser(db)

    if mode == "preview":
        result = parser.parse_csv(file_content, preview=True)

        if result.get('error'):
            raise HTTPException(status_code=400, detail=result['error'])

        return ImportPreviewResponse(**result)

    else:  # commit
        result = parser.parse_csv(file_content, preview=False)

        if result.get('error'):
            raise HTTPException(status_code=400, detail=result['error'])

        df = result['dataframe']

        # Import transactions
        import_result = parser.import_transactions(df, file.filename, file_hash)

        return ImportCommitResponse(**import_result)


@router.get("/")
def get_import_history(
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get import history"""
    imports = db.query(ImportLog).order_by(
        ImportLog.created_at.desc()
    ).limit(limit).all()

    return [
        {
            'id': imp.id,
            'source': imp.source,
            'file_name': imp.file_name,
            'status': imp.status,
            'rows_processed': imp.rows_processed,
            'rows_imported': imp.rows_imported,
            'rows_error': imp.rows_error,
            'created_at': imp.created_at
        }
        for imp in imports
    ]


@router.delete("/{import_id}")
def delete_import(
    import_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Delete an import and all its transactions.
    WARNING: This will delete all transactions from this import.
    """
    # Find the import
    import_log = db.query(ImportLog).filter(ImportLog.id == import_id).first()
    if not import_log:
        raise HTTPException(status_code=404, detail="Import not found")

    # Count transactions to delete
    txn_count = db.query(Transaction).filter(
        Transaction.import_log_id == import_id
    ).count()

    # Delete all transactions from this import
    db.query(Transaction).filter(
        Transaction.import_log_id == import_id
    ).delete()

    # Delete the import log
    db.delete(import_log)
    db.commit()

    return {
        'deleted': True,
        'import_id': import_id,
        'transactions_deleted': txn_count,
        'message': f'Deleted import {import_log.file_name} and {txn_count} transactions. Run analytics update to refresh data.'
    }
