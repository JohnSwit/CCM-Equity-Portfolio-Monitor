from fastapi import APIRouter, Depends, UploadFile, File, Query, HTTPException
from sqlalchemy.orm import Session
from typing import List
from app.core.database import get_db
from app.api.auth import get_current_user
from app.models import User, ImportLog, Transaction, AccountInception, Account
from app.models.schemas import ImportPreviewResponse, ImportCommitResponse
from app.services.bd_parser import BDParser, calculate_file_hash
from app.services.inception_parser import InceptionParser, get_accounts_with_inception

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
async def delete_import(
    import_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Delete an import and all its transactions.
    Clears analytics for affected accounts but does NOT recompute.
    Use the analytics endpoints to rebuild if needed.
    WARNING: This will delete all transactions from this import.
    """
    # Find the import
    import_log = db.query(ImportLog).filter(ImportLog.id == import_id).first()
    if not import_log:
        raise HTTPException(status_code=404, detail="Import not found")

    # Get affected account IDs before deletion
    affected_account_ids = set([
        t.account_id for t in db.query(Transaction.account_id).filter(
            Transaction.import_log_id == import_id
        ).distinct().all()
    ])

    # Count transactions to delete
    txn_count = db.query(Transaction).filter(
        Transaction.import_log_id == import_id
    ).count()

    file_name = import_log.file_name

    # Delete all transactions from this import
    db.query(Transaction).filter(
        Transaction.import_log_id == import_id
    ).delete()

    # Delete the import log
    db.delete(import_log)
    db.commit()

    # Clear analytics for all affected accounts (each call commits automatically)
    from app.workers.jobs import clear_analytics_for_account
    for account_id in affected_account_ids:
        clear_analytics_for_account(db, account_id)

    return {
        'deleted': True,
        'import_id': import_id,
        'transactions_deleted': txn_count,
        'accounts_affected': len(affected_account_ids),
        'message': f'Deleted import {file_name} and {txn_count} transactions. Analytics cleared for {len(affected_account_ids)} accounts.'
    }


# ==================== HISTORICAL INCEPTION ENDPOINTS ====================

@router.post("/inception")
async def import_inception_positions(
    file: UploadFile = File(...),
    mode: str = Query("preview", regex="^(preview|commit)$"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Import historical portfolio inception positions CSV.
    This establishes a starting point for portfolio tracking from a past date.

    Expected CSV columns:
    - Account Number
    - Account Display Name
    - Class (asset class)
    - Asset Name
    - Symbol
    - Units (shares)
    - Price
    - Market Value
    - Inception Date (e.g., 12/31/2020)

    - mode=preview: Parse and show preview with validation
    - mode=commit: Import the inception positions
    """
    file_content = await file.read()
    parser = InceptionParser(db)

    if mode == "preview":
        result = parser.parse_csv(file_content, preview=True)

        if result.get('error'):
            raise HTTPException(status_code=400, detail=result['error'])

        return {
            'mode': 'preview',
            'total_rows': result.get('total_rows', 0),
            'inception_date': result.get('inception_date'),
            'accounts_summary': result.get('accounts_summary', []),
            'preview_rows': result.get('preview_rows', []),
            'has_errors': result.get('has_errors', False),
            'multiple_dates_warning': result.get('multiple_dates_warning', False)
        }

    else:  # commit
        result = parser.parse_csv(file_content, preview=False)

        if result.get('error'):
            raise HTTPException(status_code=400, detail=result['error'])

        df = result['dataframe']

        # Commit inception positions
        commit_result = parser.commit_inception(df, user_id=current_user.id)

        if commit_result.get('error'):
            raise HTTPException(status_code=400, detail=commit_result['error'])

        # Create PositionsEOD records for all affected accounts
        from app.models import AccountInception
        inceptions = db.query(AccountInception).filter(
            AccountInception.import_log_id == commit_result.get('import_log_id')
        ).all()

        for inception in inceptions:
            parser.create_inception_positions_eod(inception.account_id)

        return {
            'mode': 'commit',
            'success': True,
            'inception_date': commit_result.get('inception_date'),
            'accounts_created': commit_result.get('accounts_created', 0),
            'accounts_updated': commit_result.get('accounts_updated', 0),
            'securities_created': commit_result.get('securities_created', 0),
            'positions_created': commit_result.get('positions_created', 0),
            'total_value': commit_result.get('total_value', 0),
            'import_log_id': commit_result.get('import_log_id'),
            'errors': commit_result.get('errors', []),
            'message': f"Successfully imported {commit_result.get('positions_created', 0)} inception positions across {commit_result.get('accounts_created', 0) + commit_result.get('accounts_updated', 0)} accounts"
        }


@router.get("/inception")
def get_inception_data(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get all accounts with inception data.
    Returns summary of inception positions for each account.
    """
    return {
        'accounts': get_accounts_with_inception(db)
    }


@router.get("/inception/{account_id}")
def get_account_inception(
    account_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get inception details for a specific account"""
    inception = db.query(AccountInception).filter(
        AccountInception.account_id == account_id
    ).first()

    if not inception:
        raise HTTPException(status_code=404, detail="No inception data found for this account")

    return {
        'account_id': account_id,
        'account_number': inception.account.account_number,
        'display_name': inception.account.display_name,
        'inception_date': inception.inception_date.isoformat(),
        'total_value': inception.total_value,
        'positions': [
            {
                'security_id': pos.security_id,
                'symbol': pos.security.symbol,
                'asset_name': pos.security.asset_name,
                'shares': pos.shares,
                'price': pos.price,
                'market_value': pos.market_value
            }
            for pos in inception.positions
        ]
    }


@router.delete("/inception/{account_id}")
async def delete_account_inception(
    account_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Delete inception data for a specific account.
    This will reset the account to only use transaction-based position tracking.
    """
    inception = db.query(AccountInception).filter(
        AccountInception.account_id == account_id
    ).first()

    if not inception:
        raise HTTPException(status_code=404, detail="No inception data found for this account")

    account_number = inception.account.account_number
    inception_date = inception.inception_date
    position_count = len(inception.positions)

    # Delete the inception (cascade deletes positions)
    db.delete(inception)
    db.commit()

    # Clear analytics for the account
    from app.workers.jobs import clear_analytics_for_account
    clear_analytics_for_account(db, account_id)

    return {
        'deleted': True,
        'account_id': account_id,
        'account_number': account_number,
        'inception_date': inception_date.isoformat(),
        'positions_deleted': position_count,
        'message': f'Deleted inception data for account {account_number}. Analytics cleared.'
    }
