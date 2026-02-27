"""
Tax Lot Import and Management API

Provides endpoints for:
- Importing tax lots from CSV files (separate from transaction imports)
- Viewing import history
- Deleting imports
"""
from fastapi import APIRouter, Depends, UploadFile, File, Query, HTTPException
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import date

from app.core.database import get_db
from app.api.auth import get_current_user
from app.models import User, TaxLotImportLog
from app.models.models import TaxLot, RealizedGain
from app.services.tax_lot_parser import TaxLotParser, calculate_file_hash

router = APIRouter(prefix="/tax-lots", tags=["tax-lots"])


@router.post("/import")
async def import_tax_lots(
    file: UploadFile = File(...),
    mode: str = Query("preview", regex="^(preview|commit)$"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Import tax lots from CSV file.

    CSV Format (columns can be in any order):
    - Account Number (required)
    - Account Display Name (optional)
    - Class (optional - asset class)
    - Symbol (required)
    - Asset Name (optional)
    - Open Date (required - purchase date)
    - Unit Cost (required - cost basis per share)
    - Units (required - number of shares)
    - Cost Basis (optional - will be calculated if not provided)
    - Market Value (optional)
    - Short-Term Gain/Loss (optional)
    - Long-Term Gain/Loss (optional)
    - Total Gain Loss (optional)

    Args:
        file: CSV file to import
        mode: "preview" to validate and preview, "commit" to actually import

    Returns:
        Preview data or import result
    """
    # Read file content
    file_content = await file.read()
    file_hash = calculate_file_hash(file_content)

    # Check if already imported (only for commit mode)
    if mode == "commit":
        existing = db.query(TaxLotImportLog).filter(
            TaxLotImportLog.file_hash == file_hash,
            TaxLotImportLog.status.in_(['completed', 'completed_with_errors'])
        ).first()

        if existing:
            raise HTTPException(
                status_code=400,
                detail=f"File already imported (import_log_id: {existing.id})"
            )

    # Parse CSV
    parser = TaxLotParser(db)
    result = parser.parse_csv(file_content, preview=(mode == "preview"))

    if result.get('error'):
        raise HTTPException(status_code=400, detail=result['error'])

    if mode == "preview":
        return {
            'status': 'preview',
            'file_name': file.filename,
            'total_rows': result['total_rows'],
            'valid_rows': result['valid_rows'],
            'error_rows': result['error_rows'],
            'errors': result['errors'],
            'warnings': result['warnings'],
            'preview_data': result['preview']
        }

    # Commit mode - import the data
    import_result = parser.import_tax_lots(
        result['dataframe'],
        file.filename,
        file_hash
    )

    return {
        'status': 'success',
        'message': f"Imported {import_result['imported']} tax lots",
        **import_result
    }


@router.get("/imports")
def get_tax_lot_imports(
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get tax lot import history"""
    imports = db.query(TaxLotImportLog).order_by(
        TaxLotImportLog.created_at.desc()
    ).limit(limit).all()

    return [
        {
            'id': imp.id,
            'file_name': imp.file_name,
            'status': imp.status,
            'rows_processed': imp.rows_processed,
            'rows_imported': imp.rows_imported,
            'rows_skipped': imp.rows_skipped,
            'rows_error': imp.rows_error,
            'created_at': imp.created_at
        }
        for imp in imports
    ]


@router.delete("/imports/{import_id}")
async def delete_tax_lot_import(
    import_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Delete a tax lot import and all tax lots from that import.

    WARNING: This will delete all tax lots from this import.
    """
    # Find the import
    import_log = db.query(TaxLotImportLog).filter(
        TaxLotImportLog.id == import_id
    ).first()

    if not import_log:
        raise HTTPException(status_code=404, detail="Import not found")

    file_name = import_log.file_name

    # Delete all tax lots from this import
    tax_lot_count = db.query(TaxLot).filter(
        TaxLot.import_log_id == import_id
    ).delete(synchronize_session=False)

    # Delete the import log
    db.delete(import_log)
    db.commit()

    return {
        'deleted': True,
        'import_id': import_id,
        'tax_lots_deleted': tax_lot_count,
        'message': f'Deleted import {file_name} and {tax_lot_count} tax lots'
    }


@router.get("/")
def get_tax_lots(
    account_id: Optional[int] = Query(None, description="Filter by account ID"),
    symbol: Optional[str] = Query(None, description="Filter by symbol"),
    include_closed: bool = Query(False, description="Include closed lots"),
    limit: int = Query(100, ge=1, le=1000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get tax lots with optional filters.

    Returns tax lots with account and security details.
    """
    from app.models import Account, Security

    query = db.query(TaxLot).join(Account).join(Security)

    if account_id:
        query = query.filter(TaxLot.account_id == account_id)

    if symbol:
        query = query.filter(Security.symbol == symbol.upper())

    if not include_closed:
        query = query.filter(TaxLot.is_closed == False)

    total = query.count()

    lots = query.order_by(
        TaxLot.purchase_date.desc()
    ).offset(offset).limit(limit).all()

    return {
        'total': total,
        'offset': offset,
        'limit': limit,
        'lots': [
            {
                'id': lot.id,
                'account_id': lot.account_id,
                'account_number': lot.account.account_number,
                'account_name': lot.account.display_name,
                'security_id': lot.security_id,
                'symbol': lot.security.symbol,
                'asset_name': lot.security.asset_name,
                'purchase_date': lot.purchase_date,
                'original_shares': lot.original_shares,
                'remaining_shares': lot.remaining_shares,
                'cost_basis_per_share': lot.cost_basis_per_share,
                'total_cost_basis': lot.total_cost_basis,
                'remaining_cost_basis': lot.remaining_cost_basis,
                'market_value': lot.market_value,
                'short_term_gain_loss': lot.short_term_gain_loss,
                'long_term_gain_loss': lot.long_term_gain_loss,
                'total_gain_loss': lot.total_gain_loss,
                'is_closed': lot.is_closed,
                'import_log_id': lot.import_log_id,
                'created_at': lot.created_at
            }
            for lot in lots
        ]
    }


@router.get("/summary")
def get_tax_lot_summary(
    account_id: Optional[int] = Query(None, description="Filter by account ID"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get summary statistics for tax lots.

    Returns totals for cost basis, market value, and gains/losses.
    """
    from sqlalchemy import func

    query = db.query(
        func.count(TaxLot.id).label('total_lots'),
        func.sum(TaxLot.remaining_shares).label('total_shares'),
        func.sum(TaxLot.remaining_cost_basis).label('total_cost_basis'),
        func.sum(TaxLot.market_value).label('total_market_value'),
        func.sum(TaxLot.short_term_gain_loss).label('total_short_term_gain_loss'),
        func.sum(TaxLot.long_term_gain_loss).label('total_long_term_gain_loss'),
        func.sum(TaxLot.total_gain_loss).label('total_gain_loss')
    ).filter(TaxLot.is_closed == False)

    if account_id:
        query = query.filter(TaxLot.account_id == account_id)

    result = query.first()

    return {
        'total_lots': result.total_lots or 0,
        'total_shares': result.total_shares or 0,
        'total_cost_basis': result.total_cost_basis or 0,
        'total_market_value': result.total_market_value or 0,
        'total_short_term_gain_loss': result.total_short_term_gain_loss or 0,
        'total_long_term_gain_loss': result.total_long_term_gain_loss or 0,
        'total_gain_loss': result.total_gain_loss or 0,
        'unrealized_gain_loss_pct': (
            ((result.total_market_value - result.total_cost_basis) / result.total_cost_basis * 100)
            if result.total_cost_basis and result.total_market_value else 0
        )
    }


@router.delete("/all")
async def delete_all_tax_lots(
    confirm: bool = Query(False, description="Must be true to confirm deletion"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Delete ALL tax lots and import logs.

    WARNING: This is irreversible! Pass confirm=true to execute.
    """
    if not confirm:
        count = db.query(TaxLot).count()
        import_count = db.query(TaxLotImportLog).count()
        return {
            'status': 'preview',
            'message': 'Pass confirm=true to delete all tax lots',
            'would_delete': {
                'tax_lots': count,
                'import_logs': import_count
            }
        }

    # Delete realized gains first (foreign key to tax lots)
    realized_gains_deleted = db.query(RealizedGain).delete(synchronize_session=False)

    # Delete all tax lots
    tax_lots_deleted = db.query(TaxLot).delete(synchronize_session=False)

    # Delete all import logs
    import_logs_deleted = db.query(TaxLotImportLog).delete(synchronize_session=False)

    db.commit()

    return {
        'status': 'success',
        'message': 'All tax lots deleted',
        'deleted': {
            'realized_gains': realized_gains_deleted,
            'tax_lots': tax_lots_deleted,
            'import_logs': import_logs_deleted
        }
    }
