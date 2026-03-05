from fastapi import APIRouter, Depends, HTTPException, Query, Body
from sqlalchemy.orm import Session, joinedload
from sqlalchemy import and_, func
from typing import Optional, List
from datetime import date
from pydantic import BaseModel
from app.core.database import get_db
from app.api.auth import get_current_user
from app.models import User, Transaction, Account, Security, ImportLog, TaxLot, RealizedGain, WashSaleViolation, AccountInception, GroupMember, PositionsEOD, PortfolioValueEOD, ViewType
from app.models.bulk_import import ImportedTransaction
from app.workers.jobs import clear_analytics_for_account
import logging

router = APIRouter(prefix="/transactions", tags=["transactions"])
logger = logging.getLogger(__name__)


def _nullify_transaction_references(db: Session, transaction_ids: List[int]):
    """
    Nullify ALL FK references pointing to the given transaction IDs,
    so the transactions can be deleted without violating foreign key constraints.

    Tables with FK to transactions.id:
    - TaxLot.purchase_transaction_id
    - RealizedGain.sale_transaction_id
    - WashSaleViolation.loss_sale_transaction_id
    - WashSaleViolation.replacement_transaction_id
    - ImportedTransaction.final_transaction_id (bulk import staging)
    """
    if not transaction_ids:
        return

    id_set = set(transaction_ids)

    # TaxLot.purchase_transaction_id
    db.query(TaxLot).filter(
        TaxLot.purchase_transaction_id.in_(id_set)
    ).update({TaxLot.purchase_transaction_id: None}, synchronize_session=False)

    # RealizedGain.sale_transaction_id
    db.query(RealizedGain).filter(
        RealizedGain.sale_transaction_id.in_(id_set)
    ).update({RealizedGain.sale_transaction_id: None}, synchronize_session=False)

    # WashSaleViolation.loss_sale_transaction_id
    db.query(WashSaleViolation).filter(
        WashSaleViolation.loss_sale_transaction_id.in_(id_set)
    ).update({WashSaleViolation.loss_sale_transaction_id: None}, synchronize_session=False)

    # WashSaleViolation.replacement_transaction_id
    db.query(WashSaleViolation).filter(
        WashSaleViolation.replacement_transaction_id.in_(id_set)
    ).update({WashSaleViolation.replacement_transaction_id: None}, synchronize_session=False)

    # ImportedTransaction.final_transaction_id (bulk import staging)
    db.query(ImportedTransaction).filter(
        ImportedTransaction.final_transaction_id.in_(id_set)
    ).update({ImportedTransaction.final_transaction_id: None}, synchronize_session=False)


def cleanup_orphaned_accounts(db: Session) -> int:
    """
    Delete accounts that have NO remaining data:
    no transactions, no positions, no inception data, and no imported tax lots.
    Also removes associated GroupMember entries.
    Returns the number of accounts deleted.
    """
    all_account_ids = {r[0] for r in db.query(Account.id).all()}
    if not all_account_ids:
        return 0

    # Accounts that still have data — keep these
    has_transactions = {r[0] for r in db.query(Transaction.account_id).distinct().all()}
    has_positions = {r[0] for r in db.query(PositionsEOD.account_id).distinct().all()}
    has_inception = {r[0] for r in db.query(AccountInception.account_id).all()}
    has_tax_lots = {r[0] for r in db.query(TaxLot.account_id).filter(
        TaxLot.import_log_id.isnot(None)  # Only imported lots count as "data"
    ).distinct().all()}

    keep = has_transactions | has_positions | has_inception | has_tax_lots
    orphaned = all_account_ids - keep

    if not orphaned:
        return 0

    # Remove GroupMember references for orphaned accounts
    db.query(GroupMember).filter(
        GroupMember.member_type == 'account',
        GroupMember.member_id.in_(orphaned)
    ).delete(synchronize_session=False)

    # Delete the orphaned accounts
    deleted = db.query(Account).filter(Account.id.in_(orphaned)).delete(synchronize_session=False)
    db.commit()

    logger.info(f"Cleaned up {deleted} orphaned accounts")
    return deleted


class BulkDeleteRequest(BaseModel):
    transaction_ids: List[int]


@router.get("/")
def get_transactions(
    account_id: Optional[int] = None,
    account_number: Optional[str] = None,
    symbol: Optional[str] = None,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    limit: int = Query(1000, ge=1, le=50000),
    offset: int = Query(0, ge=0),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get all transactions with optional filtering.
    Returns transactions ordered by trade date (most recent first).
    """
    query = db.query(Transaction).join(Account).join(Security).options(
        joinedload(Transaction.account),
        joinedload(Transaction.security)
    )

    # Apply filters
    if account_id:
        query = query.filter(Transaction.account_id == account_id)

    if account_number:
        query = query.filter(Account.account_number == account_number)

    if symbol:
        query = query.filter(Security.symbol == symbol)

    if start_date:
        query = query.filter(Transaction.trade_date >= start_date)

    if end_date:
        query = query.filter(Transaction.trade_date <= end_date)

    # Get total count before pagination
    total_count = query.count()

    # Order and paginate
    transactions = query.order_by(
        Transaction.trade_date.desc(),
        Transaction.id.desc()
    ).limit(limit).offset(offset).all()

    return {
        'total_count': total_count,
        'limit': limit,
        'offset': offset,
        'transactions': [
            {
                'id': t.id,
                'account_id': t.account_id,
                'account_number': t.account.account_number,
                'account_name': t.account.display_name,
                'security_id': t.security_id,
                'symbol': t.security.symbol,
                'asset_name': t.security.asset_name,
                'asset_class': t.security.asset_class.value,
                'trade_date': t.trade_date,
                'transaction_type': t.transaction_type.value,
                'quantity': float(t.units) if t.units else 0.0,
                'price': float(t.price) if t.price else None,
                'amount': float(t.market_value) if t.market_value else None,
                'import_log_id': t.import_log_id,
                'created_at': t.created_at
            }
            for t in transactions
        ]
    }


@router.get("/accounts")
def get_accounts_with_transaction_counts(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get all accounts with their transaction counts.
    Useful for showing which accounts have data.
    """
    # Only return accounts that have portfolio values
    accounts_with_portfolio = db.query(PortfolioValueEOD.view_id).filter(
        PortfolioValueEOD.view_type == ViewType.ACCOUNT
    ).distinct().subquery()

    accounts = db.query(
        Account.id,
        Account.account_number,
        Account.display_name,
        func.count(Transaction.id).label('transaction_count')
    ).join(Transaction).filter(
        Account.id.in_(db.query(accounts_with_portfolio.c.view_id))
    ).group_by(
        Account.id,
        Account.account_number,
        Account.display_name
    ).order_by(Account.account_number).all()

    return [
        {
            'id': acc.id,
            'account_number': acc.account_number,
            'account_name': acc.display_name,
            'transaction_count': acc.transaction_count
        }
        for acc in accounts
    ]


@router.delete("/all")
async def delete_all_transactions(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Delete ALL transactions across ALL accounts.
    WARNING: This is irreversible and clears all analytics.
    """
    # Count before deletion
    total_count = db.query(func.count(Transaction.id)).scalar() or 0
    if total_count == 0:
        return {
            'deleted': False,
            'transactions_deleted': 0,
            'accounts_affected': 0,
            'message': 'No transactions to delete.'
        }

    # Get all affected account IDs
    affected_account_ids = {
        r[0] for r in db.query(Transaction.account_id).distinct().all()
    }

    # Unconditional FK nullification — no .in_() needed since we're deleting ALL.
    # This avoids PostgreSQL parameter limits with large datasets (60K+ IDs).
    db.query(TaxLot).filter(
        TaxLot.purchase_transaction_id.isnot(None)
    ).update({TaxLot.purchase_transaction_id: None}, synchronize_session=False)

    db.query(RealizedGain).filter(
        RealizedGain.sale_transaction_id.isnot(None)
    ).update({RealizedGain.sale_transaction_id: None}, synchronize_session=False)

    db.query(WashSaleViolation).filter(
        WashSaleViolation.loss_sale_transaction_id.isnot(None)
    ).update({WashSaleViolation.loss_sale_transaction_id: None}, synchronize_session=False)

    db.query(WashSaleViolation).filter(
        WashSaleViolation.replacement_transaction_id.isnot(None)
    ).update({WashSaleViolation.replacement_transaction_id: None}, synchronize_session=False)

    db.query(ImportedTransaction).filter(
        ImportedTransaction.final_transaction_id.isnot(None)
    ).update({ImportedTransaction.final_transaction_id: None}, synchronize_session=False)

    # Clean up transaction-built tax data (built from transactions, now meaningless)
    txn_built_lot_ids = db.query(TaxLot.id).filter(TaxLot.import_log_id.is_(None)).subquery()
    db.query(RealizedGain).filter(
        RealizedGain.tax_lot_id.in_(txn_built_lot_ids)
    ).delete(synchronize_session=False)
    db.query(WashSaleViolation).filter(
        WashSaleViolation.adjusted_lot_id.in_(txn_built_lot_ids)
    ).delete(synchronize_session=False)
    db.query(TaxLot).filter(TaxLot.import_log_id.is_(None)).delete(synchronize_session=False)

    # Delete all transactions
    db.query(Transaction).delete(synchronize_session=False)

    # Delete import logs so re-imports aren't blocked by stale file_hash checks
    db.query(ImportLog).filter(ImportLog.source == 'blackdiamond').delete(synchronize_session=False)

    db.commit()

    # Clear analytics for all affected accounts
    for account_id in affected_account_ids:
        clear_analytics_for_account(db, account_id)

    # Clean up orphaned accounts (no transactions, no positions, no inception, no imported tax lots)
    accounts_deleted = cleanup_orphaned_accounts(db)

    return {
        'deleted': True,
        'transactions_deleted': total_count,
        'accounts_affected': len(affected_account_ids),
        'accounts_deleted': accounts_deleted,
        'message': f'Deleted ALL {total_count} transactions across {len(affected_account_ids)} accounts. '
                   f'{accounts_deleted} orphaned accounts removed. All analytics cleared.'
    }


@router.delete("/bulk")
async def delete_transactions_bulk(
    request: BulkDeleteRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Delete multiple transactions at once and clear associated analytics.
    Does NOT recompute analytics - use the analytics endpoints to rebuild if needed.
    """
    transaction_ids = request.transaction_ids
    if not transaction_ids:
        raise HTTPException(status_code=400, detail="No transaction IDs provided")

    # Verify transactions exist and collect affected accounts
    transactions = db.query(Transaction).filter(Transaction.id.in_(transaction_ids)).all()
    found_ids = {t.id for t in transactions}
    missing_ids = set(transaction_ids) - found_ids

    if missing_ids:
        logger.warning(f"Some transaction IDs not found: {missing_ids}")

    if not transactions:
        raise HTTPException(status_code=404, detail="No transactions found with provided IDs")

    # Get affected account IDs
    affected_account_ids = set(t.account_id for t in transactions)

    # Nullify FK references from tax tables before deleting
    _nullify_transaction_references(db, transaction_ids)

    # Delete transactions
    deleted_count = db.query(Transaction).filter(Transaction.id.in_(transaction_ids)).delete(synchronize_session=False)
    db.commit()

    # Clear analytics for all affected accounts (each call commits automatically)
    for account_id in affected_account_ids:
        clear_analytics_for_account(db, account_id)

    # Clean up orphaned accounts
    accounts_deleted = cleanup_orphaned_accounts(db)

    return {
        'deleted': True,
        'transactions_deleted': deleted_count,
        'accounts_affected': len(affected_account_ids),
        'accounts_deleted': accounts_deleted,
        'message': f'Deleted {deleted_count} transactions. Analytics cleared for {len(affected_account_ids)} accounts. '
                   f'{accounts_deleted} orphaned accounts removed.'
    }


@router.delete("/accounts/{account_id}/all")
async def delete_all_account_transactions(
    account_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Delete all transactions for a specific account and clear associated analytics.
    Does NOT recompute analytics - use the analytics endpoints to rebuild if needed.
    WARNING: This will delete ALL transactions for this account.
    """
    account = db.query(Account).filter(Account.id == account_id).first()
    if not account:
        raise HTTPException(status_code=404, detail="Account not found")

    # Count transactions
    txn_count = db.query(Transaction).filter(Transaction.account_id == account_id).count()

    if txn_count == 0:
        return {
            'deleted': False,
            'account_id': account_id,
            'account_number': account.account_number,
            'transactions_deleted': 0,
            'message': f'No transactions found for account {account.account_number}'
        }

    # Nullify FK references from tax tables before deleting
    txn_ids = [t.id for t in db.query(Transaction.id).filter(Transaction.account_id == account_id).all()]
    _nullify_transaction_references(db, txn_ids)

    # Delete all transactions
    db.query(Transaction).filter(Transaction.account_id == account_id).delete(synchronize_session=False)
    db.commit()

    # Clear analytics for the affected account (this commits automatically)
    clear_analytics_for_account(db, account_id)

    # Clean up orphaned accounts
    accounts_deleted = cleanup_orphaned_accounts(db)

    return {
        'deleted': True,
        'account_id': account_id,
        'account_number': account.account_number,
        'account_name': account.display_name,
        'transactions_deleted': txn_count,
        'account_removed': accounts_deleted > 0,
        'message': f'Deleted {txn_count} transactions for account {account.account_number}. Analytics cleared.'
                   + (f' Account removed (no remaining data).' if accounts_deleted > 0 else '')
    }


@router.delete("/{transaction_id}")
async def delete_transaction(
    transaction_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Delete a single transaction and clear associated analytics.
    Does NOT recompute analytics - use the analytics endpoints to rebuild if needed.
    """
    transaction = db.query(Transaction).filter(Transaction.id == transaction_id).first()
    if not transaction:
        raise HTTPException(status_code=404, detail="Transaction not found")

    # Get details before deletion for response
    account_id = transaction.account_id
    account_number = transaction.account.account_number
    symbol = transaction.security.symbol
    trade_date = transaction.trade_date

    # Nullify FK references from tax tables before deleting
    _nullify_transaction_references(db, [transaction_id])

    # Delete transaction
    db.delete(transaction)
    db.commit()

    # Clear analytics for the affected account (this commits automatically)
    clear_analytics_for_account(db, account_id)

    # Clean up orphaned accounts
    accounts_deleted = cleanup_orphaned_accounts(db)

    return {
        'deleted': True,
        'transaction_id': transaction_id,
        'account_number': account_number,
        'symbol': symbol,
        'trade_date': trade_date,
        'account_removed': accounts_deleted > 0,
        'message': f'Deleted transaction for {symbol} in account {account_number}. Analytics cleared.'
                   + (f' Account removed (no remaining data).' if accounts_deleted > 0 else '')
    }
