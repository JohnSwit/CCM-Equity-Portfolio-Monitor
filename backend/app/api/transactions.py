from fastapi import APIRouter, Depends, HTTPException, Query, Body
from sqlalchemy.orm import Session
from sqlalchemy import and_, func
from typing import Optional, List
from datetime import date
from pydantic import BaseModel
from app.core.database import get_db
from app.api.auth import get_current_user
from app.models import User, Transaction, Account, Security, ImportLog
from app.workers.jobs import recompute_analytics_job
import logging

router = APIRouter(prefix="/transactions", tags=["transactions"])
logger = logging.getLogger(__name__)


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
    query = db.query(Transaction).join(Account).join(Security)

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
    accounts = db.query(
        Account.id,
        Account.account_number,
        Account.display_name,
        func.count(Transaction.id).label('transaction_count')
    ).outerjoin(Transaction).group_by(
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


@router.delete("/{transaction_id}")
async def delete_transaction(
    transaction_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Delete a single transaction and recompute analytics.
    """
    transaction = db.query(Transaction).filter(Transaction.id == transaction_id).first()
    if not transaction:
        raise HTTPException(status_code=404, detail="Transaction not found")

    # Get details before deletion for response
    account_number = transaction.account.account_number
    symbol = transaction.security.symbol
    trade_date = transaction.trade_date

    # Delete transaction
    db.delete(transaction)
    db.commit()

    # Recompute analytics
    try:
        await recompute_analytics_job(db)
    except Exception as e:
        logger.error(f"Failed to recompute analytics after transaction deletion: {e}")

    return {
        'deleted': True,
        'transaction_id': transaction_id,
        'account_number': account_number,
        'symbol': symbol,
        'trade_date': trade_date,
        'message': f'Deleted transaction for {symbol} in account {account_number}. Analytics have been recomputed.'
    }


@router.delete("/accounts/{account_id}/all")
async def delete_all_account_transactions(
    account_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Delete all transactions for a specific account and recompute analytics.
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

    # Delete all transactions
    db.query(Transaction).filter(Transaction.account_id == account_id).delete()
    db.commit()

    # Recompute analytics
    try:
        await recompute_analytics_job(db)
    except Exception as e:
        logger.error(f"Failed to recompute analytics after account transactions deletion: {e}")

    return {
        'deleted': True,
        'account_id': account_id,
        'account_number': account.account_number,
        'account_name': account.display_name,
        'transactions_deleted': txn_count,
        'message': f'Deleted {txn_count} transactions for account {account.account_number}. Analytics have been recomputed.'
    }


@router.delete("/bulk")
async def delete_transactions_bulk(
    request: BulkDeleteRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Delete multiple transactions at once and recompute analytics.
    """
    transaction_ids = request.transaction_ids
    if not transaction_ids:
        raise HTTPException(status_code=400, detail="No transaction IDs provided")

    # Verify transactions exist
    transactions = db.query(Transaction).filter(Transaction.id.in_(transaction_ids)).all()
    found_ids = {t.id for t in transactions}
    missing_ids = set(transaction_ids) - found_ids

    if missing_ids:
        logger.warning(f"Some transaction IDs not found: {missing_ids}")

    if not transactions:
        raise HTTPException(status_code=404, detail="No transactions found with provided IDs")

    # Delete transactions
    deleted_count = db.query(Transaction).filter(Transaction.id.in_(transaction_ids)).delete(synchronize_session=False)
    db.commit()

    # Recompute analytics
    try:
        await recompute_analytics_job(db)
    except Exception as e:
        logger.error(f"Failed to recompute analytics after bulk deletion: {e}")

    return {
        'deleted': True,
        'transactions_deleted': deleted_count,
        'message': f'Deleted {deleted_count} transactions. Analytics have been recomputed.'
    }
