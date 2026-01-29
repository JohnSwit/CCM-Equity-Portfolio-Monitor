import pandas as pd
from typing import List, Dict, Optional
from datetime import date, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_, func
from app.models import (
    Transaction, PositionsEOD, PricesEOD, Security,
    TransactionType, Account
)
import logging

logger = logging.getLogger(__name__)


class PositionsEngine:
    """Builds daily positions from transactions"""

    def __init__(self, db: Session):
        self.db = db

    def get_transaction_unit_delta(self, txn_type: TransactionType, units: float) -> float:
        """Get share delta for a transaction type"""
        if txn_type == TransactionType.BUY or txn_type == TransactionType.TRANSFER_IN:
            return units
        elif txn_type == TransactionType.SELL or txn_type == TransactionType.TRANSFER_OUT:
            return -units
        else:
            return 0.0

    def build_positions_for_account(
        self,
        account_id: int,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> int:
        """Build daily positions for an account"""
        if not end_date:
            end_date = date.today()

        if not start_date:
            # Get first transaction date for this account
            first_txn = self.db.query(Transaction).filter(
                Transaction.account_id == account_id
            ).order_by(Transaction.trade_date).first()

            if not first_txn:
                return 0

            start_date = first_txn.trade_date

        # Get all transactions for this account
        transactions = self.db.query(Transaction).filter(
            and_(
                Transaction.account_id == account_id,
                Transaction.trade_date.isnot(None),
                Transaction.trade_date >= start_date,
                Transaction.trade_date <= end_date,
                Transaction.security_id.isnot(None)
            )
        ).order_by(Transaction.trade_date, Transaction.id).all()

        if not transactions:
            return 0

        # Group by security and build cumulative positions
        security_positions = {}

        for txn in transactions:
            security_id = txn.security_id
            if security_id not in security_positions:
                security_positions[security_id] = []

            delta = self.get_transaction_unit_delta(txn.transaction_type, txn.units or 0)

            security_positions[security_id].append({
                'date': txn.trade_date,
                'delta': delta
            })

        # Get trading calendar from prices
        trading_dates = self._get_trading_calendar(start_date, end_date)

        # Build EOD positions for each security
        positions_created = 0

        for security_id, txn_list in security_positions.items():
            # Convert to DataFrame
            df = pd.DataFrame(txn_list)
            df = df.groupby('date')['delta'].sum().reset_index()
            df = df.sort_values('date')

            # Compute cumulative shares
            df['shares'] = df['delta'].cumsum()

            # Create date index covering all trading dates
            date_index = pd.DataFrame({'date': trading_dates})
            df = date_index.merge(df[['date', 'shares']], on='date', how='left')

            # Forward fill shares
            df['shares'] = df['shares'].ffill().fillna(0)

            # Store positions
            for _, row in df.iterrows():
                if row['shares'] == 0:
                    continue  # Don't store zero positions

                existing = self.db.query(PositionsEOD).filter(
                    and_(
                        PositionsEOD.account_id == account_id,
                        PositionsEOD.security_id == security_id,
                        PositionsEOD.date == row['date']
                    )
                ).first()

                if existing:
                    if existing.shares != row['shares']:
                        existing.shares = row['shares']
                else:
                    position = PositionsEOD(
                        account_id=account_id,
                        security_id=security_id,
                        date=row['date'],
                        shares=row['shares']
                    )
                    self.db.add(position)
                    positions_created += 1

        self.db.commit()
        logger.info(f"Created {positions_created} positions for account {account_id}")
        return positions_created

    def build_positions_for_all_accounts(self) -> Dict[str, int]:
        """Build positions for all accounts"""
        accounts = self.db.query(Account).all()

        results = {
            'total_accounts': len(accounts),
            'updated': 0,
            'failed': 0,
            'total_positions': 0
        }

        for account in accounts:
            try:
                count = self.build_positions_for_account(account.id)
                results['total_positions'] += count
                results['updated'] += 1
            except Exception as e:
                logger.error(f"Failed to build positions for account {account.id}: {e}")
                results['failed'] += 1

        return results

    def _get_trading_calendar(self, start_date: date, end_date: date) -> List[date]:
        """Get trading calendar from available price dates"""
        # Get unique dates from prices_eod
        dates = self.db.query(PricesEOD.date).filter(
            and_(
                PricesEOD.date >= start_date,
                PricesEOD.date <= end_date
            )
        ).distinct().order_by(PricesEOD.date).all()

        trading_dates = [d[0] for d in dates]

        # If no prices yet, use all dates (will be filtered later)
        if not trading_dates:
            all_dates = pd.date_range(start=start_date, end=end_date, freq='D')
            trading_dates = [d.date() for d in all_dates]

        return trading_dates

    def get_holdings_as_of(
        self,
        account_id: int,
        as_of_date: date
    ) -> List[Dict]:
        """Get holdings for an account as of a date"""
        positions = self.db.query(
            PositionsEOD,
            Security,
            PricesEOD.close
        ).join(
            Security, PositionsEOD.security_id == Security.id
        ).outerjoin(
            PricesEOD,
            and_(
                PricesEOD.security_id == PositionsEOD.security_id,
                PricesEOD.date == as_of_date
            )
        ).filter(
            and_(
                PositionsEOD.account_id == account_id,
                PositionsEOD.date == as_of_date,
                PositionsEOD.shares > 0
            )
        ).all()

        holdings = []
        total_value = 0

        for position, security, price in positions:
            market_value = 0
            if price is not None:
                market_value = position.shares * price

            holdings.append({
                'symbol': security.symbol,
                'asset_name': security.asset_name,
                'asset_class': security.asset_class.value,
                'shares': position.shares,
                'price': price,
                'market_value': market_value,
                'has_price': price is not None
            })

            total_value += market_value

        # Add weights
        for holding in holdings:
            holding['weight'] = holding['market_value'] / total_value if total_value > 0 else 0

        return holdings

    def get_unpriced_securities(self, as_of_date: Optional[date] = None) -> List[Dict]:
        """Get securities with positions but no prices"""
        if not as_of_date:
            as_of_date = date.today()

        # Get securities with positions but no price on as_of_date
        subq = self.db.query(PositionsEOD.security_id).filter(
            PositionsEOD.date == as_of_date
        ).distinct().subquery()

        unpriced = self.db.query(
            Security,
            func.max(PositionsEOD.date).label('last_seen')
        ).join(
            PositionsEOD, Security.id == PositionsEOD.security_id
        ).outerjoin(
            PricesEOD,
            and_(
                PricesEOD.security_id == Security.id,
                PricesEOD.date == as_of_date
            )
        ).filter(
            and_(
                Security.id.in_(subq),
                PricesEOD.id.is_(None)
            )
        ).group_by(Security.id).all()

        result = []
        for security, last_seen in unpriced:
            result.append({
                'symbol': security.symbol,
                'asset_name': security.asset_name,
                'asset_class': security.asset_class.value,
                'is_option': security.is_option,
                'last_seen_date': last_seen
            })

        return result
