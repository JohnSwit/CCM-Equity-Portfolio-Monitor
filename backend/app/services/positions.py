import pandas as pd
from typing import List, Dict, Optional
from datetime import date, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_, func, desc
from app.models import (
    Transaction, PositionsEOD, PricesEOD, Security,
    TransactionType, Account, AccountInception, InceptionPosition
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
        """
        Build daily positions for an account.

        If the account has inception data, uses inception positions as the starting
        point and only processes transactions after the inception date.
        """
        if not end_date:
            end_date = date.today()

        # Check for inception data
        inception = self.db.query(AccountInception).filter(
            AccountInception.account_id == account_id
        ).first()

        inception_positions = {}  # security_id -> shares at inception
        effective_start_date = start_date

        if inception:
            # Load inception positions as starting point
            for pos in inception.positions:
                inception_positions[pos.security_id] = pos.shares

            # Start from the day after inception for transactions
            # (inception date already has positions set)
            if not start_date or start_date < inception.inception_date:
                effective_start_date = inception.inception_date + timedelta(days=1)

            logger.info(f"Account {account_id} has inception data from {inception.inception_date} with {len(inception_positions)} positions")

        if not effective_start_date:
            # No inception - get first transaction date
            first_txn = self.db.query(Transaction).filter(
                Transaction.account_id == account_id
            ).order_by(Transaction.trade_date).first()

            if not first_txn:
                # No transactions and no inception - nothing to build
                if inception:
                    # But we have inception, so ensure inception positions exist
                    return self._ensure_inception_positions_eod(account_id, inception)
                return 0

            effective_start_date = first_txn.trade_date

        # Get all transactions for this account from start date
        transactions = self.db.query(Transaction).filter(
            and_(
                Transaction.account_id == account_id,
                Transaction.trade_date.isnot(None),
                Transaction.trade_date >= effective_start_date,
                Transaction.trade_date <= end_date,
                Transaction.security_id.isnot(None)
            )
        ).order_by(Transaction.trade_date, Transaction.id).all()

        # If no transactions and no inception, nothing to build
        if not transactions and not inception:
            return 0

        # Note: Even if no transactions, we continue if we have inception data
        # The inception positions will be forward-filled to all trading dates

        # Group by security and build cumulative positions
        security_positions = {}

        # Initialize with inception positions (these are carried forward)
        for security_id, shares in inception_positions.items():
            security_positions[security_id] = []

        for txn in transactions:
            security_id = txn.security_id
            if security_id not in security_positions:
                security_positions[security_id] = []

            delta = self.get_transaction_unit_delta(txn.transaction_type, txn.units or 0)

            security_positions[security_id].append({
                'date': txn.trade_date,
                'delta': delta
            })

        # Determine calendar start date
        calendar_start = effective_start_date
        if inception:
            # Include inception date in calendar
            calendar_start = inception.inception_date

        # Get trading calendar from prices
        trading_dates = self._get_trading_calendar(calendar_start, end_date)

        # Ensure we have at least some trading dates
        if not trading_dates:
            logger.warning(f"No trading dates found for account {account_id} from {calendar_start} to {end_date}")
            # Fallback: use end_date at minimum so positions exist for today
            trading_dates = [end_date]

        logger.info(f"Building positions for account {account_id}: {len(inception_positions)} inception securities, {len(trading_dates)} trading dates")

        # Build EOD positions for each security
        positions_created = 0

        for security_id, txn_list in security_positions.items():
            # Get starting shares (from inception if available)
            starting_shares = inception_positions.get(security_id, 0.0)

            if txn_list:
                # Convert to DataFrame
                df = pd.DataFrame(txn_list)
                df = df.groupby('date')['delta'].sum().reset_index()
                df = df.sort_values('date')

                # Compute cumulative shares starting from inception
                df['shares'] = starting_shares + df['delta'].cumsum()

                # Create date index covering all trading dates
                date_index = pd.DataFrame({'date': trading_dates})
                df = date_index.merge(df[['date', 'shares']], on='date', how='left')

                # Set inception date shares if we have inception
                if inception and trading_dates and inception.inception_date in trading_dates:
                    inception_idx = df[df['date'] == inception.inception_date].index
                    if len(inception_idx) > 0:
                        df.loc[inception_idx[0], 'shares'] = starting_shares

                # Forward fill shares from inception value
                df['shares'] = df['shares'].ffill()

                # Fill any remaining NaN before inception date with 0
                df['shares'] = df['shares'].fillna(0 if not starting_shares else starting_shares)

            else:
                # No transactions for this security, just inception position
                date_index = pd.DataFrame({'date': trading_dates})
                date_index['shares'] = starting_shares
                df = date_index

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

    def _ensure_inception_positions_eod(self, account_id: int, inception: AccountInception) -> int:
        """
        Ensure PositionsEOD records exist for inception positions.
        Creates positions for ALL trading dates from inception to today.
        """
        end_date = date.today()
        trading_dates = self._get_trading_calendar(inception.inception_date, end_date)

        if not trading_dates:
            # Fallback: at minimum create for inception date and today
            trading_dates = [inception.inception_date, end_date]

        created = 0
        for pos in inception.positions:
            if pos.shares <= 0:
                continue

            for trade_date in trading_dates:
                existing = self.db.query(PositionsEOD).filter(
                    and_(
                        PositionsEOD.account_id == account_id,
                        PositionsEOD.security_id == pos.security_id,
                        PositionsEOD.date == trade_date
                    )
                ).first()

                if not existing:
                    position = PositionsEOD(
                        account_id=account_id,
                        security_id=pos.security_id,
                        date=trade_date,
                        shares=pos.shares
                    )
                    self.db.add(position)
                    created += 1

        if created > 0:
            self.db.commit()
            logger.info(f"Created {created} positions for account {account_id} from inception data")
        return created

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

        # If no prices yet, use business days (weekdays) instead of all calendar days.
        # This is more correct and avoids creating positions for weekends/holidays.
        if not trading_dates:
            all_dates = pd.bdate_range(start=start_date, end=end_date)
            trading_dates = [d.date() for d in all_dates]
        else:
            # Always ensure end_date is included so positions exist for "today"
            # even if today's prices haven't been fetched yet
            if end_date not in trading_dates:
                trading_dates.append(end_date)

        return trading_dates

    def get_average_costs(
        self,
        account_id: int,
        security_ids: List[int],
        as_of_date: date
    ) -> Dict[int, float]:
        """
        Compute weighted average cost per share for securities in an account.
        Uses all BUY and TRANSFER_IN transactions up to as_of_date.
        """
        if not security_ids:
            return {}

        # Get all buy-type transactions for these securities
        transactions = self.db.query(Transaction).filter(
            and_(
                Transaction.account_id == account_id,
                Transaction.security_id.in_(security_ids),
                Transaction.trade_date <= as_of_date,
                Transaction.transaction_type.in_([TransactionType.BUY, TransactionType.TRANSFER_IN]),
                Transaction.units.isnot(None),
                Transaction.units > 0
            )
        ).all()

        # Group by security and compute weighted average cost
        security_costs: Dict[int, Dict] = {}
        for txn in transactions:
            if txn.security_id not in security_costs:
                security_costs[txn.security_id] = {'total_cost': 0.0, 'total_units': 0.0}

            # Use price * units if available, otherwise market_value
            if txn.price is not None and txn.units:
                cost = txn.price * txn.units
            elif txn.market_value is not None:
                cost = abs(txn.market_value)
            else:
                continue

            security_costs[txn.security_id]['total_cost'] += cost
            security_costs[txn.security_id]['total_units'] += txn.units

        # Compute average cost per share
        avg_costs = {}
        for sec_id, data in security_costs.items():
            if data['total_units'] > 0:
                avg_costs[sec_id] = data['total_cost'] / data['total_units']

        return avg_costs

    def get_previous_trading_date(self, as_of_date: date) -> Optional[date]:
        """Get the previous trading date from available prices"""
        prev_date = self.db.query(PricesEOD.date).filter(
            PricesEOD.date < as_of_date
        ).order_by(PricesEOD.date.desc()).first()

        return prev_date[0] if prev_date else None

    def get_holdings_as_of(
        self,
        account_id: int,
        as_of_date: date,
        include_cost_basis: bool = True,
        include_1d_gain: bool = True
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

        if not positions:
            return []

        # Get security IDs for cost and previous price lookups
        security_ids = [pos.security_id for pos, _, _ in positions]

        # Get average costs if requested
        avg_costs = {}
        if include_cost_basis:
            avg_costs = self.get_average_costs(account_id, security_ids, as_of_date)

        # Get previous day prices for 1D gain if requested
        prev_prices = {}
        if include_1d_gain:
            prev_date = self.get_previous_trading_date(as_of_date)
            if prev_date:
                prev_price_rows = self.db.query(
                    PricesEOD.security_id,
                    PricesEOD.close
                ).filter(
                    and_(
                        PricesEOD.security_id.in_(security_ids),
                        PricesEOD.date == prev_date
                    )
                ).all()
                prev_prices = {row.security_id: row.close for row in prev_price_rows}

        holdings = []
        total_value = 0

        for position, security, price in positions:
            market_value = 0
            if price is not None:
                market_value = position.shares * price

            holding = {
                'symbol': security.symbol,
                'asset_name': security.asset_name,
                'asset_class': security.asset_class.value,
                'shares': position.shares,
                'price': price,
                'market_value': market_value,
                'has_price': price is not None,
                'security_id': security.id,
            }

            # Add average cost and unrealized gain
            avg_cost = avg_costs.get(security.id)
            if avg_cost is not None and price is not None:
                holding['avg_cost'] = avg_cost
                holding['unr_gain'] = (price - avg_cost) * position.shares
                holding['unr_gain_pct'] = (price - avg_cost) / avg_cost if avg_cost > 0 else 0
            else:
                holding['avg_cost'] = None
                holding['unr_gain'] = None
                holding['unr_gain_pct'] = None

            # Add 1D gain
            prev_price = prev_prices.get(security.id)
            if prev_price is not None and price is not None:
                holding['gain_1d'] = (price - prev_price) * position.shares
                holding['gain_1d_pct'] = (price - prev_price) / prev_price if prev_price > 0 else 0
            else:
                holding['gain_1d'] = None
                holding['gain_1d_pct'] = None

            holdings.append(holding)
            total_value += market_value

        # Add weights
        for holding in holdings:
            holding['weight'] = holding['market_value'] / total_value if total_value > 0 else 0

        return holdings

    def get_unpriced_securities(self, as_of_date: Optional[date] = None) -> List[Dict]:
        """Get securities with positions but no prices on the given date.

        If no as_of_date is provided, uses the last date that has prices in PricesEOD
        rather than today. This prevents showing all securities as "unpriced" when
        today's market data hasn't been fetched yet (e.g., during market hours or
        if the data provider hasn't updated).
        """
        if not as_of_date:
            # Use the last date with actual price data, not today
            latest_price_date = self.db.query(func.max(PricesEOD.date)).scalar()
            as_of_date = latest_price_date if latest_price_date else date.today()

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
