import pandas as pd
import numpy as np
from typing import List, Dict, Optional, Tuple
from datetime import date, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_, func, or_
from app.models import (
    PositionsEOD, PricesEOD, PortfolioValueEOD, ReturnsEOD,
    Transaction, ViewType, TransactionType, Security
)
import logging

logger = logging.getLogger(__name__)


class ReturnsEngine:
    """
    Computes equity sleeve returns ignoring cash flows.
    Uses holdings-based approach with start-of-day weights.
    """

    def __init__(self, db: Session):
        self.db = db

    def compute_portfolio_values_for_account(
        self,
        account_id: int,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> int:
        """Compute daily portfolio values for an account"""
        if not end_date:
            end_date = date.today()

        # Get positions
        positions_query = self.db.query(
            PositionsEOD.date,
            PositionsEOD.security_id,
            PositionsEOD.shares
        ).filter(
            PositionsEOD.account_id == account_id
        )

        if start_date:
            positions_query = positions_query.filter(PositionsEOD.date >= start_date)

        positions_query = positions_query.filter(PositionsEOD.date <= end_date)

        positions = positions_query.all()

        if not positions:
            return 0

        # Convert to DataFrame
        pos_df = pd.DataFrame(positions, columns=['date', 'security_id', 'shares'])

        # Get prices
        security_ids = pos_df['security_id'].unique().tolist()
        prices_query = self.db.query(
            PricesEOD.date,
            PricesEOD.security_id,
            PricesEOD.close
        ).filter(
            and_(
                PricesEOD.security_id.in_(security_ids),
                PricesEOD.date <= end_date
            )
        )

        if start_date:
            prices_query = prices_query.filter(PricesEOD.date >= start_date)

        prices = prices_query.all()

        if not prices:
            logger.warning(f"No prices found for account {account_id}")
            return 0

        prices_df = pd.DataFrame(prices, columns=['date', 'security_id', 'close'])

        # Merge positions and prices
        merged = pos_df.merge(prices_df, on=['date', 'security_id'], how='inner')

        # Compute market value per position
        merged['market_value'] = merged['shares'] * merged['close']

        # Aggregate by date
        daily_values = merged.groupby('date')['market_value'].sum().reset_index()
        daily_values.columns = ['date', 'total_value']

        # Store values
        count = 0
        for _, row in daily_values.iterrows():
            existing = self.db.query(PortfolioValueEOD).filter(
                and_(
                    PortfolioValueEOD.view_type == ViewType.ACCOUNT,
                    PortfolioValueEOD.view_id == account_id,
                    PortfolioValueEOD.date == row['date']
                )
            ).first()

            if existing:
                if existing.total_value != row['total_value']:
                    existing.total_value = row['total_value']
            else:
                value = PortfolioValueEOD(
                    view_type=ViewType.ACCOUNT,
                    view_id=account_id,
                    date=row['date'],
                    total_value=row['total_value']
                )
                self.db.add(value)
                count += 1

        self.db.commit()
        logger.info(f"Created {count} portfolio values for account {account_id}")
        return count

    def compute_returns_for_account(
        self,
        account_id: int,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> int:
        """
        Compute returns for an account using equity sleeve logic.
        Returns = (shares_t-1 * price_t) / (shares_t-1 * price_t-1) - 1
        This avoids counting trades as performance.
        """
        if not end_date:
            end_date = date.today()

        # Get positions and prices
        positions_query = self.db.query(
            PositionsEOD.date,
            PositionsEOD.security_id,
            PositionsEOD.shares
        ).filter(
            PositionsEOD.account_id == account_id
        )

        if start_date:
            positions_query = positions_query.filter(PositionsEOD.date >= start_date)

        positions_query = positions_query.filter(
            PositionsEOD.date <= end_date
        ).order_by(PositionsEOD.date)

        positions = positions_query.all()

        if not positions:
            return 0

        # Convert to DataFrame
        pos_df = pd.DataFrame(positions, columns=['date', 'security_id', 'shares'])

        # Get prices
        security_ids = pos_df['security_id'].unique().tolist()
        prices = self.db.query(
            PricesEOD.date,
            PricesEOD.security_id,
            PricesEOD.close
        ).filter(
            PricesEOD.security_id.in_(security_ids)
        ).all()

        prices_df = pd.DataFrame(prices, columns=['date', 'security_id', 'close'])

        # Pivot to wide format
        pos_wide = pos_df.pivot(index='date', columns='security_id', values='shares').fillna(0)
        price_wide = prices_df.pivot(index='date', columns='security_id', values='close')

        # Forward fill prices (use last known price)
        price_wide = price_wide.ffill()

        # Align dates
        all_dates = sorted(set(pos_wide.index) | set(price_wide.index))
        pos_wide = pos_wide.reindex(all_dates).ffill().fillna(0)
        price_wide = price_wide.reindex(all_dates).ffill()

        # Compute portfolio value each day
        portfolio_values = (pos_wide * price_wide).sum(axis=1)

        # Get daily fees
        fees_df = self._get_daily_fees(account_id, start_date, end_date)

        # Compute returns using start-of-day holdings
        returns_data = []
        index_value = 100.0

        for i in range(1, len(portfolio_values)):
            date_t = portfolio_values.index[i]
            date_t_minus_1 = portfolio_values.index[i-1]

            # Start-of-day value (yesterday's holdings at yesterday's prices)
            V_t_minus_1 = portfolio_values.iloc[i-1]

            if V_t_minus_1 == 0 or pd.isna(V_t_minus_1):
                continue

            # End-of-day value using START-OF-DAY holdings at today's prices
            # This is the key: we use yesterday's shares with today's prices
            holdings_t_minus_1 = pos_wide.iloc[i-1]
            prices_t = price_wide.iloc[i]

            V_t_no_trade = (holdings_t_minus_1 * prices_t).sum()

            # Gross return (price return only, no trade impact)
            r_gross = (V_t_no_trade / V_t_minus_1) - 1 if V_t_minus_1 > 0 else 0

            # Fee drag
            fee_t = fees_df.get(date_t, 0.0)
            fee_drag = fee_t / V_t_minus_1 if V_t_minus_1 > 0 else 0

            # Net return
            r_net = r_gross - fee_drag

            # Update index
            index_value = index_value * (1 + r_net)

            returns_data.append({
                'date': date_t,
                'twr_return': r_net,
                'twr_index': index_value
            })

        # Store returns
        count = 0
        for row in returns_data:
            existing = self.db.query(ReturnsEOD).filter(
                and_(
                    ReturnsEOD.view_type == ViewType.ACCOUNT,
                    ReturnsEOD.view_id == account_id,
                    ReturnsEOD.date == row['date']
                )
            ).first()

            if existing:
                if existing.twr_return != row['twr_return']:
                    existing.twr_return = row['twr_return']
                    existing.twr_index = row['twr_index']
            else:
                ret = ReturnsEOD(
                    view_type=ViewType.ACCOUNT,
                    view_id=account_id,
                    date=row['date'],
                    twr_return=row['twr_return'],
                    twr_index=row['twr_index']
                )
                self.db.add(ret)
                count += 1

        self.db.commit()
        logger.info(f"Created {count} returns for account {account_id}")
        return count

    def _get_daily_fees(
        self,
        account_id: int,
        start_date: Optional[date],
        end_date: Optional[date]
    ) -> Dict[date, float]:
        """Get daily fee totals"""
        query = self.db.query(
            Transaction.trade_date,
            func.sum(Transaction.transaction_fee).label('total_fee')
        ).filter(
            Transaction.account_id == account_id
        )

        if start_date:
            query = query.filter(Transaction.trade_date >= start_date)
        if end_date:
            query = query.filter(Transaction.trade_date <= end_date)

        fees = query.group_by(Transaction.trade_date).all()

        return {f[0]: f[1] for f in fees}

    def get_returns_series(
        self,
        view_type: ViewType,
        view_id: int,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> List[Dict]:
        """Get returns series for a view"""
        query = self.db.query(ReturnsEOD).filter(
            and_(
                ReturnsEOD.view_type == view_type,
                ReturnsEOD.view_id == view_id
            )
        )

        if start_date:
            query = query.filter(ReturnsEOD.date >= start_date)
        if end_date:
            query = query.filter(ReturnsEOD.date <= end_date)

        returns = query.order_by(ReturnsEOD.date).all()

        return [
            {
                'date': r.date,
                'return': r.twr_return,
                'index': r.twr_index
            }
            for r in returns
        ]

    def compute_period_returns(
        self,
        view_type: ViewType,
        view_id: int,
        as_of_date: date
    ) -> Dict[str, Optional[float]]:
        """Compute returns for standard periods"""
        returns = self.db.query(ReturnsEOD).filter(
            and_(
                ReturnsEOD.view_type == view_type,
                ReturnsEOD.view_id == view_id,
                ReturnsEOD.date <= as_of_date
            )
        ).order_by(ReturnsEOD.date.desc()).all()

        if not returns:
            return {
                '1M': None, '3M': None, '6M': None,
                'YTD': None, '1Y': None, '3Y': None,
                '5Y': None, '10Y': None, 'inception': None
            }

        # Convert to DataFrame
        df = pd.DataFrame([
            {'date': r.date, 'return': r.twr_return, 'index': r.twr_index}
            for r in reversed(returns)
        ])

        current_index = df.iloc[-1]['index']

        def calc_return(lookback_days: int) -> Optional[float]:
            lookback_date = as_of_date - timedelta(days=lookback_days)
            mask = df['date'] >= lookback_date
            if mask.sum() == 0:
                return None
            start_index = df[mask].iloc[0]['index']
            return (current_index / start_index - 1) if start_index > 0 else None

        # Calculate returns
        result = {
            '1M': calc_return(30),
            '3M': calc_return(90),
            '6M': calc_return(180),
            '1Y': calc_return(365),
            '3Y': calc_return(3*365),
            '5Y': calc_return(5*365),
            '10Y': calc_return(10*365),
        }

        # YTD
        year_start = date(as_of_date.year, 1, 1)
        ytd_mask = df['date'] >= year_start
        if ytd_mask.sum() > 0:
            ytd_start_index = df[ytd_mask].iloc[0]['index']
            result['YTD'] = (current_index / ytd_start_index - 1) if ytd_start_index > 0 else None
        else:
            result['YTD'] = None

        # Inception
        inception_index = df.iloc[0]['index']
        result['inception'] = (current_index / inception_index - 1) if inception_index > 0 else None

        return result
