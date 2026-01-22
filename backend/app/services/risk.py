import pandas as pd
import numpy as np
from typing import Optional, Dict
from datetime import date, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_
from app.models import RiskEOD, ReturnsEOD, ViewType, PortfolioValueEOD
import logging

logger = logging.getLogger(__name__)


class RiskEngine:
    """Computes risk metrics: volatility, drawdown, VaR"""

    def __init__(self, db: Session):
        self.db = db

    def compute_risk_metrics(
        self,
        view_type: ViewType,
        view_id: int,
        as_of_date: date
    ) -> Optional[Dict]:
        """
        Compute risk metrics for a view as of a date:
        - vol_21d: 21-day rolling volatility (annualized)
        - vol_63d: 63-day rolling volatility (annualized)
        - max_drawdown_1y: Maximum drawdown over trailing 1 year
        - var_95_1d_hist: Historical VaR 95% 1-day using trailing 252 returns
        """
        # Get returns
        returns_query = self.db.query(ReturnsEOD).filter(
            and_(
                ReturnsEOD.view_type == view_type,
                ReturnsEOD.view_id == view_id,
                ReturnsEOD.date <= as_of_date
            )
        ).order_by(ReturnsEOD.date.desc()).limit(252)

        returns = returns_query.all()

        if not returns or len(returns) < 21:
            return None

        # Convert to DataFrame
        df = pd.DataFrame([
            {'date': r.date, 'return': r.twr_return, 'index': r.twr_index}
            for r in reversed(returns)
        ])

        # Volatility (21-day)
        vol_21d = None
        if len(df) >= 21:
            recent_21 = df.tail(21)['return']
            vol_21d = float(recent_21.std() * np.sqrt(252))

        # Volatility (63-day)
        vol_63d = None
        if len(df) >= 63:
            recent_63 = df.tail(63)['return']
            vol_63d = float(recent_63.std() * np.sqrt(252))

        # Maximum drawdown (1 year)
        max_drawdown_1y = None
        if len(df) >= 252:
            trailing_1y = df.tail(252)
        else:
            trailing_1y = df

        if len(trailing_1y) > 0:
            index_values = trailing_1y['index'].values
            running_max = np.maximum.accumulate(index_values)
            drawdowns = (index_values - running_max) / running_max
            max_drawdown_1y = float(np.min(drawdowns))

        # Historical VaR 95% (1-day)
        var_95_1d_hist = None
        if len(df) >= 252:
            returns_252 = df.tail(252)['return'].values
            var_95_1d_hist = float(np.percentile(returns_252, 5))

        metrics = {
            'vol_21d': vol_21d,
            'vol_63d': vol_63d,
            'max_drawdown_1y': max_drawdown_1y,
            'var_95_1d_hist': var_95_1d_hist
        }

        # Store metrics
        existing = self.db.query(RiskEOD).filter(
            and_(
                RiskEOD.view_type == view_type,
                RiskEOD.view_id == view_id,
                RiskEOD.date == as_of_date
            )
        ).first()

        if existing:
            for key, value in metrics.items():
                setattr(existing, key, value)
        else:
            risk = RiskEOD(
                view_type=view_type,
                view_id=view_id,
                date=as_of_date,
                **metrics
            )
            self.db.add(risk)

        self.db.commit()

        return metrics

    def compute_all_risk_metrics(
        self,
        as_of_date: Optional[date] = None
    ) -> Dict[str, int]:
        """Compute risk metrics for all views"""
        if not as_of_date:
            as_of_date = date.today()

        # Get all accounts with returns
        accounts_query = self.db.query(
            ReturnsEOD.view_id
        ).filter(
            and_(
                ReturnsEOD.view_type == ViewType.ACCOUNT,
                ReturnsEOD.date == as_of_date
            )
        ).distinct()

        account_ids = [a[0] for a in accounts_query.all()]

        # Get all groups with returns
        groups_query = self.db.query(
            ReturnsEOD.view_id
        ).filter(
            and_(
                ReturnsEOD.view_type == ViewType.GROUP,
                ReturnsEOD.date == as_of_date
            )
        ).distinct()

        group_ids = [g[0] for g in groups_query.all()]

        results = {
            'accounts_updated': 0,
            'groups_updated': 0,
            'failed': 0
        }

        # Compute for accounts
        for account_id in account_ids:
            try:
                self.compute_risk_metrics(ViewType.ACCOUNT, account_id, as_of_date)
                results['accounts_updated'] += 1
            except Exception as e:
                logger.error(f"Failed to compute risk for account {account_id}: {e}")
                results['failed'] += 1

        # Compute for groups
        for group_id in group_ids:
            try:
                self.compute_risk_metrics(ViewType.GROUP, group_id, as_of_date)
                results['groups_updated'] += 1
            except Exception as e:
                logger.error(f"Failed to compute risk for group {group_id}: {e}")
                results['failed'] += 1

        return results
