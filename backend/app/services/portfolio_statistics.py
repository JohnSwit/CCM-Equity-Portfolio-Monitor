import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import date, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_, func, desc
from app.models import (
    ReturnsEOD, PositionsEOD, PortfolioValueEOD, PricesEOD,
    Security, Account, ViewType, BenchmarkReturn, FactorRegression
)
import logging

logger = logging.getLogger(__name__)


class PortfolioStatisticsEngine:
    """Calculate advanced portfolio statistics and analytics"""

    def __init__(self, db: Session):
        self.db = db
        self.trading_days_per_year = 252

    def get_contribution_to_returns(
        self,
        view_type: ViewType,
        view_id: int,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        top_n: int = 20
    ) -> Dict:
        """
        Calculate each holding's contribution to total portfolio return.
        Contribution = (position weight) * (position return)
        """
        if not end_date:
            end_date = date.today()
        if not start_date:
            # Use earliest available returns date for "all time"
            earliest = self.db.query(func.min(ReturnsEOD.date)).filter(
                and_(
                    ReturnsEOD.view_type == view_type,
                    ReturnsEOD.view_id == view_id
                )
            ).scalar()
            start_date = earliest if earliest else end_date - timedelta(days=365)

        # Get portfolio returns for the period
        portfolio_returns = self.db.query(ReturnsEOD).filter(
            and_(
                ReturnsEOD.view_type == view_type,
                ReturnsEOD.view_id == view_id,
                ReturnsEOD.date >= start_date,
                ReturnsEOD.date <= end_date
            )
        ).order_by(ReturnsEOD.date).all()

        if not portfolio_returns:
            return {'contributions': [], 'total_return': 0.0}

        # Get all positions for the period
        if view_type == ViewType.ACCOUNT:
            positions = self.db.query(
                PositionsEOD.security_id,
                PositionsEOD.date,
                PositionsEOD.shares,
                Security.symbol,
                Security.asset_name
            ).join(Security).filter(
                and_(
                    PositionsEOD.account_id == view_id,
                    PositionsEOD.date >= start_date,
                    PositionsEOD.date <= end_date
                )
            ).all()
        else:
            # For groups/firm, would need to aggregate - simplified for now
            return {'contributions': [], 'total_return': 0.0}

        # Build position DataFrame
        pos_df = pd.DataFrame([{
            'security_id': p.security_id,
            'date': p.date,
            'shares': p.shares,
            'symbol': p.symbol,
            'asset_name': p.asset_name
        } for p in positions])

        if pos_df.empty:
            return {'contributions': [], 'total_return': 0.0}

        # Get prices for all securities
        security_ids = pos_df['security_id'].unique().tolist()
        prices = self.db.query(
            PricesEOD.security_id,
            PricesEOD.date,
            PricesEOD.close
        ).filter(
            and_(
                PricesEOD.security_id.in_(security_ids),
                PricesEOD.date >= start_date,
                PricesEOD.date <= end_date
            )
        ).all()

        prices_df = pd.DataFrame([{
            'security_id': p.security_id,
            'date': p.date,
            'price': p.close
        } for p in prices])

        # Merge positions and prices
        df = pos_df.merge(prices_df, on=['security_id', 'date'], how='left')
        df['market_value'] = df['shares'] * df['price']

        # Calculate daily portfolio values
        daily_values = df.groupby('date')['market_value'].sum().reset_index()
        daily_values.columns = ['date', 'total_value']

        # Merge to get weights
        df = df.merge(daily_values, on='date')
        df['weight'] = df['market_value'] / df['total_value']

        # Calculate daily returns for each security
        df = df.sort_values(['security_id', 'date'])
        df['prev_price'] = df.groupby('security_id')['price'].shift(1)
        df['security_return'] = (df['price'] / df['prev_price']) - 1
        df['security_return'] = df['security_return'].fillna(0)

        # Calculate contribution (weight * return)
        df['contribution'] = df['weight'] * df['security_return']

        # Aggregate by security
        contributions = df.groupby(['security_id', 'symbol', 'asset_name']).agg({
            'contribution': 'sum',
            'weight': 'mean'  # Average weight over period
        }).reset_index()

        contributions = contributions.sort_values('contribution', ascending=False)
        contributions = contributions.head(top_n)

        # Calculate total portfolio return
        total_return = ((portfolio_returns[-1].twr_index / portfolio_returns[0].twr_index) - 1) if portfolio_returns else 0.0

        return {
            'contributions': [
                {
                    'symbol': row['symbol'],
                    'asset_name': row['asset_name'],
                    'contribution': float(row['contribution']),
                    'avg_weight': float(row['weight']),
                    'contribution_pct': float(row['contribution'] / total_return * 100) if total_return != 0 else 0.0
                }
                for _, row in contributions.iterrows()
            ],
            'total_return': float(total_return),
            'period_start': start_date,
            'period_end': end_date
        }

    def get_volatility_metrics(
        self,
        view_type: ViewType,
        view_id: int,
        benchmark_code: str = 'SPY',
        window: int = 252
    ) -> Dict:
        """
        Calculate volatility and related risk metrics.
        """
        # Get portfolio returns
        returns = self.db.query(ReturnsEOD).filter(
            and_(
                ReturnsEOD.view_type == view_type,
                ReturnsEOD.view_id == view_id
            )
        ).order_by(desc(ReturnsEOD.date)).limit(window).all()

        if len(returns) < 20:
            return {'error': 'Insufficient data'}

        returns = list(reversed(returns))
        port_returns = np.array([r.twr_return for r in returns])

        # Get benchmark returns
        dates = [r.date for r in returns]
        bench_returns = self.db.query(BenchmarkReturn).filter(
            and_(
                BenchmarkReturn.code == benchmark_code,
                BenchmarkReturn.date.in_(dates)
            )
        ).all()

        bench_dict = {b.date: b.return_value for b in bench_returns}
        bench_returns_arr = np.array([bench_dict.get(d, 0) for d in dates])

        # Active returns
        active_returns = port_returns - bench_returns_arr

        # Calculate metrics
        volatility = np.std(port_returns) * np.sqrt(self.trading_days_per_year)
        active_vol = np.std(active_returns) * np.sqrt(self.trading_days_per_year)

        # Tracking error
        tracking_error = active_vol

        # Information ratio
        mean_active_return = np.mean(active_returns) * self.trading_days_per_year
        information_ratio = mean_active_return / tracking_error if tracking_error > 0 else 0

        # Downside deviation (Sortino)
        downside_returns = port_returns[port_returns < 0]
        downside_deviation = np.std(downside_returns) * np.sqrt(self.trading_days_per_year) if len(downside_returns) > 0 else 0

        mean_return = np.mean(port_returns) * self.trading_days_per_year
        sortino_ratio = mean_return / downside_deviation if downside_deviation > 0 else 0

        # Skewness and Kurtosis
        from scipy import stats
        skewness = stats.skew(port_returns)
        kurtosis = stats.kurtosis(port_returns)

        return {
            'annualized_volatility': float(volatility),
            'tracking_error': float(tracking_error),
            'active_volatility': float(active_vol),
            'information_ratio': float(information_ratio),
            'downside_deviation': float(downside_deviation),
            'sortino_ratio': float(sortino_ratio),
            'skewness': float(skewness),
            'kurtosis': float(kurtosis),
            'mean_return': float(mean_return),
            'active_return': float(mean_active_return),
            'window_days': len(returns),
            'benchmark': benchmark_code
        }

    def get_drawdown_analysis(
        self,
        view_type: ViewType,
        view_id: int
    ) -> Dict:
        """
        Calculate drawdown metrics including max drawdown, current drawdown,
        time to recovery, and ulcer index.
        """
        # Get all returns
        returns = self.db.query(ReturnsEOD).filter(
            and_(
                ReturnsEOD.view_type == view_type,
                ReturnsEOD.view_id == view_id
            )
        ).order_by(ReturnsEOD.date).all()

        if len(returns) < 2:
            return {'error': 'Insufficient data'}

        # Build cumulative index
        dates = [r.date for r in returns]
        index_values = np.array([r.twr_index for r in returns])

        # Calculate running maximum
        running_max = np.maximum.accumulate(index_values)

        # Calculate drawdown
        drawdown = (index_values / running_max) - 1

        # Max drawdown
        max_drawdown = np.min(drawdown)
        max_dd_idx = np.argmin(drawdown)
        max_dd_date = dates[max_dd_idx]

        # Current drawdown
        current_drawdown = drawdown[-1]

        # Find recovery date for max drawdown
        recovery_date = None
        if max_dd_idx < len(dates) - 1:
            peak_value = running_max[max_dd_idx]
            for i in range(max_dd_idx + 1, len(dates)):
                if index_values[i] >= peak_value:
                    recovery_date = dates[i]
                    break

        # Time to recovery
        days_to_recovery = None
        if recovery_date:
            days_to_recovery = (recovery_date - max_dd_date).days

        # Ulcer Index (RMS of drawdowns)
        ulcer_index = np.sqrt(np.mean(drawdown ** 2))

        # Drawdown periods (consecutive negative periods)
        drawdown_periods = []
        in_drawdown = False
        dd_start = None
        dd_peak_value = None

        for i, dd in enumerate(drawdown):
            if dd < -0.01 and not in_drawdown:  # Start of drawdown (>1% down)
                in_drawdown = True
                dd_start = dates[i]
                dd_peak_value = running_max[i]
            elif dd >= -0.001 and in_drawdown:  # Recovery (within 0.1%)
                drawdown_periods.append({
                    'start_date': dd_start,
                    'end_date': dates[i],
                    'peak_value': float(dd_peak_value),
                    'trough_value': float(index_values[i]),
                    'max_drawdown': float(np.min(drawdown[dates.index(dd_start):i+1])),
                    'days': (dates[i] - dd_start).days
                })
                in_drawdown = False

        # If still in drawdown
        if in_drawdown:
            drawdown_periods.append({
                'start_date': dd_start,
                'end_date': dates[-1],
                'peak_value': float(dd_peak_value),
                'trough_value': float(index_values[-1]),
                'max_drawdown': float(np.min(drawdown[dates.index(dd_start):])),
                'days': (dates[-1] - dd_start).days,
                'ongoing': True
            })

        return {
            'max_drawdown': float(max_drawdown),
            'max_drawdown_date': max_dd_date,
            'days_to_recovery': days_to_recovery,
            'recovery_date': recovery_date,
            'current_drawdown': float(current_drawdown),
            'ulcer_index': float(ulcer_index),
            'drawdown_periods': sorted(drawdown_periods, key=lambda x: x['max_drawdown'])[:10]  # Top 10 worst
        }

    def get_var_cvar(
        self,
        view_type: ViewType,
        view_id: int,
        confidence_levels: List[float] = [0.95, 0.99],
        window: int = 252
    ) -> Dict:
        """
        Calculate Value at Risk (VaR) and Conditional Value at Risk (CVaR/Expected Shortfall).
        """
        # Get returns
        returns = self.db.query(ReturnsEOD).filter(
            and_(
                ReturnsEOD.view_type == view_type,
                ReturnsEOD.view_id == view_id
            )
        ).order_by(desc(ReturnsEOD.date)).limit(window).all()

        if len(returns) < 20:
            return {'error': 'Insufficient data'}

        returns_arr = np.array([r.twr_return for r in reversed(returns)])

        results = {}
        for conf in confidence_levels:
            # Historical VaR
            var = np.percentile(returns_arr, (1 - conf) * 100)

            # CVaR (Expected Shortfall) - average of returns worse than VaR
            cvar = np.mean(returns_arr[returns_arr <= var])

            results[f'var_{int(conf*100)}'] = float(var)
            results[f'cvar_{int(conf*100)}'] = float(cvar)

        return results

    def get_factor_analysis(
        self,
        view_type: ViewType,
        view_id: int,
        as_of_date: Optional[date] = None
    ) -> Dict:
        """
        Get factor tilts and attribution from factor regression.
        """
        if not as_of_date:
            as_of_date = date.today()

        # Get latest factor regression
        regression = self.db.query(FactorRegression).filter(
            and_(
                FactorRegression.view_type == view_type,
                FactorRegression.view_id == view_id,
                FactorRegression.as_of_date <= as_of_date
            )
        ).order_by(desc(FactorRegression.as_of_date)).first()

        if not regression:
            return {'error': 'No factor regression data available'}

        # Parse exposures (stored in betas_json)
        exposures = regression.betas_json or {}

        # Calculate total factor variance vs idiosyncratic
        total_var = regression.r_squared if regression.r_squared else 0
        idio_var = 1 - total_var

        return {
            'alpha_annualized': float(regression.alpha) if regression.alpha else 0,
            'r_squared': float(regression.r_squared) if regression.r_squared else 0,
            'factor_exposures': {k: float(v) for k, v in exposures.items()},
            'factor_variance_pct': float(total_var * 100),
            'idiosyncratic_variance_pct': float(idio_var * 100),
            'as_of_date': regression.as_of_date,
            'window_days': regression.window
        }
