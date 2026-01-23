import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import date, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_, func, desc
from app.models import (
    Transaction, PositionsEOD, PortfolioValueEOD, ReturnsEOD,
    PricesEOD, Security, Account, ViewType, TransactionType
)
from app.models.sector_models import SectorClassification, BenchmarkConstituent, FactorReturns
from app.utils.ticker_utils import TickerNormalizer
import logging
from scipy import stats

logger = logging.getLogger(__name__)


class TurnoverAnalyzer:
    """Calculate portfolio turnover metrics"""

    def __init__(self, db: Session):
        self.db = db

    def calculate_turnover(
        self,
        view_type: ViewType,
        view_id: int,
        start_date: date,
        end_date: date,
        period: str = 'monthly'
    ) -> Dict:
        """
        Calculate portfolio turnover metrics.

        Gross Turnover = (Total Buys + Total Sells) / Average Portfolio Value
        Net Turnover = abs(Total Buys - Total Sells) / Average Portfolio Value
        """
        if view_type != ViewType.ACCOUNT:
            return {'error': 'Turnover analysis only supported for account views'}

        # Get all transactions in period
        transactions = self.db.query(Transaction).filter(
            and_(
                Transaction.account_id == view_id,
                Transaction.trade_date >= start_date,
                Transaction.trade_date <= end_date,
                Transaction.transaction_type.in_([TransactionType.BUY, TransactionType.SELL])
            )
        ).order_by(Transaction.trade_date).all()

        if not transactions:
            return {'error': 'No transactions found'}

        # Get portfolio values for the period
        portfolio_values = self.db.query(PortfolioValueEOD).filter(
            and_(
                PortfolioValueEOD.view_type == view_type,
                PortfolioValueEOD.view_id == view_id,
                PortfolioValueEOD.date >= start_date,
                PortfolioValueEOD.date <= end_date
            )
        ).all()

        if not portfolio_values:
            return {'error': 'No portfolio values found'}

        # Build DataFrame
        txn_df = pd.DataFrame([{
            'date': t.trade_date,
            'type': t.transaction_type.value,
            'amount': abs(float(t.market_value or 0))
        } for t in transactions])

        value_df = pd.DataFrame([{
            'date': v.date,
            'value': float(v.total_value)
        } for v in portfolio_values])

        # Calculate by period
        if period == 'monthly':
            txn_df['period'] = pd.to_datetime(txn_df['date']).dt.to_period('M')
            value_df['period'] = pd.to_datetime(value_df['date']).dt.to_period('M')
        elif period == 'quarterly':
            txn_df['period'] = pd.to_datetime(txn_df['date']).dt.to_period('Q')
            value_df['period'] = pd.to_datetime(value_df['date']).dt.to_period('Q')
        else:  # annual
            txn_df['period'] = pd.to_datetime(txn_df['date']).dt.to_period('Y')
            value_df['period'] = pd.to_datetime(value_df['date']).dt.to_period('Y')

        # Aggregate by period
        period_stats = []
        for period_key in sorted(txn_df['period'].unique()):
            period_txns = txn_df[txn_df['period'] == period_key]
            period_values = value_df[value_df['period'] == period_key]

            if period_values.empty:
                continue

            buys = period_txns[period_txns['type'] == 'BUY']['amount'].sum()
            sells = period_txns[period_txns['type'] == 'SELL']['amount'].sum()
            avg_value = period_values['value'].mean()

            gross_turnover = (buys + sells) / avg_value if avg_value > 0 else 0
            net_turnover = abs(buys - sells) / avg_value if avg_value > 0 else 0

            period_stats.append({
                'period': str(period_key),
                'buys': float(buys),
                'sells': float(sells),
                'avg_portfolio_value': float(avg_value),
                'gross_turnover': float(gross_turnover),
                'net_turnover': float(net_turnover),
                'trade_count': len(period_txns)
            })

        # Overall metrics
        total_buys = txn_df[txn_df['type'] == 'BUY']['amount'].sum()
        total_sells = txn_df[txn_df['type'] == 'SELL']['amount'].sum()
        avg_portfolio_value = value_df['value'].mean()

        overall_gross = (total_buys + total_sells) / avg_portfolio_value if avg_portfolio_value > 0 else 0
        overall_net = abs(total_buys - total_sells) / avg_portfolio_value if avg_portfolio_value > 0 else 0

        # Annualize if needed
        days = (end_date - start_date).days
        years = days / 365.25
        annualized_gross = overall_gross / years if years > 0 else overall_gross
        annualized_net = overall_net / years if years > 0 else overall_net

        return {
            'overall': {
                'gross_turnover': float(overall_gross),
                'net_turnover': float(overall_net),
                'annualized_gross_turnover': float(annualized_gross),
                'annualized_net_turnover': float(annualized_net),
                'total_buys': float(total_buys),
                'total_sells': float(total_sells),
                'trade_count': len(transactions),
                'avg_portfolio_value': float(avg_portfolio_value)
            },
            'by_period': period_stats,
            'start_date': start_date,
            'end_date': end_date,
            'period': period
        }


class SectorAnalyzer:
    """Analyze sector exposures and compare to benchmarks"""

    def __init__(self, db: Session):
        self.db = db

    def get_portfolio_sector_weights(
        self,
        view_type: ViewType,
        view_id: int,
        as_of_date: Optional[date] = None
    ) -> Dict:
        """Get portfolio weights by sector"""
        if not as_of_date:
            as_of_date = date.today()

        if view_type != ViewType.ACCOUNT:
            return {'error': 'Sector analysis only supported for account views'}

        # Get positions
        positions = self.db.query(
            PositionsEOD.security_id,
            PositionsEOD.shares,
            Security.symbol,
            Security.asset_name,
            SectorClassification.sector,
            SectorClassification.gics_sector
        ).join(Security).outerjoin(SectorClassification).filter(
            and_(
                PositionsEOD.account_id == view_id,
                PositionsEOD.date == as_of_date,
                PositionsEOD.shares > 0
            )
        ).all()

        if not positions:
            return {'error': 'No positions found', 'sectors': []}

        # Get prices
        security_ids = [p.security_id for p in positions]
        prices = self.db.query(PricesEOD).filter(
            and_(
                PricesEOD.security_id.in_(security_ids),
                PricesEOD.date <= as_of_date
            )
        ).order_by(PricesEOD.security_id, desc(PricesEOD.date)).all()

        # Build price dict (latest price per security)
        price_dict = {}
        for p in prices:
            if p.security_id not in price_dict:
                price_dict[p.security_id] = p.close

        # Calculate market values and weights
        holdings = []
        total_value = 0

        for pos in positions:
            if pos.security_id not in price_dict:
                continue

            market_value = pos.shares * price_dict[pos.security_id]
            total_value += market_value

            holdings.append({
                'security_id': pos.security_id,
                'symbol': pos.symbol,
                'asset_name': pos.asset_name,
                'sector': pos.sector or 'Unclassified',
                'gics_sector': pos.gics_sector or 'Unclassified',
                'shares': pos.shares,
                'price': price_dict[pos.security_id],
                'market_value': market_value
            })

        # Calculate weights
        for holding in holdings:
            holding['weight'] = holding['market_value'] / total_value if total_value > 0 else 0

        # Aggregate by sector
        sector_weights = {}
        for holding in holdings:
            sector = holding['sector']
            if sector not in sector_weights:
                sector_weights[sector] = {
                    'sector': sector,
                    'weight': 0,
                    'market_value': 0,
                    'holdings_count': 0
                }
            sector_weights[sector]['weight'] += holding['weight']
            sector_weights[sector]['market_value'] += holding['market_value']
            sector_weights[sector]['holdings_count'] += 1

        return {
            'sectors': sorted(sector_weights.values(), key=lambda x: x['weight'], reverse=True),
            'total_value': float(total_value),
            'as_of_date': as_of_date,
            'holdings': holdings
        }

    def compare_to_benchmark(
        self,
        view_type: ViewType,
        view_id: int,
        benchmark_code: str = 'SP500',
        as_of_date: Optional[date] = None
    ) -> Dict:
        """Compare portfolio sector weights to benchmark"""
        portfolio_data = self.get_portfolio_sector_weights(view_type, view_id, as_of_date)

        if 'error' in portfolio_data:
            return portfolio_data

        # Get benchmark sector weights
        benchmark_constituents = self.db.query(
            BenchmarkConstituent.sector,
            func.sum(BenchmarkConstituent.weight).label('total_weight')
        ).filter(
            BenchmarkConstituent.benchmark_code == benchmark_code
        ).group_by(BenchmarkConstituent.sector).all()

        benchmark_weights = {b.sector: float(b.total_weight) for b in benchmark_constituents}

        # Build portfolio sector dict
        portfolio_weights = {s['sector']: s['weight'] for s in portfolio_data['sectors']}

        # Calculate over/under weights
        all_sectors = set(portfolio_weights.keys()) | set(benchmark_weights.keys())

        comparison = []
        for sector in all_sectors:
            port_weight = portfolio_weights.get(sector, 0)
            bench_weight = benchmark_weights.get(sector, 0)
            active_weight = port_weight - bench_weight

            comparison.append({
                'sector': sector,
                'portfolio_weight': float(port_weight),
                'benchmark_weight': float(bench_weight),
                'active_weight': float(active_weight),
                'over_under': 'Overweight' if active_weight > 0 else 'Underweight' if active_weight < 0 else 'Neutral'
            })

        comparison.sort(key=lambda x: abs(x['active_weight']), reverse=True)

        return {
            'comparison': comparison,
            'benchmark': benchmark_code,
            'as_of_date': as_of_date or date.today()
        }


class BrinsonAttributionAnalyzer:
    """Perform Brinson attribution analysis"""

    def __init__(self, db: Session):
        self.db = db

    def calculate_brinson_attribution(
        self,
        view_type: ViewType,
        view_id: int,
        benchmark_code: str,
        start_date: date,
        end_date: date
    ) -> Dict:
        """
        Calculate Brinson attribution:
        - Allocation effect: benefit from sector weighting decisions
        - Selection effect: benefit from security selection within sectors
        - Interaction effect: combined allocation and selection

        Formulas:
        Allocation = (W_p - W_b) * R_b
        Selection = W_b * (R_p - R_b)
        Interaction = (W_p - W_b) * (R_p - R_b)
        """
        if view_type != ViewType.ACCOUNT:
            return {'error': 'Brinson attribution only supported for account views'}

        # Get sector analyzer
        sector_analyzer = SectorAnalyzer(self.db)

        # Get portfolio sector weights and holdings at start
        port_start = sector_analyzer.get_portfolio_sector_weights(view_type, view_id, start_date)
        if 'error' in port_start:
            return port_start

        # Get portfolio sector weights at end
        port_end = sector_analyzer.get_portfolio_sector_weights(view_type, view_id, end_date)
        if 'error' in port_end:
            return {'error': 'Could not get end-period portfolio data'}

        # Get benchmark weights at start (use latest available before start_date)
        latest_bench_date = self.db.query(func.max(BenchmarkConstituent.as_of_date)).filter(
            and_(
                BenchmarkConstituent.benchmark_code == benchmark_code,
                BenchmarkConstituent.as_of_date <= start_date
            )
        ).scalar()

        if not latest_bench_date:
            return {
                'error': f'No benchmark data available for {benchmark_code}',
                'missing_data': 'benchmark_constituents',
                'action_required': f'Run POST /data-management/refresh-benchmark/{benchmark_code}'
            }

        # Get benchmark constituents with sectors
        benchmark_holdings = self.db.query(
            BenchmarkConstituent.symbol,
            BenchmarkConstituent.weight,
            SectorClassification.sector
        ).outerjoin(
            Security, Security.symbol == BenchmarkConstituent.symbol
        ).outerjoin(
            SectorClassification, SectorClassification.security_id == Security.id
        ).filter(
            and_(
                BenchmarkConstituent.benchmark_code == benchmark_code,
                BenchmarkConstituent.as_of_date == latest_bench_date
            )
        ).all()

        if not benchmark_holdings:
            return {'error': f'No holdings found for benchmark {benchmark_code}'}

        # Calculate sector returns for portfolio holdings
        portfolio_sector_returns = self._calculate_sector_returns(port_start['holdings'], start_date, end_date)

        # Calculate sector returns for benchmark holdings
        benchmark_sector_returns = self._calculate_benchmark_sector_returns(benchmark_holdings, start_date, end_date)

        # Build portfolio sector weights dict (use start weights)
        port_weights = {s['sector']: s['weight'] for s in port_start['sectors']}

        # Build benchmark sector weights dict
        bench_weights = {}
        for holding in benchmark_holdings:
            sector = holding.sector or 'Unclassified'
            bench_weights[sector] = bench_weights.get(sector, 0) + float(holding.weight)

        # Get all sectors
        all_sectors = set(port_weights.keys()) | set(bench_weights.keys())

        # Calculate Brinson components by sector
        attribution_by_sector = []
        total_allocation = 0.0
        total_selection = 0.0
        total_interaction = 0.0

        for sector in all_sectors:
            W_p = port_weights.get(sector, 0.0)
            W_b = bench_weights.get(sector, 0.0)
            R_p = portfolio_sector_returns.get(sector, 0.0)
            R_b = benchmark_sector_returns.get(sector, 0.0)

            allocation = (W_p - W_b) * R_b
            selection = W_b * (R_p - R_b)
            interaction = (W_p - W_b) * (R_p - R_b)

            total_allocation += allocation
            total_selection += selection
            total_interaction += interaction

            attribution_by_sector.append({
                'sector': sector,
                'portfolio_weight': float(W_p),
                'benchmark_weight': float(W_b),
                'portfolio_return': float(R_p),
                'benchmark_return': float(R_b),
                'allocation_effect': float(allocation),
                'selection_effect': float(selection),
                'interaction_effect': float(interaction),
                'total_effect': float(allocation + selection + interaction)
            })

        attribution_by_sector.sort(key=lambda x: abs(x['total_effect']), reverse=True)

        return {
            'allocation_effect': float(total_allocation),
            'selection_effect': float(total_selection),
            'interaction_effect': float(total_interaction),
            'total_active_return': float(total_allocation + total_selection + total_interaction),
            'by_sector': attribution_by_sector,
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat(),
            'benchmark': benchmark_code,
            'benchmark_data_date': latest_bench_date.isoformat()
        }

    def _calculate_sector_returns(self, holdings: List[Dict], start_date: date, end_date: date) -> Dict[str, float]:
        """Calculate sector returns for portfolio holdings"""
        sector_returns = {}
        sector_start_values = {}
        sector_end_values = {}

        for holding in holdings:
            sector = holding['sector']
            security_id = holding['security_id']

            # Get prices at start and end
            start_price = self.db.query(PricesEOD.close).filter(
                and_(
                    PricesEOD.security_id == security_id,
                    PricesEOD.date <= start_date
                )
            ).order_by(desc(PricesEOD.date)).first()

            end_price = self.db.query(PricesEOD.close).filter(
                and_(
                    PricesEOD.security_id == security_id,
                    PricesEOD.date <= end_date
                )
            ).order_by(desc(PricesEOD.date)).first()

            if start_price and end_price:
                start_val = holding['shares'] * float(start_price[0])
                end_val = holding['shares'] * float(end_price[0])

                sector_start_values[sector] = sector_start_values.get(sector, 0) + start_val
                sector_end_values[sector] = sector_end_values.get(sector, 0) + end_val

        # Calculate returns
        for sector in sector_start_values:
            if sector_start_values[sector] > 0:
                sector_returns[sector] = (sector_end_values[sector] / sector_start_values[sector]) - 1

        return sector_returns

    def _calculate_benchmark_sector_returns(self, holdings: List, start_date: date, end_date: date) -> Dict[str, float]:
        """Calculate sector returns for benchmark holdings"""
        sector_returns = {}
        sector_weighted_returns = {}
        sector_weights = {}

        for holding in holdings:
            sector = holding.sector or 'Unclassified'
            ticker = holding.symbol
            weight = float(holding.weight)

            # Find security by ticker
            security = self.db.query(Security).filter(
                Security.symbol == TickerNormalizer.normalize(ticker)
            ).first()

            if not security:
                continue

            # Get prices
            start_price = self.db.query(PricesEOD.close).filter(
                and_(
                    PricesEOD.security_id == security.id,
                    PricesEOD.date <= start_date
                )
            ).order_by(desc(PricesEOD.date)).first()

            end_price = self.db.query(PricesEOD.close).filter(
                and_(
                    PricesEOD.security_id == security.id,
                    PricesEOD.date <= end_date
                )
            ).order_by(desc(PricesEOD.date)).first()

            if start_price and end_price:
                security_return = (float(end_price[0]) / float(start_price[0])) - 1

                sector_weighted_returns[sector] = sector_weighted_returns.get(sector, 0) + (security_return * weight)
                sector_weights[sector] = sector_weights.get(sector, 0) + weight

        # Calculate sector returns (weighted average)
        for sector in sector_weights:
            if sector_weights[sector] > 0:
                sector_returns[sector] = sector_weighted_returns[sector] / sector_weights[sector]

        return sector_returns


class AdvancedFactorAnalyzer:
    """Advanced factor analysis including attribution and crowding"""

    def __init__(self, db: Session):
        self.db = db

    def calculate_factor_attribution(
        self,
        view_type: ViewType,
        view_id: int,
        start_date: date,
        end_date: date
    ) -> Dict:
        """
        Decompose returns into factor contributions and alpha using Fama-French factors.
        Shows how much of return came from each factor tilt vs stock selection (alpha).

        Uses regression: R_portfolio - R_f = alpha + beta_mkt*(R_m - R_f) + beta_smb*SMB + ...
        """
        # Get portfolio daily returns
        returns = self.db.query(ReturnsEOD).filter(
            and_(
                ReturnsEOD.view_type == view_type,
                ReturnsEOD.view_id == view_id,
                ReturnsEOD.date >= start_date,
                ReturnsEOD.date <= end_date
            )
        ).order_by(ReturnsEOD.date).all()

        if not returns or len(returns) < 2:
            return {'error': 'Insufficient returns data found'}

        # Get factor returns for the same period
        factor_returns_data = self.db.query(FactorReturns).filter(
            and_(
                FactorReturns.date >= start_date,
                FactorReturns.date <= end_date
            )
        ).order_by(FactorReturns.date).all()

        if not factor_returns_data:
            return {
                'error': 'No factor returns data available',
                'missing_data': 'factor_returns',
                'action_required': 'Run POST /data-management/refresh-factor-returns'
            }

        # Build dataframes
        portfolio_df = pd.DataFrame([{
            'date': r.date,
            'daily_return': float(r.daily_return) if r.daily_return else 0
        } for r in returns])

        factor_df = pd.DataFrame([{
            'date': f.date,
            'factor': f.factor_name,
            'return': float(f.value)
        } for f in factor_returns_data])

        # Pivot factor data
        factor_pivot = factor_df.pivot(index='date', columns='factor', values='return')

        # Merge with portfolio returns
        merged = portfolio_df.merge(factor_pivot, left_on='date', right_index=True, how='inner')

        if len(merged) < 10:
            return {'error': 'Insufficient overlapping data for regression (need at least 10 days)'}

        # Calculate excess returns (portfolio return - risk-free rate)
        if 'RF' in merged.columns:
            merged['excess_return'] = merged['daily_return'] - merged['RF']
        else:
            merged['excess_return'] = merged['daily_return']  # Assume RF = 0

        # Prepare regression data
        y = merged['excess_return'].values
        X_factors = []
        factor_names = []

        # Market factor (Mkt-RF)
        if 'Mkt-RF' in merged.columns:
            X_factors.append(merged['Mkt-RF'].values)
            factor_names.append('Market')

        # Size factor (SMB)
        if 'SMB' in merged.columns:
            X_factors.append(merged['SMB'].values)
            factor_names.append('Size (SMB)')

        # Value factor (HML)
        if 'HML' in merged.columns:
            X_factors.append(merged['HML'].values)
            factor_names.append('Value (HML)')

        # Profitability factor (RMW)
        if 'RMW' in merged.columns:
            X_factors.append(merged['RMW'].values)
            factor_names.append('Profitability (RMW)')

        # Investment factor (CMA)
        if 'CMA' in merged.columns:
            X_factors.append(merged['CMA'].values)
            factor_names.append('Investment (CMA)')

        # Momentum factor
        if 'Mom' in merged.columns:
            X_factors.append(merged['Mom'].values)
            factor_names.append('Momentum')

        if not X_factors:
            return {'error': 'No factor data available for regression'}

        # Run regression
        X = np.column_stack(X_factors)
        X_with_intercept = np.column_stack([np.ones(len(X)), X])

        # Use statsmodels-style regression with scipy
        result = np.linalg.lstsq(X_with_intercept, y, rcond=None)
        coefficients = result[0]

        alpha = coefficients[0]
        betas = coefficients[1:]

        # Calculate factor contributions to total return
        # Contribution = beta * sum(factor_returns)
        factor_contributions = {}
        factor_exposures = {}

        for i, factor_name in enumerate(factor_names):
            beta = betas[i]
            factor_col = ['Mkt-RF', 'SMB', 'HML', 'RMW', 'CMA', 'Mom'][i] if i < 6 else None

            if factor_col and factor_col in merged.columns:
                factor_total_return = merged[factor_col].sum()
                contribution = beta * factor_total_return
                factor_contributions[factor_name] = float(contribution)
                factor_exposures[factor_name] = float(beta)

        # Calculate R-squared
        y_pred = X_with_intercept @ coefficients
        ss_res = np.sum((y - y_pred) ** 2)
        ss_tot = np.sum((y - np.mean(y)) ** 2)
        r_squared = 1 - (ss_res / ss_tot) if ss_tot > 0 else 0

        # Calculate total returns
        total_portfolio_return = float(merged['daily_return'].sum())
        total_factor_contribution = sum(factor_contributions.values())
        alpha_contribution = alpha * len(merged)  # Alpha per day * number of days

        return {
            'total_return': total_portfolio_return,
            'factor_contributions': factor_contributions,
            'factor_exposures': factor_exposures,
            'alpha': float(alpha_contribution),
            'alpha_daily': float(alpha),
            'r_squared': float(r_squared),
            'explained_return': float(total_factor_contribution),
            'unexplained_return': float(alpha_contribution),
            'observation_count': len(merged),
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat()
        }

    def analyze_factor_crowding(
        self,
        view_type: ViewType,
        view_id: int
    ) -> Dict:
        """
        Analyze how correlated portfolio holdings are on factor basis.
        High factor crowding means multiple positions express same factor bets.
        """
        # Placeholder for crowding analysis
        # Full implementation would:
        # 1. Get factor loadings for all holdings
        # 2. Calculate correlation matrix of factor exposures
        # 3. Identify clusters of similar factor profiles

        return {
            'crowding_score': 0.0,
            'diversification_ratio': 0.0,
            'note': 'Factor crowding analysis requires factor loading data. This is a placeholder.'
        }
