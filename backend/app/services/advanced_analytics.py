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
from app.models.sector_models import SectorClassification, BenchmarkConstituent
import logging

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
        Selection = W_p * (R_p - R_b)
        Interaction = (W_p - W_b) * (R_p - R_b)
        """
        if view_type != ViewType.ACCOUNT:
            return {'error': 'Brinson attribution only supported for account views'}

        # Get sector analyzer
        sector_analyzer = SectorAnalyzer(self.db)

        # Get portfolio sector weights at start
        port_start = sector_analyzer.get_portfolio_sector_weights(view_type, view_id, start_date)
        if 'error' in port_start:
            return port_start

        # Calculate sector returns for portfolio (simplified - would need actual tracking)
        # This is a placeholder - full implementation would track sector returns over period

        return {
            'allocation_effect': 0.0,
            'selection_effect': 0.0,
            'interaction_effect': 0.0,
            'total_active_return': 0.0,
            'note': 'Brinson attribution requires sector-level return tracking. This is a placeholder for future implementation.',
            'start_date': start_date,
            'end_date': end_date,
            'benchmark': benchmark_code
        }


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
        Decompose returns into factor contributions and alpha.
        Shows how much of return came from each factor tilt vs stock selection.
        """
        # Get returns for period
        returns = self.db.query(ReturnsEOD).filter(
            and_(
                ReturnsEOD.view_type == view_type,
                ReturnsEOD.view_id == view_id,
                ReturnsEOD.date >= start_date,
                ReturnsEOD.date <= end_date
            )
        ).order_by(ReturnsEOD.date).all()

        if not returns:
            return {'error': 'No returns data found'}

        # Calculate total return
        total_return = (returns[-1].twr_index / returns[0].twr_index - 1) if returns else 0

        # Placeholder for factor attribution
        # Full implementation would require:
        # 1. Factor returns over period
        # 2. Portfolio factor exposures over time
        # 3. Decomposition: Return = Sum(beta_i * factor_return_i) + alpha

        return {
            'total_return': float(total_return),
            'factor_contributions': {
                'market': 0.0,
                'size': 0.0,
                'value': 0.0,
                'momentum': 0.0,
                'quality': 0.0,
                'low_vol': 0.0
            },
            'alpha': float(total_return),  # Placeholder
            'note': 'Factor attribution requires factor return data. This is a placeholder.',
            'start_date': start_date,
            'end_date': end_date
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
