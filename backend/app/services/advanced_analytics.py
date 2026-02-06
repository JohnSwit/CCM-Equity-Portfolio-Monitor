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


def _resolve_sector_for_symbol(db: Session, symbol: str):
    """
    Resolve sector for a symbol using multiple fallback sources.
    Tries: Static mapping -> SectorClassification table.
    Returns the sector string or None if unresolvable.
    """
    from app.services.data_sourcing import ClassificationService

    if not symbol:
        return None

    normalized = TickerNormalizer.normalize(symbol)

    # 1. Try static mapping (fast, in-memory)
    static = ClassificationService.STATIC_MAPPING.get(normalized)
    if static and static.get('sector'):
        return static['sector']

    # 2. Try SectorClassification table
    result = db.query(SectorClassification.sector).join(
        Security, SectorClassification.security_id == Security.id
    ).filter(Security.symbol == normalized).first()
    if result and result.sector:
        return result.sector

    return None


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

        # Find the latest position date on or before as_of_date
        latest_pos_date = self.db.query(func.max(PositionsEOD.date)).filter(
            and_(
                PositionsEOD.account_id == view_id,
                PositionsEOD.date <= as_of_date
            )
        ).scalar()

        if not latest_pos_date:
            return {'error': 'No positions found', 'sectors': []}

        # Get positions using the latest available date
        positions = self.db.query(
            PositionsEOD.security_id,
            PositionsEOD.shares,
            Security.symbol,
            Security.asset_name,
            SectorClassification.sector,
            SectorClassification.gics_sector
        ).select_from(PositionsEOD).join(
            Security, PositionsEOD.security_id == Security.id
        ).outerjoin(
            SectorClassification, SectorClassification.security_id == Security.id
        ).filter(
            and_(
                PositionsEOD.account_id == view_id,
                PositionsEOD.date == latest_pos_date,
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

        # Calculate market values, resolve sectors, and compute weights
        holdings = []
        total_value = 0

        for pos in positions:
            if pos.security_id not in price_dict:
                continue

            market_value = pos.shares * price_dict[pos.security_id]

            # Resolve sector: DB classification first, then static mapping fallback
            resolved_sector = pos.sector or _resolve_sector_for_symbol(self.db, pos.symbol)
            if not resolved_sector:
                # Skip holdings that cannot be classified into any sector
                continue

            total_value += market_value

            holdings.append({
                'security_id': pos.security_id,
                'symbol': pos.symbol,
                'asset_name': pos.asset_name,
                'sector': resolved_sector,
                'gics_sector': pos.gics_sector or resolved_sector,
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
            'as_of_date': latest_pos_date,
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

        # Get the latest benchmark data date
        latest_bench_date = self.db.query(func.max(BenchmarkConstituent.as_of_date)).filter(
            BenchmarkConstituent.benchmark_code == benchmark_code
        ).scalar()

        if not latest_bench_date:
            return {
                'error': f'No benchmark data available for {benchmark_code}',
                'missing_data': 'benchmark_constituents',
                'action_required': f'Run POST /data-management/refresh-benchmark/{benchmark_code}'
            }

        # Get benchmark sector weights using the sector stored in BenchmarkConstituent
        # Filter by latest date to ensure we only use current data
        benchmark_constituents = self.db.query(
            BenchmarkConstituent.symbol,
            BenchmarkConstituent.weight,
            BenchmarkConstituent.sector
        ).filter(
            and_(
                BenchmarkConstituent.benchmark_code == benchmark_code,
                BenchmarkConstituent.as_of_date == latest_bench_date
            )
        ).all()

        if not benchmark_constituents:
            return {
                'error': f'No benchmark data available for {benchmark_code}',
                'missing_data': 'benchmark_constituents',
                'action_required': f'Run POST /data-management/refresh-benchmark/{benchmark_code}'
            }

        # Aggregate weights by sector, resolving unclassified via fallback
        benchmark_weights = {}
        total_classified_weight = 0.0
        for constituent in benchmark_constituents:
            sector = constituent.sector or _resolve_sector_for_symbol(self.db, constituent.symbol)
            if not sector:
                continue  # Skip constituents that cannot be classified
            weight = float(constituent.weight)
            benchmark_weights[sector] = benchmark_weights.get(sector, 0) + weight
            total_classified_weight += weight

        # Renormalize benchmark weights so they sum to 1.0
        if total_classified_weight > 0 and abs(total_classified_weight - 1.0) > 0.001:
            for sector in benchmark_weights:
                benchmark_weights[sector] /= total_classified_weight

        # Log for debugging
        logger.info(f"Benchmark {benchmark_code}: {len(benchmark_constituents)} constituents, classified weight={total_classified_weight:.4f}")

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

        # Get benchmark weights (use latest available data)
        # Note: We use the latest snapshot since we don't have historical benchmark constituents
        latest_bench_date = self.db.query(func.max(BenchmarkConstituent.as_of_date)).filter(
            BenchmarkConstituent.benchmark_code == benchmark_code
        ).scalar()

        if not latest_bench_date:
            return {
                'error': f'No benchmark data available for {benchmark_code}',
                'missing_data': 'benchmark_constituents',
                'action_required': f'Run POST /data-management/refresh-benchmark/{benchmark_code}'
            }

        # Get benchmark constituents with sectors (sector is stored during refresh)
        benchmark_holdings = self.db.query(
            BenchmarkConstituent.symbol,
            BenchmarkConstituent.weight,
            BenchmarkConstituent.sector
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

        # Build benchmark sector weights dict, resolving unclassified via fallback
        bench_weights = {}
        total_bench_classified = 0.0
        for holding in benchmark_holdings:
            sector = holding.sector or _resolve_sector_for_symbol(self.db, holding.symbol)
            if not sector:
                continue  # Skip unclassifiable benchmark constituents
            weight = float(holding.weight)
            bench_weights[sector] = bench_weights.get(sector, 0) + weight
            total_bench_classified += weight

        # Renormalize benchmark weights so they sum to 1.0
        if total_bench_classified > 0 and abs(total_bench_classified - 1.0) > 0.001:
            for sector in bench_weights:
                bench_weights[sector] /= total_bench_classified

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

    # Mapping from GICS sector names to SPDR sector ETFs
    SECTOR_ETF_MAP = {
        'Information Technology': 'XLK',
        'Technology': 'XLK',
        'Health Care': 'XLV',
        'Healthcare': 'XLV',
        'Financials': 'XLF',
        'Consumer Discretionary': 'XLY',
        'Communication Services': 'XLC',
        'Communication': 'XLC',
        'Industrials': 'XLI',
        'Consumer Staples': 'XLP',
        'Energy': 'XLE',
        'Utilities': 'XLU',
        'Real Estate': 'XLRE',
        'Materials': 'XLB',
    }

    def _calculate_benchmark_sector_returns(self, holdings: List, start_date: date, end_date: date) -> Dict[str, float]:
        """Calculate sector returns for benchmark holdings.

        Uses local price data where available, then falls back to sector ETF
        proxies (SPDR ETFs) for sectors without local price data.
        """
        sector_returns = {}
        sector_weighted_returns = {}
        sector_weights = {}
        all_benchmark_sectors = set()

        for holding in holdings:
            sector = holding.sector or _resolve_sector_for_symbol(self.db, holding.symbol)
            if not sector:
                continue
            all_benchmark_sectors.add(sector)
            ticker = holding.symbol
            weight = float(holding.weight)

            # Find security by ticker in local DB
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

        # Calculate sector returns from local data (weighted average)
        for sector in sector_weights:
            if sector_weights[sector] > 0:
                sector_returns[sector] = sector_weighted_returns[sector] / sector_weights[sector]

        # Identify sectors missing returns and fill via sector ETF proxies
        missing_sectors = all_benchmark_sectors - set(sector_returns.keys())
        if missing_sectors:
            etf_returns = self._fetch_sector_etf_returns(missing_sectors, start_date, end_date)
            for sector, ret in etf_returns.items():
                sector_returns[sector] = ret

        return sector_returns

    def _fetch_sector_etf_returns(self, sectors: set, start_date: date, end_date: date) -> Dict[str, float]:
        """Fetch sector returns using SPDR sector ETF proxies via yfinance."""
        import yfinance as yf

        results = {}
        # Collect unique ETF tickers needed
        etf_to_sectors: Dict[str, List[str]] = {}
        for sector in sectors:
            etf = self.SECTOR_ETF_MAP.get(sector)
            if etf:
                etf_to_sectors.setdefault(etf, []).append(sector)

        if not etf_to_sectors:
            return results

        tickers = list(etf_to_sectors.keys())
        try:
            # Fetch prices for all needed ETFs in one call
            # Add buffer days to ensure we get prices on or before start_date
            fetch_start = start_date - timedelta(days=10)
            fetch_end = end_date + timedelta(days=1)
            data = yf.download(tickers, start=fetch_start, end=fetch_end, progress=False)

            if data.empty:
                logger.warning("No ETF price data returned from yfinance")
                return results

            if isinstance(data.columns, pd.MultiIndex):
                close = data['Close']
            else:
                # Single ticker - no multi-index
                close = pd.DataFrame({tickers[0]: data['Close']})

            for etf, sector_list in etf_to_sectors.items():
                col = etf if etf in close.columns else None
                if col is None:
                    continue
                series = close[col].dropna()
                if series.empty:
                    continue

                # Get price on or before start_date
                start_prices = series[series.index.date <= start_date]
                end_prices = series[series.index.date <= end_date]

                if start_prices.empty or end_prices.empty:
                    continue

                p_start = float(start_prices.iloc[-1])
                p_end = float(end_prices.iloc[-1])

                if p_start > 0:
                    etf_return = (p_end / p_start) - 1
                    for sector in sector_list:
                        results[sector] = etf_return

        except Exception as e:
            logger.error(f"Error fetching sector ETF returns: {e}")

        return results


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
            'daily_return': float(r.twr_return) if r.twr_return else 0
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

    def calculate_historical_factor_exposures(
        self,
        view_type: ViewType,
        view_id: int,
        end_date: date,
        lookback_days: int = 504,  # 2 years
        rolling_window: int = 63   # ~3 months
    ) -> Dict:
        """
        Calculate rolling factor exposures over time.
        Shows how factor tilts have evolved.
        """
        from app.models import ReturnsEOD, FactorReturns

        start_date = end_date - timedelta(days=lookback_days)

        # Get portfolio returns
        returns = self.db.query(ReturnsEOD).filter(
            and_(
                ReturnsEOD.view_type == view_type,
                ReturnsEOD.view_id == view_id,
                ReturnsEOD.date >= start_date,
                ReturnsEOD.date <= end_date
            )
        ).order_by(ReturnsEOD.date).all()

        if len(returns) < rolling_window + 10:
            return {'error': 'Insufficient returns data for rolling analysis'}

        # Get factor returns
        factor_returns_data = self.db.query(FactorReturns).filter(
            and_(
                FactorReturns.date >= start_date,
                FactorReturns.date <= end_date
            )
        ).order_by(FactorReturns.date).all()

        if not factor_returns_data:
            return {'error': 'No factor returns data available'}

        # Build DataFrames
        portfolio_df = pd.DataFrame([{
            'date': r.date,
            'return': float(r.twr_return) if r.twr_return else 0
        } for r in returns]).set_index('date')

        factor_df = pd.DataFrame([{
            'date': f.date,
            'factor': f.factor_name,
            'return': float(f.value)
        } for f in factor_returns_data])

        factor_pivot = factor_df.pivot(index='date', columns='factor', values='return')

        # Merge
        merged = portfolio_df.join(factor_pivot, how='inner')

        if len(merged) < rolling_window + 10:
            return {'error': 'Insufficient overlapping data'}

        # Factor columns to use
        factor_cols = [c for c in ['Mkt-RF', 'SMB', 'HML', 'RMW', 'CMA', 'Mom'] if c in merged.columns]
        factor_labels = {
            'Mkt-RF': 'Market', 'SMB': 'Size', 'HML': 'Value',
            'RMW': 'Profitability', 'CMA': 'Investment', 'Mom': 'Momentum'
        }

        # Calculate rolling regressions
        historical_exposures = []

        for i in range(rolling_window, len(merged)):
            window_data = merged.iloc[i-rolling_window:i]
            window_date = merged.index[i]

            y = window_data['return'].values
            if 'RF' in window_data.columns:
                y = y - window_data['RF'].values

            X = window_data[factor_cols].values
            X_with_intercept = np.column_stack([np.ones(len(X)), X])

            try:
                coefficients = np.linalg.lstsq(X_with_intercept, y, rcond=None)[0]
                alpha = coefficients[0]
                betas = coefficients[1:]

                exposures = {'date': window_date.isoformat(), 'alpha': float(alpha) * 252}
                for j, col in enumerate(factor_cols):
                    exposures[factor_labels.get(col, col)] = float(betas[j])

                historical_exposures.append(exposures)
            except Exception:
                continue

        return {
            'historical_exposures': historical_exposures,
            'rolling_window_days': rolling_window,
            'factors': [factor_labels.get(c, c) for c in factor_cols]
        }

    def calculate_factor_risk_decomposition(
        self,
        view_type: ViewType,
        view_id: int,
        start_date: date,
        end_date: date
    ) -> Dict:
        """
        Decompose portfolio risk (variance) into factor risk and specific risk.
        Shows what % of portfolio volatility comes from each factor.
        """
        from app.models import ReturnsEOD, FactorReturns

        # Get portfolio returns
        returns = self.db.query(ReturnsEOD).filter(
            and_(
                ReturnsEOD.view_type == view_type,
                ReturnsEOD.view_id == view_id,
                ReturnsEOD.date >= start_date,
                ReturnsEOD.date <= end_date
            )
        ).order_by(ReturnsEOD.date).all()

        if len(returns) < 30:
            return {'error': 'Insufficient returns data (need at least 30 days)'}

        # Get factor returns
        factor_returns_data = self.db.query(FactorReturns).filter(
            and_(
                FactorReturns.date >= start_date,
                FactorReturns.date <= end_date
            )
        ).order_by(FactorReturns.date).all()

        if not factor_returns_data:
            return {'error': 'No factor returns data available'}

        # Build DataFrames
        portfolio_df = pd.DataFrame([{
            'date': r.date,
            'return': float(r.twr_return) if r.twr_return else 0
        } for r in returns]).set_index('date')

        factor_df = pd.DataFrame([{
            'date': f.date,
            'factor': f.factor_name,
            'return': float(f.value)
        } for f in factor_returns_data])

        factor_pivot = factor_df.pivot(index='date', columns='factor', values='return')
        merged = portfolio_df.join(factor_pivot, how='inner')

        if len(merged) < 30:
            return {'error': 'Insufficient overlapping data'}

        # Factor columns
        factor_cols = [c for c in ['Mkt-RF', 'SMB', 'HML', 'RMW', 'CMA', 'Mom'] if c in merged.columns]
        factor_labels = {
            'Mkt-RF': 'Market', 'SMB': 'Size', 'HML': 'Value',
            'RMW': 'Profitability', 'CMA': 'Investment', 'Mom': 'Momentum'
        }

        if len(factor_cols) == 0:
            return {'error': 'No factor columns available in data'}

        y = merged['return'].values
        if 'RF' in merged.columns:
            y = y - merged['RF'].values

        X = merged[factor_cols].values
        X_with_intercept = np.column_stack([np.ones(len(X)), X])

        # Run regression
        coefficients = np.linalg.lstsq(X_with_intercept, y, rcond=None)[0]
        betas = coefficients[1:]

        # Calculate predicted returns and residuals
        y_pred = X_with_intercept @ coefficients
        residuals = y - y_pred

        # Total variance
        total_variance = np.var(y)

        # Factor variance contribution
        # Var(factor component) = beta^2 * Var(factor) + covariance terms
        # Handle case of single factor (returns scalar) vs multiple factors (returns matrix)
        factor_cov = np.cov(X.T)
        if factor_cov.ndim == 0:
            # Single factor case: cov is a scalar
            factor_variance = float(betas[0] ** 2 * factor_cov)
        else:
            # Multiple factors case: cov is a matrix
            factor_variance = float(betas @ factor_cov @ betas)

        # Specific (residual) variance
        specific_variance = np.var(residuals)

        # Calculate individual factor contributions
        factor_risk_contributions = {}
        for i, col in enumerate(factor_cols):
            factor_var = np.var(X[:, i])
            contribution = (betas[i] ** 2) * factor_var
            factor_risk_contributions[factor_labels.get(col, col)] = {
                'variance_contribution': float(contribution),
                'pct_of_total': float(contribution / total_variance * 100) if total_variance > 0 else 0,
                'beta': float(betas[i])
            }

        # Annualized volatilities
        ann_factor = np.sqrt(252)
        total_vol = np.std(y) * ann_factor
        factor_vol = np.sqrt(factor_variance) * ann_factor
        specific_vol = np.sqrt(specific_variance) * ann_factor

        return {
            'total_volatility': float(total_vol),
            'factor_volatility': float(factor_vol),
            'specific_volatility': float(specific_vol),
            'factor_risk_pct': float(factor_variance / total_variance * 100) if total_variance > 0 else 0,
            'specific_risk_pct': float(specific_variance / total_variance * 100) if total_variance > 0 else 0,
            'factor_contributions': factor_risk_contributions,
            'observation_count': len(merged)
        }
