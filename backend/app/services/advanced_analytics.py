import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple
from datetime import date, timedelta
from concurrent.futures import ThreadPoolExecutor, as_completed
from sqlalchemy.orm import Session
from sqlalchemy import and_, func, desc
from sqlalchemy.dialects.postgresql import insert as pg_insert
from app.models import (
    Transaction, PositionsEOD, PortfolioValueEOD, ReturnsEOD,
    PricesEOD, Security, Account, ViewType, TransactionType, AssetClass
)
from app.models.sector_models import SectorClassification, BenchmarkConstituent, FactorReturns
from app.utils.ticker_utils import TickerNormalizer
from app.core.config import settings
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

    def _get_account_ids(self, view_type: ViewType, view_id: int) -> List[int]:
        """Get account IDs for a given view (account, group, or firm)"""
        from app.models import Account, Group, GroupMember

        if view_type == ViewType.ACCOUNT:
            return [view_id]
        elif view_type in (ViewType.GROUP, ViewType.FIRM):
            # For GROUP and FIRM views, get all member account IDs
            # GroupMember uses member_id and member_type fields
            members = self.db.query(GroupMember.member_id).filter(
                GroupMember.group_id == view_id,
                GroupMember.member_type == "account"
            ).all()
            return [m.member_id for m in members]
        else:
            return []

    def get_portfolio_sector_weights(
        self,
        view_type: ViewType,
        view_id: int,
        as_of_date: Optional[date] = None,
        group_by: str = 'sector'
    ) -> Dict:
        """Get portfolio weights grouped by sector, industry, or country.

        Args:
            group_by: 'sector' (default), 'industry', or 'country'
        """
        if not as_of_date:
            as_of_date = date.today()

        # Get account IDs based on view type
        account_ids = self._get_account_ids(view_type, view_id)
        if not account_ids:
            return {'error': 'No accounts found for this view', 'sectors': []}

        # Find the latest position date on or before as_of_date across all accounts
        latest_pos_date = self.db.query(func.max(PositionsEOD.date)).filter(
            and_(
                PositionsEOD.account_id.in_(account_ids),
                PositionsEOD.date <= as_of_date
            )
        ).scalar()

        if not latest_pos_date:
            return {'error': 'No positions found', 'sectors': []}

        # Get positions with all classification fields
        positions = self.db.query(
            PositionsEOD.security_id,
            func.sum(PositionsEOD.shares).label('shares'),
            Security.symbol,
            Security.asset_name,
            SectorClassification.sector,
            SectorClassification.gics_sector,
            SectorClassification.gics_industry,
            SectorClassification.country,
        ).select_from(PositionsEOD).join(
            Security, PositionsEOD.security_id == Security.id
        ).outerjoin(
            SectorClassification, SectorClassification.security_id == Security.id
        ).filter(
            and_(
                PositionsEOD.account_id.in_(account_ids),
                PositionsEOD.date == latest_pos_date,
                PositionsEOD.shares > 0
            )
        ).group_by(
            PositionsEOD.security_id,
            Security.symbol,
            Security.asset_name,
            SectorClassification.sector,
            SectorClassification.gics_sector,
            SectorClassification.gics_industry,
            SectorClassification.country,
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
                'industry': pos.gics_industry or 'Unclassified',
                'country': pos.country or 'Unclassified',
                'shares': pos.shares,
                'price': price_dict[pos.security_id],
                'market_value': market_value
            })

        # Calculate weights
        for holding in holdings:
            holding['weight'] = holding['market_value'] / total_value if total_value > 0 else 0

        # Determine which field to group by
        group_field = {
            'sector': 'sector',
            'industry': 'industry',
            'country': 'country',
        }.get(group_by, 'sector')

        # Aggregate by chosen field
        sector_weights = {}
        for holding in holdings:
            key = holding[group_field]
            if key not in sector_weights:
                sector_weights[key] = {
                    'sector': key,
                    'weight': 0,
                    'market_value': 0,
                    'holdings_count': 0
                }
            sector_weights[key]['weight'] += holding['weight']
            sector_weights[key]['market_value'] += holding['market_value']
            sector_weights[key]['holdings_count'] += 1

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

        # Debug: log what sectors are in the database
        sectors_from_db = set(c.sector for c in benchmark_constituents if c.sector)
        no_sector_count = sum(1 for c in benchmark_constituents if not c.sector)
        logger.info(f"Benchmark {benchmark_code}: {len(benchmark_constituents)} constituents, "
                    f"{len(sectors_from_db)} unique sectors, {no_sector_count} without sector")
        if sectors_from_db:
            logger.info(f"  Sectors from DB: {sorted(sectors_from_db)}")
        # Sample constituents
        for c in benchmark_constituents[:5]:
            logger.info(f"  Sample: {c.symbol} weight={c.weight} sector='{c.sector}'")

        # Build a fallback sector lookup from SectorClassification for symbols without sectors
        sector_lookup = {}
        classifications = self.db.query(
            Security.symbol,
            SectorClassification.sector,
            SectorClassification.gics_sector
        ).join(
            SectorClassification, Security.id == SectorClassification.security_id
        ).all()
        for c in classifications:
            sector_lookup[c.symbol] = c.sector or c.gics_sector

        # Aggregate weights by sector
        benchmark_weights = {}
        total_weight = 0.0
        unclassified_count = 0
        for constituent in benchmark_constituents:
            # Use stored sector, or look up from SectorClassification
            sector = constituent.sector
            if not sector:
                sector = sector_lookup.get(constituent.symbol)
            if not sector:
                unclassified_count += 1
                continue  # Skip unclassified instead of grouping them
            weight = float(constituent.weight)
            benchmark_weights[sector] = benchmark_weights.get(sector, 0) + weight
            total_weight += weight

        if unclassified_count > 0:
            logger.warning(f"Benchmark {benchmark_code}: {unclassified_count} constituents without sector classification (skipped)")

        # Log for debugging - total should be ~1.0
        logger.info(f"Benchmark {benchmark_code}: {len(benchmark_constituents)} constituents, total weight={total_weight:.4f}")

        # Normalize benchmark weights if they're stored as percentages (total near 100)
        # Portfolio weights are decimals (0.05 for 5%), benchmark might be percentages (5.0 for 5%)
        if total_weight > 10:  # Weights stored as percentages
            for sector in benchmark_weights:
                benchmark_weights[sector] = benchmark_weights[sector] / total_weight

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

    # Mapping from benchmark definition codes to ETF proxy codes used in BenchmarkReturn table
    BENCHMARK_ETF_PROXY = {
        'SP500': 'SPY',
        'NASDAQ': 'QQQ',
        'DOW': 'INDU',
    }

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

        Formulas (BHB model):
        Allocation = (W_p - W_b) * R_b
        Selection = W_b * (R_p - R_b)
        Interaction = (W_p - W_b) * (R_p - R_b)
        """
        sector_analyzer = SectorAnalyzer(self.db)

        # Get portfolio sector weights and holdings at start
        port_start = sector_analyzer.get_portfolio_sector_weights(view_type, view_id, start_date)
        if 'error' in port_start:
            return port_start

        # Get portfolio sector weights at end (validates end-period data exists)
        port_end = sector_analyzer.get_portfolio_sector_weights(view_type, view_id, end_date)
        if 'error' in port_end:
            return {'error': 'Could not get end-period portfolio data'}

        # ── Benchmark constituent data ──────────────────────────────────
        latest_bench_date = self.db.query(func.max(BenchmarkConstituent.as_of_date)).filter(
            BenchmarkConstituent.benchmark_code == benchmark_code
        ).scalar()

        if not latest_bench_date:
            return {
                'error': f'No benchmark data available for {benchmark_code}',
                'missing_data': 'benchmark_constituents',
                'action_required': f'Run POST /data-management/refresh-benchmark/{benchmark_code}'
            }

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

        # ── Build unified sector lookup from SectorClassification ───────
        # Use the SAME sector source for both portfolio and benchmark to prevent
        # taxonomy mismatches (e.g., "Technology" vs "Communication" for META).
        sector_lookup = {}
        classifications = self.db.query(
            Security.symbol,
            SectorClassification.sector,
            SectorClassification.gics_sector
        ).join(
            SectorClassification, Security.id == SectorClassification.security_id
        ).all()
        for c in classifications:
            sector_lookup[c.symbol] = c.sector or c.gics_sector

        # Enrich benchmark holdings — prefer SectorClassification (same as portfolio),
        # fall back to BenchmarkConstituent.sector from the SPY file
        enriched_holdings = []
        unclassified_count = 0
        for holding in benchmark_holdings:
            sector = sector_lookup.get(holding.symbol) or holding.sector
            if sector:
                enriched_holdings.append({
                    'symbol': holding.symbol,
                    'weight': holding.weight,
                    'sector': sector
                })
            else:
                unclassified_count += 1

        if unclassified_count > 0:
            logger.warning(f"Brinson: {unclassified_count} benchmark constituents have no sector classification")

        # ── Pre-fetch prices in batch (avoid N+1 queries) ──────────────
        # Collect all security IDs we'll need prices for
        benchmark_symbols = [h['symbol'] for h in enriched_holdings]
        portfolio_security_ids = [h['security_id'] for h in port_start['holdings']]

        # Lookup security IDs for benchmark tickers
        bench_securities = self.db.query(
            Security.id, Security.symbol
        ).filter(
            Security.symbol.in_([TickerNormalizer.normalize(s) for s in benchmark_symbols])
        ).all()
        bench_symbol_to_id = {s.symbol: s.id for s in bench_securities}

        all_security_ids = set(portfolio_security_ids) | set(bench_symbol_to_id.values())

        # Batch-fetch closest prices at start_date and end_date
        start_prices = self._batch_get_prices(all_security_ids, start_date)
        end_prices = self._batch_get_prices(all_security_ids, end_date)

        # ── Tiingo fallback for missing benchmark prices ─────────────────
        # Fetch prices from Tiingo for any benchmark constituents not in the
        # local PricesEOD table, store them for future caching, and update
        # the in-memory dicts so this request gets 100% coverage.
        tiingo_filled = self._ensure_benchmark_prices(
            enriched_holdings, bench_symbol_to_id,
            start_date, end_date,
            start_prices, end_prices,
        )
        if tiingo_filled > 0:
            logger.info(f"Brinson: Tiingo filled {tiingo_filled} missing benchmark tickers")

        # ── Calculate sector returns ────────────────────────────────────
        portfolio_sector_returns = self._calculate_sector_returns_batch(
            port_start['holdings'], start_prices, end_prices
        )

        benchmark_sector_returns, bench_coverage = self._calculate_benchmark_sector_returns_batch(
            enriched_holdings, bench_symbol_to_id, start_prices, end_prices
        )

        # ── Build weight dictionaries ───────────────────────────────────
        port_weights = {s['sector']: s['weight'] for s in port_start['sectors']}

        bench_weights = {}
        total_bench_weight = 0.0
        for holding in enriched_holdings:
            sector = holding['sector']
            weight = float(holding['weight'])
            bench_weights[sector] = bench_weights.get(sector, 0) + weight
            total_bench_weight += weight

        # Normalize benchmark weights if stored as percentages (total near 100)
        if total_bench_weight > 10:
            for sector in bench_weights:
                bench_weights[sector] = bench_weights[sector] / total_bench_weight

        # ── Brinson decomposition ───────────────────────────────────────
        all_sectors = set(port_weights.keys()) | set(bench_weights.keys())

        attribution_by_sector = []
        total_allocation = 0.0
        total_selection = 0.0
        total_interaction = 0.0
        total_portfolio_return = 0.0
        total_benchmark_return = 0.0

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

            total_portfolio_return += W_p * R_p
            total_benchmark_return += W_b * R_b

            sector_entry: Dict = {
                'sector': sector,
                'portfolio_weight': float(W_p),
                'benchmark_weight': float(W_b),
                'portfolio_return': float(R_p),
                'benchmark_return': float(R_b),
                'allocation_effect': float(allocation),
                'selection_effect': float(selection),
                'interaction_effect': float(interaction),
                'total_effect': float(allocation + selection + interaction),
            }

            # Attach coverage info for benchmark sectors
            if sector in bench_coverage:
                cov = bench_coverage[sector]
                sector_entry['bench_coverage'] = {
                    'priced': cov['priced'],
                    'total': cov['total'],
                    'pct': round(cov['priced'] / cov['total'] * 100, 1) if cov['total'] > 0 else 0,
                }

            attribution_by_sector.append(sector_entry)

        attribution_by_sector.sort(key=lambda x: abs(x['total_effect']), reverse=True)

        contributors = sorted(
            [s for s in attribution_by_sector if s['total_effect'] > 0],
            key=lambda x: x['total_effect'],
            reverse=True
        )[:5]

        detractors = sorted(
            [s for s in attribution_by_sector if s['total_effect'] < 0],
            key=lambda x: x['total_effect']
        )[:5]

        # ── Actual returns for reconciliation ───────────────────────────
        actual_portfolio_return = self._get_actual_portfolio_return(view_type, view_id, start_date, end_date)

        # Map benchmark definition code to ETF proxy code for BenchmarkReturn lookup
        etf_proxy = self.BENCHMARK_ETF_PROXY.get(benchmark_code, 'SPY')
        actual_benchmark_return = self._get_actual_benchmark_return(etf_proxy, start_date, end_date)

        attributed_active_return = float(total_allocation + total_selection + total_interaction)
        actual_active_return = (actual_portfolio_return or 0) - (actual_benchmark_return or 0)

        # ── Overall coverage summary ────────────────────────────────────
        total_bench_constituents = len(enriched_holdings)
        total_priced = sum(c['priced'] for c in bench_coverage.values())
        total_in_sectors = sum(c['total'] for c in bench_coverage.values())
        overall_coverage_pct = round(total_priced / total_in_sectors * 100, 1) if total_in_sectors > 0 else 0

        return {
            'allocation_effect': float(total_allocation),
            'selection_effect': float(total_selection),
            'interaction_effect': float(total_interaction),
            'total_active_return': actual_active_return,
            'portfolio_return': actual_portfolio_return,
            'benchmark_return': actual_benchmark_return,
            'attributed_active_return': attributed_active_return,
            'unattributed': actual_active_return - attributed_active_return if actual_portfolio_return and actual_benchmark_return else 0,
            'top_contributors': contributors,
            'top_detractors': detractors,
            'by_sector': attribution_by_sector,
            'start_date': start_date.isoformat(),
            'end_date': end_date.isoformat(),
            'benchmark': benchmark_code,
            'benchmark_data_date': latest_bench_date.isoformat(),
            'coverage': {
                'benchmark_constituents': total_bench_constituents,
                'priced_constituents': total_priced,
                'coverage_pct': overall_coverage_pct,
                'unclassified_constituents': unclassified_count,
                'tiingo_fetched': tiingo_filled,
            },
        }

    def _batch_get_prices(self, security_ids: set, as_of_date: date) -> Dict[int, float]:
        """
        Batch-fetch the latest closing price on or before as_of_date for each security.
        Returns {security_id: close_price}.
        """
        if not security_ids:
            return {}

        # Use a subquery to find the max date <= as_of_date per security
        subq = self.db.query(
            PricesEOD.security_id,
            func.max(PricesEOD.date).label('max_date')
        ).filter(
            and_(
                PricesEOD.security_id.in_(security_ids),
                PricesEOD.date <= as_of_date
            )
        ).group_by(PricesEOD.security_id).subquery()

        rows = self.db.query(PricesEOD.security_id, PricesEOD.close).join(
            subq,
            and_(
                PricesEOD.security_id == subq.c.security_id,
                PricesEOD.date == subq.c.max_date
            )
        ).all()

        return {r.security_id: float(r.close) for r in rows}

    def _ensure_benchmark_prices(
        self,
        enriched_holdings: List[Dict],
        bench_symbol_to_id: Dict[str, int],
        start_date: date,
        end_date: date,
        start_prices: Dict[int, float],
        end_prices: Dict[int, float],
    ) -> int:
        """
        For benchmark constituents missing local price data, fetch from Tiingo
        and store in PricesEOD for future caching.  Updates bench_symbol_to_id,
        start_prices, and end_prices dicts *in-place*.

        Returns the number of newly-priced tickers.
        """
        if not settings.TIINGO_API_KEY:
            logger.warning("No TIINGO_API_KEY configured – cannot fetch missing benchmark prices")
            return 0

        # ── 1. Identify which benchmark symbols are missing ──────────────
        missing_symbols: set = set()
        for holding in enriched_holdings:
            ticker = TickerNormalizer.normalize(holding['symbol'])
            security_id = bench_symbol_to_id.get(ticker)

            if security_id is None:
                missing_symbols.add(ticker)
            elif security_id not in start_prices or security_id not in end_prices:
                missing_symbols.add(ticker)

        if not missing_symbols:
            logger.info("Brinson: All benchmark constituents have local price data (100%% coverage)")
            return 0

        logger.info(f"Brinson: Fetching Tiingo prices for {len(missing_symbols)} missing benchmark tickers")

        # ── 2. Initialise Tiingo client ──────────────────────────────────
        try:
            from tiingo import TiingoClient
            client = TiingoClient({'api_key': settings.TIINGO_API_KEY, 'session': True})
        except Exception as e:
            logger.error(f"Failed to init Tiingo client for Brinson fallback: {e}")
            return 0

        # ── 3. Fetch prices in parallel ──────────────────────────────────
        fetch_start = start_date - timedelta(days=7)  # buffer for weekends/holidays

        def _fetch_one(symbol: str):
            """Fetch prices for a single ticker from Tiingo."""
            try:
                tiingo_sym = symbol.replace('.', '-').upper()
                data = client.get_ticker_price(
                    tiingo_sym,
                    startDate=fetch_start.strftime('%Y-%m-%d'),
                    endDate=end_date.strftime('%Y-%m-%d'),
                    frequency='daily',
                )
                if not data:
                    return symbol, None

                prices = {}
                for row in data:
                    d = pd.to_datetime(row['date']).date()
                    prices[d] = float(row.get('adjClose', row.get('close', 0)))

                if not prices:
                    return symbol, None

                # Find closest price on or before start_date and end_date
                sorted_dates = sorted(prices.keys(), reverse=True)
                sp = ep = None
                for d in sorted_dates:
                    if d <= end_date and ep is None:
                        ep = prices[d]
                    if d <= start_date and sp is None:
                        sp = prices[d]
                    if sp is not None and ep is not None:
                        break

                return symbol, {'start': sp, 'end': ep, 'all_prices': prices}
            except Exception as exc:
                logger.debug(f"Tiingo fetch failed for {symbol}: {exc}")
                return symbol, None

        results: Dict[str, Dict] = {}
        with ThreadPoolExecutor(max_workers=10) as executor:
            futures = {executor.submit(_fetch_one, sym): sym for sym in missing_symbols}
            for future in as_completed(futures):
                sym, data = future.result()
                if data and data['start'] is not None and data['end'] is not None:
                    results[sym] = data

        if not results:
            logger.warning("Brinson: Could not fetch any missing benchmark prices from Tiingo")
            return 0

        logger.info(
            f"Brinson: Got Tiingo prices for {len(results)}/{len(missing_symbols)} missing tickers"
        )

        # ── 4. Create Security records for truly-new tickers ─────────────
        for symbol in results:
            if symbol in bench_symbol_to_id:
                continue  # already has a Security row
            existing = self.db.query(Security).filter(Security.symbol == symbol).first()
            if existing:
                bench_symbol_to_id[symbol] = existing.id
            else:
                new_sec = Security(symbol=symbol, asset_class=AssetClass.EQUITY)
                self.db.add(new_sec)
                self.db.flush()  # assigns id
                bench_symbol_to_id[symbol] = new_sec.id

        # ── 5. Bulk-insert prices into PricesEOD (cache for next time) ───
        price_rows = []
        for symbol, price_data in results.items():
            security_id = bench_symbol_to_id[symbol]
            for d, price in price_data['all_prices'].items():
                price_rows.append({
                    'security_id': security_id,
                    'date': d,
                    'close': price,
                    'source': 'tiingo',
                })

        if price_rows:
            try:
                stmt = pg_insert(PricesEOD.__table__).values(price_rows)
                stmt = stmt.on_conflict_do_nothing(
                    constraint='uq_price_security_date'
                )
                self.db.execute(stmt)
                self.db.commit()
                logger.info(f"Brinson: Cached {len(price_rows)} price records in PricesEOD")
            except Exception as e:
                logger.warning(f"Failed to cache Tiingo prices in PricesEOD: {e}")
                self.db.rollback()

        # ── 6. Update the in-memory price dicts for this request ─────────
        fetched_count = 0
        for symbol, price_data in results.items():
            security_id = bench_symbol_to_id[symbol]
            start_prices[security_id] = price_data['start']
            end_prices[security_id] = price_data['end']
            fetched_count += 1

        return fetched_count

    def _get_actual_portfolio_return(self, view_type: ViewType, view_id: int, start_date: date, end_date: date) -> Optional[float]:
        """Get actual portfolio return from ReturnsEOD table"""
        from app.models import ReturnsEOD

        # FIRM views are stored as GROUP in the database
        db_vt = ViewType.GROUP if view_type == ViewType.FIRM else view_type

        start_record = self.db.query(ReturnsEOD.twr_index).filter(
            and_(
                ReturnsEOD.view_type == db_vt,
                ReturnsEOD.view_id == view_id,
                ReturnsEOD.date <= start_date
            )
        ).order_by(desc(ReturnsEOD.date)).first()

        end_record = self.db.query(ReturnsEOD.twr_index).filter(
            and_(
                ReturnsEOD.view_type == db_vt,
                ReturnsEOD.view_id == view_id,
                ReturnsEOD.date <= end_date
            )
        ).order_by(desc(ReturnsEOD.date)).first()

        if start_record and end_record and start_record[0] and end_record[0]:
            return (float(end_record[0]) / float(start_record[0])) - 1
        return None

    def _get_actual_benchmark_return(self, etf_code: str, start_date: date, end_date: date) -> Optional[float]:
        """Get actual benchmark return from BenchmarkReturn table by compounding daily returns.
        etf_code should be the ETF proxy code (e.g., 'SPY'), not the benchmark definition code."""
        from app.models import BenchmarkReturn

        daily_returns = self.db.query(BenchmarkReturn.return_value).filter(
            and_(
                BenchmarkReturn.code == etf_code,
                BenchmarkReturn.date > start_date,
                BenchmarkReturn.date <= end_date
            )
        ).order_by(BenchmarkReturn.date).all()

        if not daily_returns:
            return None

        cumulative = 1.0
        for (ret,) in daily_returns:
            if ret is not None:
                cumulative *= (1.0 + ret)

        return cumulative - 1.0

    def _calculate_sector_returns_batch(
        self,
        holdings: List[Dict],
        start_prices: Dict[int, float],
        end_prices: Dict[int, float]
    ) -> Dict[str, float]:
        """Calculate sector returns for portfolio holdings using pre-fetched prices."""
        sector_start_values: Dict[str, float] = {}
        sector_end_values: Dict[str, float] = {}

        for holding in holdings:
            sector = holding['sector']
            security_id = holding['security_id']

            sp = start_prices.get(security_id)
            ep = end_prices.get(security_id)

            if sp is not None and ep is not None:
                start_val = holding['shares'] * sp
                end_val = holding['shares'] * ep

                sector_start_values[sector] = sector_start_values.get(sector, 0) + start_val
                sector_end_values[sector] = sector_end_values.get(sector, 0) + end_val

        sector_returns = {}
        for sector in sector_start_values:
            if sector_start_values[sector] > 0:
                sector_returns[sector] = (sector_end_values[sector] / sector_start_values[sector]) - 1

        return sector_returns

    def _calculate_benchmark_sector_returns_batch(
        self,
        holdings: List[Dict],
        symbol_to_id: Dict[str, int],
        start_prices: Dict[int, float],
        end_prices: Dict[int, float]
    ) -> Tuple[Dict[str, float], Dict[str, Dict]]:
        """
        Calculate sector returns for benchmark holdings using pre-fetched prices.
        Returns (sector_returns, coverage_by_sector).
        coverage_by_sector: {sector: {'priced': N, 'total': M}}
        """
        sector_weighted_returns: Dict[str, float] = {}
        sector_weights: Dict[str, float] = {}
        sector_total_count: Dict[str, int] = {}
        sector_priced_count: Dict[str, int] = {}

        for holding in holdings:
            sector = holding['sector']
            ticker = TickerNormalizer.normalize(holding['symbol'])
            weight = float(holding['weight'])

            sector_total_count[sector] = sector_total_count.get(sector, 0) + 1

            security_id = symbol_to_id.get(ticker)
            if security_id is None:
                continue

            sp = start_prices.get(security_id)
            ep = end_prices.get(security_id)

            if sp is not None and ep is not None and sp > 0:
                security_return = (ep / sp) - 1

                sector_weighted_returns[sector] = sector_weighted_returns.get(sector, 0) + (security_return * weight)
                sector_weights[sector] = sector_weights.get(sector, 0) + weight
                sector_priced_count[sector] = sector_priced_count.get(sector, 0) + 1

        sector_returns = {}
        for sector in sector_weights:
            if sector_weights[sector] > 0:
                sector_returns[sector] = sector_weighted_returns[sector] / sector_weights[sector]

        # Build coverage dict
        all_sectors = set(sector_total_count.keys())
        coverage = {}
        for sector in all_sectors:
            coverage[sector] = {
                'priced': sector_priced_count.get(sector, 0),
                'total': sector_total_count[sector],
            }

        return sector_returns, coverage


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
