"""
Factor Benchmarking + Attribution Module

Provides:
1. Factor proxy data management with caching
2. Factor regression analysis
3. Return attribution by factor
"""
import pandas as pd
import numpy as np
from typing import Optional, Dict, List, Any
from datetime import date, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_, func
import statsmodels.api as sm
from scipy import stats
import logging
import os

from app.models import (
    FactorProxySeries, FactorModelDefinition, FactorAttributionResult,
    FactorDataSource, ReturnsEOD, ViewType
)
from app.services.market_data_providers import DataProviderManager

logger = logging.getLogger(__name__)


# Default factor model configurations
DEFAULT_FACTOR_MODELS = {
    'US_CORE': {
        'name': 'US Core Factor Model',
        'description': 'Core US equity factors using liquid ETF proxies',
        'factors_config': {
            'MKT': {'symbol': 'SPY', 'source': 'stooq', 'spread_vs': None, 'name': 'Market'},
            'SIZE': {'symbol': 'IWM', 'source': 'stooq', 'spread_vs': 'SPY', 'name': 'Size (Small-Cap)'},
            'VALUE': {'symbol': 'IWD', 'source': 'stooq', 'spread_vs': 'SPY', 'name': 'Value'},
            'MOM': {'symbol': 'MTUM', 'source': 'stooq', 'spread_vs': 'SPY', 'name': 'Momentum'},
            'QUAL': {'symbol': 'QUAL', 'source': 'stooq', 'spread_vs': 'SPY', 'name': 'Quality'},
            'LOWVOL': {'symbol': 'USMV', 'source': 'stooq', 'spread_vs': 'SPY', 'name': 'Low Volatility'},
        }
    },
    'US_EXTENDED': {
        'name': 'US Extended Factor Model',
        'description': 'Extended factor model with growth and dividend factors',
        'factors_config': {
            'MKT': {'symbol': 'SPY', 'source': 'stooq', 'spread_vs': None, 'name': 'Market'},
            'SIZE': {'symbol': 'IWM', 'source': 'stooq', 'spread_vs': 'SPY', 'name': 'Size'},
            'VALUE': {'symbol': 'IWD', 'source': 'stooq', 'spread_vs': 'SPY', 'name': 'Value'},
            'GROWTH': {'symbol': 'IWF', 'source': 'stooq', 'spread_vs': 'SPY', 'name': 'Growth'},
            'MOM': {'symbol': 'MTUM', 'source': 'stooq', 'spread_vs': 'SPY', 'name': 'Momentum'},
            'QUAL': {'symbol': 'QUAL', 'source': 'stooq', 'spread_vs': 'SPY', 'name': 'Quality'},
            'LOWVOL': {'symbol': 'USMV', 'source': 'stooq', 'spread_vs': 'SPY', 'name': 'Low Volatility'},
            'DIVYLD': {'symbol': 'DVY', 'source': 'stooq', 'spread_vs': 'SPY', 'name': 'Dividend Yield'},
        }
    }
}


class FactorBenchmarkingService:
    """
    Main service for factor benchmarking and attribution.
    """

    def __init__(self, db: Session, fred_api_key: Optional[str] = None):
        self.db = db
        # Try to get FRED API key from environment if not provided
        if fred_api_key is None:
            fred_api_key = os.environ.get('FRED_API_KEY')
        self.data_manager = DataProviderManager(fred_api_key=fred_api_key)

    def ensure_default_models(self):
        """Ensure default factor models exist in database"""
        for code, config in DEFAULT_FACTOR_MODELS.items():
            existing = self.db.query(FactorModelDefinition).filter(
                FactorModelDefinition.code == code
            ).first()

            if not existing:
                model = FactorModelDefinition(
                    code=code,
                    name=config['name'],
                    description=config['description'],
                    factors_config=config['factors_config'],
                    is_active=True
                )
                self.db.add(model)

        self.db.commit()

    def get_factor_model(self, code: str) -> Optional[FactorModelDefinition]:
        """Get a factor model definition"""
        return self.db.query(FactorModelDefinition).filter(
            FactorModelDefinition.code == code
        ).first()

    def get_available_models(self) -> List[Dict]:
        """Get all available factor models"""
        models = self.db.query(FactorModelDefinition).filter(
            FactorModelDefinition.is_active == True
        ).all()

        return [
            {
                'code': m.code,
                'name': m.name,
                'description': m.description,
                'factors': list(m.factors_config.keys())
            }
            for m in models
        ]

    def _get_cached_data(
        self,
        symbol: str,
        source: str,
        start_date: date,
        end_date: date
    ) -> pd.DataFrame:
        """Get cached data from database"""
        rows = self.db.query(FactorProxySeries).filter(
            and_(
                FactorProxySeries.symbol == symbol,
                FactorProxySeries.source == source,
                FactorProxySeries.date >= start_date,
                FactorProxySeries.date <= end_date
            )
        ).order_by(FactorProxySeries.date).all()

        if not rows:
            return pd.DataFrame()

        return pd.DataFrame([
            {
                'date': r.date,
                'close': r.close,
                'value': r.value,
                'daily_return': r.daily_return
            }
            for r in rows
        ])

    def _get_missing_date_ranges(
        self,
        symbol: str,
        source: str,
        start_date: date,
        end_date: date
    ) -> List[tuple]:
        """
        Determine which date ranges need to be fetched.
        Returns list of (start, end) tuples for missing ranges.
        """
        # Get all existing dates
        existing_dates = self.db.query(FactorProxySeries.date).filter(
            and_(
                FactorProxySeries.symbol == symbol,
                FactorProxySeries.source == source,
                FactorProxySeries.date >= start_date,
                FactorProxySeries.date <= end_date
            )
        ).all()

        existing_set = {d[0] for d in existing_dates}

        if not existing_set:
            return [(start_date, end_date)]

        # Find gaps
        all_dates = pd.date_range(start=start_date, end=end_date, freq='B')  # Business days
        all_dates_set = {d.date() for d in all_dates}

        missing_dates = sorted(all_dates_set - existing_set)

        if not missing_dates:
            return []

        # Group consecutive missing dates into ranges
        ranges = []
        range_start = missing_dates[0]
        prev_date = missing_dates[0]

        for d in missing_dates[1:]:
            if (d - prev_date).days > 5:  # Gap > 5 days = new range
                ranges.append((range_start, prev_date))
                range_start = d
            prev_date = d

        ranges.append((range_start, prev_date))

        return ranges

    def _cache_series_data(
        self,
        symbol: str,
        source: str,
        df: pd.DataFrame
    ):
        """Cache fetched data to database"""
        for _, row in df.iterrows():
            existing = self.db.query(FactorProxySeries).filter(
                and_(
                    FactorProxySeries.symbol == symbol,
                    FactorProxySeries.source == source,
                    FactorProxySeries.date == row['date']
                )
            ).first()

            if not existing:
                entry = FactorProxySeries(
                    symbol=symbol,
                    source=FactorDataSource(source),
                    date=row['date'],
                    close=row.get('close'),
                    value=row.get('value'),
                    daily_return=row.get('daily_return')
                )
                self.db.add(entry)

        self.db.commit()

    def refresh_factor_data(
        self,
        model_code: str,
        start_date: date,
        end_date: date
    ) -> Dict[str, int]:
        """
        Refresh factor proxy data for a model.
        Only fetches missing dates.
        Returns dict of symbol -> rows_fetched
        """
        model = self.get_factor_model(model_code)
        if not model:
            raise ValueError(f"Factor model {model_code} not found")

        results = {}
        all_symbols = set()

        # Collect all symbols needed (including spread_vs symbols)
        for factor_name, config in model.factors_config.items():
            all_symbols.add(config['symbol'])
            if config.get('spread_vs'):
                all_symbols.add(config['spread_vs'])

        for symbol in all_symbols:
            # Determine source (all ETFs use stooq for now)
            source = 'stooq'

            # Check for missing data
            missing_ranges = self._get_missing_date_ranges(
                symbol, source, start_date, end_date
            )

            rows_fetched = 0
            for range_start, range_end in missing_ranges:
                # Fetch from provider
                df, used_source = self.data_manager.fetch_etf_data(
                    symbol, range_start, range_end
                )

                if df is not None and len(df) > 0:
                    self._cache_series_data(symbol, used_source, df)
                    rows_fetched += len(df)

            results[symbol] = rows_fetched

        return results

    def get_factor_returns(
        self,
        model_code: str,
        start_date: date,
        end_date: date
    ) -> pd.DataFrame:
        """
        Get factor returns for a model over a date range.
        Computes spreads where configured.
        """
        model = self.get_factor_model(model_code)
        if not model:
            raise ValueError(f"Factor model {model_code} not found")

        # Get all required series
        all_symbols = set()
        for config in model.factors_config.values():
            all_symbols.add(config['symbol'])
            if config.get('spread_vs'):
                all_symbols.add(config['spread_vs'])

        # Fetch cached data for all symbols
        symbol_data = {}
        for symbol in all_symbols:
            df = self._get_cached_data(symbol, 'stooq', start_date, end_date)
            if len(df) > 0:
                symbol_data[symbol] = df.set_index('date')['daily_return']

        if not symbol_data:
            return pd.DataFrame()

        # Build factor returns DataFrame
        factor_returns = {}

        for factor_name, config in model.factors_config.items():
            symbol = config['symbol']
            spread_vs = config.get('spread_vs')

            if symbol not in symbol_data:
                continue

            if spread_vs and spread_vs in symbol_data:
                # Factor = symbol return - spread_vs return
                factor_returns[factor_name] = symbol_data[symbol] - symbol_data[spread_vs]
            else:
                factor_returns[factor_name] = symbol_data[symbol]

        df = pd.DataFrame(factor_returns)
        df = df.dropna()

        return df

    def run_factor_regression(
        self,
        view_type: ViewType,
        view_id: int,
        model_code: str,
        start_date: date,
        end_date: date,
        use_excess_returns: bool = False
    ) -> Optional[Dict]:
        """
        Run factor regression for a portfolio.

        Returns dict with:
        - betas: factor exposures
        - alpha: intercept (daily and annualized)
        - r_squared, adj_r_squared
        - t_stats, p_values
        - diagnostics (residual_std, durbin_watson)
        """
        # Get portfolio returns
        portfolio_returns = self.db.query(ReturnsEOD).filter(
            and_(
                ReturnsEOD.view_type == view_type,
                ReturnsEOD.view_id == view_id,
                ReturnsEOD.date >= start_date,
                ReturnsEOD.date <= end_date
            )
        ).order_by(ReturnsEOD.date).all()

        if not portfolio_returns or len(portfolio_returns) < 30:
            logger.warning(f"Insufficient portfolio returns: {len(portfolio_returns) if portfolio_returns else 0}")
            return None

        port_df = pd.DataFrame([
            {'date': r.date, 'portfolio_return': r.twr_return}
            for r in portfolio_returns
        ]).set_index('date')

        # Get factor returns
        factor_df = self.get_factor_returns(model_code, start_date, end_date)

        if factor_df.empty:
            logger.warning("No factor returns available")
            return None

        # Align dates
        merged = port_df.join(factor_df, how='inner')
        merged = merged.dropna()

        if len(merged) < 30:
            logger.warning(f"Insufficient aligned data points: {len(merged)}")
            return None

        # Prepare regression data
        y = merged['portfolio_return'].values
        factor_names = [c for c in merged.columns if c != 'portfolio_return']
        X = merged[factor_names].values

        # Winsorize extreme returns (>5 std from mean)
        y_mean, y_std = np.mean(y), np.std(y)
        y = np.clip(y, y_mean - 5*y_std, y_mean + 5*y_std)

        # Add constant for alpha
        X_with_const = sm.add_constant(X)

        try:
            # Run OLS regression
            model = sm.OLS(y, X_with_const)
            results = model.fit()

            # Extract results
            betas = {
                factor: float(results.params[i+1])
                for i, factor in enumerate(factor_names)
            }

            alpha_daily = float(results.params[0])
            alpha_annualized = alpha_daily * 252

            t_stats = {
                factor: float(results.tvalues[i+1])
                for i, factor in enumerate(factor_names)
            }

            p_values = {
                factor: float(results.pvalues[i+1])
                for i, factor in enumerate(factor_names)
            }

            # Diagnostics
            residuals = results.resid
            residual_std = float(np.std(residuals))

            # Durbin-Watson statistic for autocorrelation
            from statsmodels.stats.stattools import durbin_watson
            dw_stat = float(durbin_watson(residuals))

            return {
                'betas': betas,
                'alpha_daily': alpha_daily,
                'alpha_annualized': alpha_annualized,
                'r_squared': float(results.rsquared),
                'adj_r_squared': float(results.rsquared_adj),
                't_stats': t_stats,
                'p_values': p_values,
                'residual_std': residual_std,
                'durbin_watson': dw_stat,
                'n_observations': len(merged),
                'start_date': merged.index.min(),
                'end_date': merged.index.max(),
            }

        except Exception as e:
            logger.error(f"Factor regression failed: {e}")
            return None

    def compute_attribution(
        self,
        view_type: ViewType,
        view_id: int,
        model_code: str,
        start_date: date,
        end_date: date
    ) -> Optional[Dict]:
        """
        Compute return attribution by factor.

        Returns dict with:
        - total_return: cumulative portfolio return
        - factor_contributions: dict of factor -> contribution (as % of total)
        - alpha_contribution: alpha's contribution
        - residual_contribution: unexplained portion
        - regression_results: full regression output
        """
        # Run regression first
        reg_results = self.run_factor_regression(
            view_type, view_id, model_code, start_date, end_date
        )

        if not reg_results:
            return None

        # Get model for factor names
        model = self.get_factor_model(model_code)

        # Get daily returns for attribution
        portfolio_returns = self.db.query(ReturnsEOD).filter(
            and_(
                ReturnsEOD.view_type == view_type,
                ReturnsEOD.view_id == view_id,
                ReturnsEOD.date >= start_date,
                ReturnsEOD.date <= end_date
            )
        ).order_by(ReturnsEOD.date).all()

        port_df = pd.DataFrame([
            {'date': r.date, 'portfolio_return': r.twr_return}
            for r in portfolio_returns
        ]).set_index('date')

        factor_df = self.get_factor_returns(model_code, start_date, end_date)
        merged = port_df.join(factor_df, how='inner').dropna()

        if len(merged) < 30:
            return None

        # Compute cumulative returns
        total_return = (1 + merged['portfolio_return']).prod() - 1

        # Compute factor contributions
        # Contribution = beta * cumulative factor return
        factor_contributions = {}
        factor_cum_returns = {}

        for factor_name in reg_results['betas'].keys():
            if factor_name in merged.columns:
                cum_factor_return = (1 + merged[factor_name]).prod() - 1
                factor_cum_returns[factor_name] = cum_factor_return
                contribution = reg_results['betas'][factor_name] * cum_factor_return
                factor_contributions[factor_name] = contribution

        # Alpha contribution (annualized daily alpha * trading days / 252)
        trading_days = len(merged)
        alpha_contribution = reg_results['alpha_daily'] * trading_days

        # Residual/unexplained
        explained = sum(factor_contributions.values()) + alpha_contribution
        residual_contribution = total_return - explained

        # Convert to percentages of total return
        if abs(total_return) > 0.0001:
            factor_pct = {
                f: c / total_return * 100 for f, c in factor_contributions.items()
            }
            alpha_pct = alpha_contribution / total_return * 100
            residual_pct = residual_contribution / total_return * 100
        else:
            factor_pct = {f: 0 for f in factor_contributions}
            alpha_pct = 0
            residual_pct = 0

        # Get factor display names
        factor_names_display = {}
        if model:
            for f_name, config in model.factors_config.items():
                factor_names_display[f_name] = config.get('name', f_name)

        result = {
            'total_return': float(total_return),
            'total_return_pct': float(total_return * 100),
            'factor_contributions': {
                f: {
                    'name': factor_names_display.get(f, f),
                    'beta': reg_results['betas'].get(f, 0),
                    'factor_return': factor_cum_returns.get(f, 0) * 100,
                    'contribution': c * 100,
                    'contribution_pct': factor_pct.get(f, 0),
                    't_stat': reg_results['t_stats'].get(f, 0),
                    'p_value': reg_results['p_values'].get(f, 0),
                }
                for f, c in factor_contributions.items()
            },
            'alpha_contribution': alpha_contribution * 100,
            'alpha_contribution_pct': alpha_pct,
            'residual_contribution': residual_contribution * 100,
            'residual_contribution_pct': residual_pct,
            'regression': {
                'r_squared': reg_results['r_squared'],
                'adj_r_squared': reg_results['adj_r_squared'],
                'alpha_annualized': reg_results['alpha_annualized'] * 100,
                'residual_std': reg_results['residual_std'] * 100,
                'durbin_watson': reg_results['durbin_watson'],
                'n_observations': reg_results['n_observations'],
            },
            'period': {
                'start_date': str(reg_results['start_date']),
                'end_date': str(reg_results['end_date']),
                'trading_days': trading_days,
            }
        }

        # Store result in database
        self._store_attribution_result(
            view_type, view_id, model_code, start_date, end_date,
            reg_results, result
        )

        return result

    def _store_attribution_result(
        self,
        view_type: ViewType,
        view_id: int,
        model_code: str,
        start_date: date,
        end_date: date,
        reg_results: Dict,
        attribution: Dict
    ):
        """Store attribution result in database"""
        existing = self.db.query(FactorAttributionResult).filter(
            and_(
                FactorAttributionResult.view_type == view_type,
                FactorAttributionResult.view_id == view_id,
                FactorAttributionResult.factor_model_code == model_code,
                FactorAttributionResult.start_date == start_date,
                FactorAttributionResult.end_date == end_date
            )
        ).first()

        if existing:
            existing.betas_json = reg_results['betas']
            existing.alpha_daily = reg_results['alpha_daily']
            existing.alpha_annualized = reg_results['alpha_annualized']
            existing.r_squared = reg_results['r_squared']
            existing.adj_r_squared = reg_results['adj_r_squared']
            existing.residual_std = reg_results['residual_std']
            existing.durbin_watson = reg_results['durbin_watson']
            existing.t_stats_json = reg_results['t_stats']
            existing.p_values_json = reg_results['p_values']
            existing.total_return = attribution['total_return']
            existing.factor_contribution_json = {
                f: c['contribution'] for f, c in attribution['factor_contributions'].items()
            }
            existing.alpha_contribution = attribution['alpha_contribution']
            existing.residual_contribution = attribution['residual_contribution']
        else:
            result = FactorAttributionResult(
                view_type=view_type,
                view_id=view_id,
                factor_model_code=model_code,
                start_date=start_date,
                end_date=end_date,
                betas_json=reg_results['betas'],
                alpha_daily=reg_results['alpha_daily'],
                alpha_annualized=reg_results['alpha_annualized'],
                r_squared=reg_results['r_squared'],
                adj_r_squared=reg_results['adj_r_squared'],
                residual_std=reg_results['residual_std'],
                durbin_watson=reg_results['durbin_watson'],
                t_stats_json=reg_results['t_stats'],
                p_values_json=reg_results['p_values'],
                total_return=attribution['total_return'],
                factor_contribution_json={
                    f: c['contribution'] for f, c in attribution['factor_contributions'].items()
                },
                alpha_contribution=attribution['alpha_contribution'],
                residual_contribution=attribution['residual_contribution']
            )
            self.db.add(result)

        self.db.commit()

    def get_cached_attribution(
        self,
        view_type: ViewType,
        view_id: int,
        model_code: str,
        start_date: date,
        end_date: date
    ) -> Optional[Dict]:
        """Get cached attribution result if available"""
        result = self.db.query(FactorAttributionResult).filter(
            and_(
                FactorAttributionResult.view_type == view_type,
                FactorAttributionResult.view_id == view_id,
                FactorAttributionResult.factor_model_code == model_code,
                FactorAttributionResult.start_date == start_date,
                FactorAttributionResult.end_date == end_date
            )
        ).first()

        if not result:
            return None

        model = self.get_factor_model(model_code)
        factor_names = {}
        if model:
            for f_name, config in model.factors_config.items():
                factor_names[f_name] = config.get('name', f_name)

        return {
            'total_return': result.total_return,
            'total_return_pct': result.total_return * 100,
            'factor_contributions': {
                f: {
                    'name': factor_names.get(f, f),
                    'beta': result.betas_json.get(f, 0),
                    'contribution': c,
                    't_stat': result.t_stats_json.get(f, 0) if result.t_stats_json else 0,
                    'p_value': result.p_values_json.get(f, 0) if result.p_values_json else 0,
                }
                for f, c in (result.factor_contribution_json or {}).items()
            },
            'alpha_contribution': result.alpha_contribution,
            'residual_contribution': result.residual_contribution,
            'regression': {
                'r_squared': result.r_squared,
                'adj_r_squared': result.adj_r_squared,
                'alpha_annualized': result.alpha_annualized * 100 if result.alpha_annualized else 0,
                'residual_std': result.residual_std * 100 if result.residual_std else 0,
                'durbin_watson': result.durbin_watson,
            },
            'period': {
                'start_date': str(result.start_date),
                'end_date': str(result.end_date),
            }
        }
