"""
Factor Benchmarking + Attribution Module (Enhanced)

Provides:
1. Factor proxy data management with caching
2. Factor regression analysis with diagnostics
3. Return attribution by factor
4. Rolling analysis
5. Benchmark-relative attribution
6. Multicollinearity diagnostics (VIF)
"""
import pandas as pd
import numpy as np
from typing import Optional, Dict, List, Any, Tuple
from datetime import date, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_, func
import statsmodels.api as sm
from statsmodels.stats.stattools import durbin_watson
from statsmodels.stats.outliers_influence import variance_inflation_factor
from statsmodels.stats.diagnostic import het_breuschpagan
from scipy import stats
from scipy.stats import jarque_bera
import logging
import os

from app.models import (
    FactorProxySeries, FactorModelDefinition, FactorAttributionResult,
    FactorDataSource, ReturnsEOD, ViewType
)
from app.services.market_data_providers import DataProviderManager

logger = logging.getLogger(__name__)


# Default factor model configurations - using Tiingo as primary source
DEFAULT_FACTOR_MODELS = {
    'US_CORE': {
        'name': 'US Core Factor Model',
        'description': 'Core US equity factors using liquid ETF proxies with macro overlay',
        'factors_config': {
            'MKT': {'symbol': 'SPY', 'source': 'tiingo', 'spread_vs': None, 'name': 'Market', 'category': 'style'},
            'SIZE': {'symbol': 'IWM', 'source': 'tiingo', 'spread_vs': 'SPY', 'name': 'Size', 'category': 'style'},
            'GROWTH_VALUE': {'symbol': 'IWF', 'source': 'tiingo', 'spread_vs': 'IWD', 'name': 'Growth / Value', 'category': 'style'},
            'MOM': {'symbol': 'MTUM', 'source': 'tiingo', 'spread_vs': 'SPY', 'name': 'Momentum', 'category': 'style'},
            'QUAL': {'symbol': 'QUAL', 'source': 'tiingo', 'spread_vs': 'SPY', 'name': 'Quality', 'category': 'style'},
            'LOWVOL': {'symbol': 'SPLV', 'source': 'tiingo', 'spread_vs': 'SPY', 'name': 'Low Volatility', 'category': 'style'},
            'DURATION': {'symbol': 'TLT', 'source': 'tiingo', 'spread_vs': None, 'name': 'Duration', 'category': 'macro'},
        }
    },
    'US_EXTENDED': {
        'name': 'US Extended Factor Model',
        'description': 'Extended factor model with growth, dividend, and macro factors',
        'factors_config': {
            'MKT': {'symbol': 'SPY', 'source': 'tiingo', 'spread_vs': None, 'name': 'Market', 'category': 'style'},
            'SIZE': {'symbol': 'IWM', 'source': 'tiingo', 'spread_vs': 'SPY', 'name': 'Size', 'category': 'style'},
            'GROWTH_VALUE': {'symbol': 'IWF', 'source': 'tiingo', 'spread_vs': 'IWD', 'name': 'Growth / Value', 'category': 'style'},
            'MOM': {'symbol': 'MTUM', 'source': 'tiingo', 'spread_vs': 'SPY', 'name': 'Momentum', 'category': 'style'},
            'QUAL': {'symbol': 'QUAL', 'source': 'tiingo', 'spread_vs': 'SPY', 'name': 'Quality', 'category': 'style'},
            'LOWVOL': {'symbol': 'SPLV', 'source': 'tiingo', 'spread_vs': 'SPY', 'name': 'Low Volatility', 'category': 'style'},
            'DIVYLD': {'symbol': 'DVY', 'source': 'tiingo', 'spread_vs': 'SPY', 'name': 'Dividend Yield', 'category': 'style'},
            'DURATION': {'symbol': 'TLT', 'source': 'tiingo', 'spread_vs': None, 'name': 'Duration', 'category': 'macro'},
        }
    }
}

# Benchmark configurations
BENCHMARK_CONFIGS = {
    'SPY': {'symbol': 'SPY', 'name': 'S&P 500'},
    'QQQ': {'symbol': 'QQQ', 'name': 'Nasdaq 100'},
    'IWM': {'symbol': 'IWM', 'name': 'Russell 2000'},
    'ACWI': {'symbol': 'ACWI', 'name': 'MSCI ACWI'},
}

# Risk-free rate from configuration (configurable via RISK_FREE_RATE_ANNUAL env var)
from app.core.config import settings as _settings
RISK_FREE_RATE_ANNUAL = _settings.RISK_FREE_RATE_ANNUAL


class FactorBenchmarkingService:
    """
    Main service for factor benchmarking and attribution.
    """

    def __init__(self, db: Session, fred_api_key: Optional[str] = None, tiingo_api_key: Optional[str] = None):
        self.db = db
        if fred_api_key is None:
            fred_api_key = os.environ.get('FRED_API_KEY')
        if tiingo_api_key is None:
            tiingo_api_key = os.environ.get('TIINGO_API_KEY')
        self.data_manager = DataProviderManager(fred_api_key=fred_api_key, tiingo_api_key=tiingo_api_key)

    def ensure_default_models(self):
        """Ensure default factor models exist in database and are up to date"""
        for code, config in DEFAULT_FACTOR_MODELS.items():
            existing = self.db.query(FactorModelDefinition).filter(
                FactorModelDefinition.code == code
            ).first()

            if existing:
                # Update existing model if config has changed
                import json
                existing_json = json.dumps(existing.factors_config, sort_keys=True)
                new_json = json.dumps(config['factors_config'], sort_keys=True)
                if existing_json != new_json:
                    existing.factors_config = config['factors_config']
                    existing.name = config['name']
                    existing.description = config.get('description', existing.description)
                    logger.info(f"Updated factor model {code} with new config")
            else:
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

        Optimizations:
        - Merges small ranges to reduce API calls
        - Returns broader ranges (minimum 7 days) to handle holiday gaps
        - Avoids single-date queries that might hit holidays
        """
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

        # Generate business days (excludes weekends)
        all_dates = pd.date_range(start=start_date, end=end_date, freq='B')
        all_dates_set = {d.date() for d in all_dates}
        missing_dates = sorted(all_dates_set - existing_set)

        if not missing_dates:
            return []

        # Build ranges, but merge small gaps and ensure minimum range size
        ranges = []
        range_start = missing_dates[0]
        prev_date = missing_dates[0]

        for d in missing_dates[1:]:
            gap_days = (d - prev_date).days
            # Only split if gap is more than 10 days (to avoid splitting around holidays)
            if gap_days > 10:
                ranges.append((range_start, prev_date))
                range_start = d
            prev_date = d

        ranges.append((range_start, prev_date))

        # Ensure minimum range size to avoid single-date holiday queries
        # Extend single-day or small ranges to at least 7 calendar days
        MIN_RANGE_DAYS = 7
        expanded_ranges = []
        for range_start, range_end in ranges:
            range_days = (range_end - range_start).days
            if range_days < MIN_RANGE_DAYS:
                # Expand range symmetrically
                expand_by = (MIN_RANGE_DAYS - range_days) // 2 + 1
                new_start = range_start - timedelta(days=expand_by)
                new_end = range_end + timedelta(days=expand_by)
                # Clamp to original bounds
                new_start = max(new_start, start_date)
                new_end = min(new_end, end_date)
                expanded_ranges.append((new_start, new_end))
            else:
                expanded_ranges.append((range_start, range_end))

        # Merge overlapping ranges
        if not expanded_ranges:
            return []

        expanded_ranges.sort()
        merged = [expanded_ranges[0]]
        for current_start, current_end in expanded_ranges[1:]:
            last_start, last_end = merged[-1]
            if current_start <= last_end + timedelta(days=5):  # Merge if within 5 days
                merged[-1] = (last_start, max(last_end, current_end))
            else:
                merged.append((current_start, current_end))

        return merged

    def _cache_series_data(self, symbol: str, source: str, df: pd.DataFrame):
        """Cache fetched data to database with proper error handling"""
        try:
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
        except Exception as e:
            logger.warning(f"Failed to cache data for {symbol}: {e}")
            self.db.rollback()

    def refresh_factor_data(
        self,
        model_code: str,
        start_date: date,
        end_date: date
    ) -> Dict[str, int]:
        """Refresh factor proxy data for a model. Only fetches missing dates."""
        model = self.get_factor_model(model_code)
        if not model:
            raise ValueError(f"Factor model {model_code} not found")

        results = {}
        all_symbols = set()

        for factor_name, config in model.factors_config.items():
            all_symbols.add(config['symbol'])
            if config.get('spread_vs'):
                all_symbols.add(config['spread_vs'])

        # Also add benchmark symbols
        for bm_config in BENCHMARK_CONFIGS.values():
            all_symbols.add(bm_config['symbol'])

        for symbol in all_symbols:
            source = 'tiingo'  # Primary source
            try:
                missing_ranges = self._get_missing_date_ranges(symbol, source, start_date, end_date)

                rows_fetched = 0
                for range_start, range_end in missing_ranges:
                    try:
                        df, used_source = self.data_manager.fetch_etf_data(symbol, range_start, range_end)
                        if df is not None and len(df) > 0:
                            self._cache_series_data(symbol, used_source, df)
                            rows_fetched += len(df)
                    except Exception as e:
                        logger.warning(f"Failed to fetch data for {symbol} ({range_start} to {range_end}): {e}")
                        continue

                results[symbol] = rows_fetched
            except Exception as e:
                logger.warning(f"Error processing {symbol}: {e}")
                results[symbol] = 0

        return results

    def get_factor_returns(
        self,
        model_code: str,
        start_date: date,
        end_date: date
    ) -> pd.DataFrame:
        """Get factor returns for a model over a date range."""
        model = self.get_factor_model(model_code)
        if not model:
            raise ValueError(f"Factor model {model_code} not found")

        all_symbols = set()
        for config in model.factors_config.values():
            all_symbols.add(config['symbol'])
            if config.get('spread_vs'):
                all_symbols.add(config['spread_vs'])

        symbol_data = {}
        for symbol in all_symbols:
            df = self._get_cached_data(symbol, 'tiingo', start_date, end_date)
            if len(df) > 0:
                symbol_data[symbol] = df.set_index('date')['daily_return']

        if not symbol_data:
            return pd.DataFrame()

        factor_returns = {}
        for factor_name, config in model.factors_config.items():
            symbol = config['symbol']
            spread_vs = config.get('spread_vs')

            if symbol not in symbol_data:
                continue

            if spread_vs and spread_vs in symbol_data:
                factor_returns[factor_name] = symbol_data[symbol] - symbol_data[spread_vs]
            else:
                factor_returns[factor_name] = symbol_data[symbol]

        df = pd.DataFrame(factor_returns)
        df = df.dropna()
        return df

    def get_benchmark_returns(
        self,
        benchmark_code: str,
        start_date: date,
        end_date: date
    ) -> pd.Series:
        """Get benchmark returns for a date range."""
        if benchmark_code not in BENCHMARK_CONFIGS:
            raise ValueError(f"Unknown benchmark: {benchmark_code}")

        symbol = BENCHMARK_CONFIGS[benchmark_code]['symbol']
        df = self._get_cached_data(symbol, 'tiingo', start_date, end_date)

        if len(df) > 0:
            return df.set_index('date')['daily_return']
        return pd.Series(dtype=float)

    def _compute_vif(self, X: np.ndarray, factor_names: List[str]) -> Dict[str, float]:
        """Compute Variance Inflation Factor for each factor."""
        vif_data = {}
        if X.shape[1] < 2:
            for name in factor_names:
                vif_data[name] = 1.0
            return vif_data

        for i, name in enumerate(factor_names):
            try:
                vif_data[name] = float(variance_inflation_factor(X, i))
            except Exception:
                vif_data[name] = None  # Use None instead of np.nan for JSON compatibility
        return vif_data

    def _compute_factor_correlations(self, factor_df: pd.DataFrame) -> Dict:
        """Compute correlation matrix for factors."""
        corr_matrix = factor_df.corr()
        # Convert numpy types to Python native types for JSON serialization
        matrix_dict = {}
        for col in corr_matrix.columns:
            matrix_dict[col] = {
                row: float(corr_matrix.loc[row, col]) if not pd.isna(corr_matrix.loc[row, col]) else None
                for row in corr_matrix.index
            }
        return {
            'matrix': matrix_dict,
            'factors': list(corr_matrix.columns)
        }

    def _detect_outliers(
        self,
        returns: np.ndarray,
        dates: List[date],
        threshold_pct: float = 0.10
    ) -> List[Dict]:
        """Detect outlier days (|return| > threshold)."""
        outliers = []
        for i, (ret, dt) in enumerate(zip(returns, dates)):
            if abs(ret) > threshold_pct:
                outliers.append({
                    'date': str(dt),
                    'return': float(ret * 100),
                    'z_score': float((ret - np.mean(returns)) / np.std(returns))
                })

        # Sort by absolute return, take top 10
        outliers.sort(key=lambda x: abs(x['return']), reverse=True)
        return outliers[:10]

    def _winsorize_returns(self, returns: np.ndarray, percentile: float = 0.025) -> np.ndarray:
        """Winsorize returns at given percentile."""
        lower = np.percentile(returns, percentile * 100)
        upper = np.percentile(returns, (1 - percentile) * 100)
        return np.clip(returns, lower, upper)

    def _compute_residual_diagnostics(
        self,
        residuals: np.ndarray,
        X: np.ndarray
    ) -> Dict:
        """Compute residual diagnostics."""
        diagnostics = {}

        # Durbin-Watson for autocorrelation
        dw_stat = float(durbin_watson(residuals))
        diagnostics['durbin_watson'] = dw_stat
        if dw_stat < 1.5:
            diagnostics['dw_interpretation'] = 'Positive autocorrelation detected'
        elif dw_stat > 2.5:
            diagnostics['dw_interpretation'] = 'Negative autocorrelation detected'
        else:
            diagnostics['dw_interpretation'] = 'No significant autocorrelation'

        # Jarque-Bera for normality
        try:
            jb_stat, jb_pvalue = jarque_bera(residuals)
            diagnostics['jarque_bera_stat'] = float(jb_stat)
            diagnostics['jarque_bera_pvalue'] = float(jb_pvalue)
            diagnostics['normality_ok'] = bool(jb_pvalue > 0.05)
        except Exception:
            diagnostics['normality_ok'] = None

        # Breusch-Pagan for heteroskedasticity
        try:
            X_with_const = sm.add_constant(X)
            bp_stat, bp_pvalue, _, _ = het_breuschpagan(residuals, X_with_const)
            diagnostics['breusch_pagan_stat'] = float(bp_stat)
            diagnostics['breusch_pagan_pvalue'] = float(bp_pvalue)
            diagnostics['homoskedasticity_ok'] = bool(bp_pvalue > 0.05)
        except Exception:
            diagnostics['homoskedasticity_ok'] = None

        # Residual stats
        diagnostics['residual_mean'] = float(np.mean(residuals))
        diagnostics['residual_std'] = float(np.std(residuals))
        diagnostics['residual_skew'] = float(stats.skew(residuals))
        diagnostics['residual_kurtosis'] = float(stats.kurtosis(residuals))

        return diagnostics

    def run_factor_regression(
        self,
        view_type: ViewType,
        view_id: int,
        model_code: str,
        start_date: date,
        end_date: date,
        use_excess_returns: bool = False,
        use_robust: bool = False,
        benchmark_code: Optional[str] = None
    ) -> Optional[Dict]:
        """
        Run factor regression for a portfolio with full diagnostics.
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

        # Align dates — inner join matches portfolio and factor return dates
        pre_merge_port = len(port_df)
        pre_merge_factor = len(factor_df)
        merged = port_df.join(factor_df, how='inner')

        pre_dropna = len(merged)
        merged = merged.dropna()

        if pre_merge_port - len(merged) > 0 or pre_merge_factor - len(merged) > 0:
            logger.info(
                f"Factor regression data alignment: portfolio={pre_merge_port}, factors={pre_merge_factor}, "
                f"joined={pre_dropna}, after_dropna={len(merged)} "
                f"(dropped {pre_merge_port - len(merged)} portfolio / "
                f"{pre_merge_factor - len(merged)} factor dates)"
            )

        if len(merged) < 30:
            logger.warning(f"Insufficient aligned data points: {len(merged)}")
            return None

        # Calculate risk-free rate (daily)
        rf_daily = RISK_FREE_RATE_ANNUAL / 252

        # Prepare returns
        y = merged['portfolio_return'].values.copy()

        if use_excess_returns:
            y = y - rf_daily

        factor_names = [c for c in merged.columns if c != 'portfolio_return']
        X = merged[factor_names].values.copy()

        # Winsorize if robust mode
        if use_robust:
            y = self._winsorize_returns(y, 0.025)
            for i in range(X.shape[1]):
                X[:, i] = self._winsorize_returns(X[:, i], 0.025)

        # Detect outliers before regression
        outliers = self._detect_outliers(y, list(merged.index))

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

            # Confidence intervals (95%)
            conf_int = results.conf_int(alpha=0.05)
            # Handle both DataFrame and numpy array returns
            if hasattr(conf_int, 'iloc'):
                # DataFrame case
                beta_ci = {
                    factor: {
                        'lower': float(conf_int.iloc[i+1, 0]),
                        'upper': float(conf_int.iloc[i+1, 1])
                    }
                    for i, factor in enumerate(factor_names)
                }
                alpha_ci = {
                    'lower': float(conf_int.iloc[0, 0]) * 252,
                    'upper': float(conf_int.iloc[0, 1]) * 252
                }
            else:
                # Numpy array case
                beta_ci = {
                    factor: {
                        'lower': float(conf_int[i+1, 0]),
                        'upper': float(conf_int[i+1, 1])
                    }
                    for i, factor in enumerate(factor_names)
                }
                alpha_ci = {
                    'lower': float(conf_int[0, 0]) * 252,
                    'upper': float(conf_int[0, 1]) * 252
                }

            # Standard errors
            std_errors = {
                factor: float(results.bse[i+1])
                for i, factor in enumerate(factor_names)
            }
            alpha_std_error = float(results.bse[0])

            residuals = results.resid
            residual_std = float(np.std(residuals))
            residual_std_ann = residual_std * np.sqrt(252)

            # VIF for multicollinearity
            vif = self._compute_vif(X, factor_names)
            vif_values = [v for v in vif.values() if v is not None]
            max_vif = float(max(vif_values)) if vif_values else 0.0
            multicollinearity_warning = bool(max_vif > 5)
            multicollinearity_severe = bool(max_vif > 10)

            # Factor correlations
            factor_correlations = self._compute_factor_correlations(merged[factor_names])

            # Residual diagnostics
            residual_diagnostics = self._compute_residual_diagnostics(residuals, X)

            # Alpha Information Ratio
            alpha_ir = alpha_annualized / residual_std_ann if residual_std_ann > 0 else 0

            return {
                'betas': betas,
                'alpha_daily': alpha_daily,
                'alpha_annualized': alpha_annualized,
                'alpha_ci': alpha_ci,
                'alpha_std_error': alpha_std_error * 252,
                'alpha_ir': alpha_ir,
                'r_squared': float(results.rsquared),
                'adj_r_squared': float(results.rsquared_adj),
                't_stats': t_stats,
                'p_values': p_values,
                'beta_ci': beta_ci,
                'std_errors': std_errors,
                'vif': vif,
                'multicollinearity_warning': multicollinearity_warning,
                'multicollinearity_severe': multicollinearity_severe,
                'factor_correlations': factor_correlations,
                'residual_std': residual_std * 100,
                'residual_std_ann': residual_std_ann * 100,
                'residual_diagnostics': residual_diagnostics,
                'outliers': outliers,
                'n_observations': len(merged),
                'start_date': merged.index.min(),
                'end_date': merged.index.max(),
                'use_excess_returns': use_excess_returns,
                'use_robust': use_robust,
                'risk_free_rate_annual': RISK_FREE_RATE_ANNUAL,
            }

        except Exception as e:
            logger.error(f"Factor regression failed: {e}")
            return None

    def run_active_return_regression(
        self,
        view_type: ViewType,
        view_id: int,
        model_code: str,
        benchmark_code: str,
        start_date: date,
        end_date: date,
        use_robust: bool = False
    ) -> Optional[Dict]:
        """
        Run factor regression on ACTIVE returns (portfolio - benchmark).
        Used in benchmark-relative mode. Returns same shape as run_factor_regression()
        plus daily component series for attribution.

        Attribution convention: arithmetic sum of daily returns.
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
            logger.warning(f"Insufficient portfolio returns for active regression: {len(portfolio_returns) if portfolio_returns else 0}")
            return None

        port_df = pd.DataFrame([
            {'date': r.date, 'portfolio_return': r.twr_return}
            for r in portfolio_returns
        ]).set_index('date')

        # Get benchmark returns
        benchmark_returns = self.get_benchmark_returns(benchmark_code, start_date, end_date)
        if benchmark_returns.empty:
            logger.warning(f"No benchmark returns for {benchmark_code}")
            return None

        bench_df = pd.DataFrame({'benchmark_return': benchmark_returns})

        # Get factor returns
        factor_df = self.get_factor_returns(model_code, start_date, end_date)
        if factor_df.empty:
            logger.warning("No factor returns available for active regression")
            return None

        # Inner-join all three on date
        merged = port_df.join(bench_df, how='inner').join(factor_df, how='inner').dropna()

        if len(merged) < 30:
            logger.warning(f"Insufficient aligned data for active regression: {len(merged)}")
            return None

        # Compute daily active returns
        merged['active_return'] = merged['portfolio_return'] - merged['benchmark_return']

        # Prepare regression variables
        y = merged['active_return'].values.copy()
        factor_names = [c for c in merged.columns if c not in ['portfolio_return', 'benchmark_return', 'active_return']]
        X = merged[factor_names].values.copy()

        # Detect outliers before regression
        outliers = self._detect_outliers(y, list(merged.index))

        # Add constant for alpha
        X_with_const = sm.add_constant(X)

        try:
            model = sm.OLS(y, X_with_const)

            if use_robust:
                # HAC (Newey-West) standard errors for robust inference
                results = model.fit(cov_type='HAC', cov_kwds={'maxlags': int(len(y) ** (1/3))})
            else:
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

            # Confidence intervals (95%)
            conf_int = results.conf_int(alpha=0.05)
            if hasattr(conf_int, 'iloc'):
                beta_ci = {
                    factor: {
                        'lower': float(conf_int.iloc[i+1, 0]),
                        'upper': float(conf_int.iloc[i+1, 1])
                    }
                    for i, factor in enumerate(factor_names)
                }
                alpha_ci = {
                    'lower': float(conf_int.iloc[0, 0]) * 252,
                    'upper': float(conf_int.iloc[0, 1]) * 252
                }
            else:
                beta_ci = {
                    factor: {
                        'lower': float(conf_int[i+1, 0]),
                        'upper': float(conf_int[i+1, 1])
                    }
                    for i, factor in enumerate(factor_names)
                }
                alpha_ci = {
                    'lower': float(conf_int[0, 0]) * 252,
                    'upper': float(conf_int[0, 1]) * 252
                }

            # Standard errors
            std_errors = {
                factor: float(results.bse[i+1])
                for i, factor in enumerate(factor_names)
            }
            alpha_std_error = float(results.bse[0])

            residuals = results.resid
            residual_std = float(np.std(residuals))
            residual_std_ann = residual_std * np.sqrt(252)

            # VIF for multicollinearity
            vif = self._compute_vif(X, factor_names)
            vif_values = [v for v in vif.values() if v is not None]
            max_vif = float(max(vif_values)) if vif_values else 0.0
            multicollinearity_warning = bool(max_vif > 5)
            multicollinearity_severe = bool(max_vif > 10)

            # Factor correlations
            factor_correlations = self._compute_factor_correlations(merged[factor_names])

            # Residual diagnostics
            residual_diagnostics = self._compute_residual_diagnostics(residuals, X)

            # Alpha Information Ratio (using active regression residual vol)
            alpha_ir = alpha_annualized / residual_std_ann if residual_std_ann > 0 else 0

            # Build daily component series for attribution
            daily_factor_contribs = {}
            for i, factor in enumerate(factor_names):
                daily_factor_contribs[factor] = merged[factor].values * betas[factor]

            daily_residuals = residuals

            # Tracking error = annualized std of daily active returns
            tracking_error_ann = float(np.std(merged['active_return'].values)) * np.sqrt(252)

            return {
                'betas': betas,
                'alpha_daily': alpha_daily,
                'alpha_annualized': alpha_annualized,
                'alpha_ci': alpha_ci,
                'alpha_std_error': alpha_std_error * 252,
                'alpha_ir': alpha_ir,
                'r_squared': float(results.rsquared),
                'adj_r_squared': float(results.rsquared_adj),
                't_stats': t_stats,
                'p_values': p_values,
                'beta_ci': beta_ci,
                'std_errors': std_errors,
                'vif': vif,
                'multicollinearity_warning': multicollinearity_warning,
                'multicollinearity_severe': multicollinearity_severe,
                'factor_correlations': factor_correlations,
                'residual_std': residual_std * 100,
                'residual_std_ann': residual_std_ann * 100,
                'residual_diagnostics': residual_diagnostics,
                'outliers': outliers,
                'n_observations': len(merged),
                'start_date': merged.index.min(),
                'end_date': merged.index.max(),
                'use_robust': use_robust,
                'tracking_error_ann': tracking_error_ann,
                # Daily component series for attribution
                'daily_active_returns': merged['active_return'].values,
                'daily_portfolio_returns': merged['portfolio_return'].values,
                'daily_benchmark_returns': merged['benchmark_return'].values,
                'daily_factor_contribs': daily_factor_contribs,
                'daily_alpha': alpha_daily,
                'daily_residuals': np.array(daily_residuals),
                'factor_names': factor_names,
                'factor_df': merged[factor_names],
            }

        except Exception as e:
            logger.error(f"Active return regression failed: {e}")
            return None

    def compute_rolling_analysis(
        self,
        view_type: ViewType,
        view_id: int,
        model_code: str,
        start_date: date,
        end_date: date,
        window_days: int = 63,
        use_excess_returns: bool = False
    ) -> Optional[Dict]:
        """
        Compute rolling betas, alpha, R², and tracking error.
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

        if not portfolio_returns or len(portfolio_returns) < window_days + 10:
            return None

        port_df = pd.DataFrame([
            {'date': r.date, 'portfolio_return': r.twr_return}
            for r in portfolio_returns
        ]).set_index('date')

        factor_df = self.get_factor_returns(model_code, start_date, end_date)
        if factor_df.empty:
            return None

        merged = port_df.join(factor_df, how='inner').dropna()
        if len(merged) < window_days + 10:
            return None

        rf_daily = RISK_FREE_RATE_ANNUAL / 252 if use_excess_returns else 0
        factor_names = [c for c in merged.columns if c != 'portfolio_return']

        rolling_data = []
        dates = list(merged.index)

        for i in range(window_days, len(merged)):
            window_data = merged.iloc[i-window_days:i]
            y = window_data['portfolio_return'].values - rf_daily
            X = window_data[factor_names].values
            X_with_const = sm.add_constant(X)

            try:
                model = sm.OLS(y, X_with_const)
                results = model.fit()

                betas = {f: float(results.params[j+1]) for j, f in enumerate(factor_names)}

                rolling_data.append({
                    'date': str(dates[i]),
                    'alpha_ann': float(results.params[0]) * 252 * 100,
                    'r_squared': float(results.rsquared),
                    'residual_vol_ann': float(np.std(results.resid)) * np.sqrt(252) * 100,
                    **{f'beta_{f}': b for f, b in betas.items()}
                })
            except Exception:
                continue

        return {
            'rolling_data': rolling_data,
            'window_days': window_days,
            'factors': factor_names
        }

    def compute_contribution_over_time(
        self,
        view_type: ViewType,
        view_id: int,
        model_code: str,
        start_date: date,
        end_date: date,
        frequency: str = 'M',  # M=monthly, Q=quarterly
        use_excess_returns: bool = False
    ) -> Optional[Dict]:
        """
        Compute factor contributions over time periods.
        """
        # First run the full regression to get betas
        reg_results = self.run_factor_regression(
            view_type, view_id, model_code, start_date, end_date,
            use_excess_returns=use_excess_returns
        )
        if not reg_results:
            return None

        # Get daily data
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

        if len(merged) < 20:
            return None

        # Apply excess returns if needed
        rf_daily = RISK_FREE_RATE_ANNUAL / 252 if use_excess_returns else 0
        merged['portfolio_return'] = merged['portfolio_return'] - rf_daily

        # Group by period
        merged['period'] = pd.to_datetime(merged.index).to_period(frequency)
        factor_names = [c for c in merged.columns if c not in ['portfolio_return', 'period']]

        contributions_by_period = []

        for period, group in merged.groupby('period'):
            # Calculate period returns (cumulative)
            period_portfolio_return = (1 + group['portfolio_return']).prod() - 1

            period_data = {
                'period': str(period),
                'period_start': str(group.index.min()),
                'period_end': str(group.index.max()),
                'portfolio_return': float(period_portfolio_return * 100),
                'factor_contributions': {}
            }

            # Factor contributions using betas from full regression
            total_factor_contribution = 0
            for factor in factor_names:
                factor_period_return = (1 + group[factor]).prod() - 1
                contribution = reg_results['betas'][factor] * factor_period_return
                period_data['factor_contributions'][factor] = float(contribution * 100)
                total_factor_contribution += contribution

            # Alpha contribution (daily alpha * days in period)
            alpha_contribution = reg_results['alpha_daily'] * len(group)
            period_data['alpha_contribution'] = float(alpha_contribution * 100)

            # Residual
            explained = total_factor_contribution + alpha_contribution
            period_data['residual'] = float((period_portfolio_return - explained) * 100)
            period_data['factor_explained'] = float(total_factor_contribution * 100)

            contributions_by_period.append(period_data)

        # Calculate cumulative contributions
        cumulative_portfolio = 0
        cumulative_factor = 0
        cumulative_alpha = 0

        for period_data in contributions_by_period:
            cumulative_portfolio += period_data['portfolio_return']
            cumulative_factor += period_data['factor_explained']
            cumulative_alpha += period_data['alpha_contribution']

            period_data['cumulative_portfolio'] = cumulative_portfolio
            period_data['cumulative_factor_explained'] = cumulative_factor
            period_data['cumulative_alpha'] = cumulative_alpha

        return {
            'periods': contributions_by_period,
            'frequency': frequency,
            'factors': factor_names,
            'betas_used': reg_results['betas']
        }

    def compute_attribution(
        self,
        view_type: ViewType,
        view_id: int,
        model_code: str,
        start_date: date,
        end_date: date,
        use_excess_returns: bool = False,
        use_robust: bool = False,
        benchmark_code: Optional[str] = None,
        rolling_window: int = 63
    ) -> Optional[Dict]:
        """
        Compute comprehensive return attribution.
        Branches into absolute mode (no benchmark) or benchmark-relative mode.
        """
        if benchmark_code:
            return self._compute_active_attribution(
                view_type, view_id, model_code, benchmark_code,
                start_date, end_date, use_robust=use_robust
            )
        else:
            return self._compute_absolute_attribution(
                view_type, view_id, model_code,
                start_date, end_date,
                use_excess_returns=use_excess_returns,
                use_robust=use_robust
            )

    def _compute_absolute_attribution(
        self,
        view_type: ViewType,
        view_id: int,
        model_code: str,
        start_date: date,
        end_date: date,
        use_excess_returns: bool = False,
        use_robust: bool = False
    ) -> Optional[Dict]:
        """
        Absolute mode: attribute portfolio return to factors + alpha + residual.
        Uses arithmetic daily return aggregation.
        """
        # Run regression
        reg_results = self.run_factor_regression(
            view_type, view_id, model_code, start_date, end_date,
            use_excess_returns=use_excess_returns,
            use_robust=use_robust
        )

        if not reg_results:
            return None

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

        # Apply risk-free adjustment
        rf_daily = RISK_FREE_RATE_ANNUAL / 252 if use_excess_returns else 0
        merged['portfolio_return'] = merged['portfolio_return'] - rf_daily

        # Compute cumulative returns (arithmetic sum of daily)
        total_return = float(merged['portfolio_return'].sum())

        # Compute factor contributions from daily fitted components
        factor_contributions = {}
        factor_cum_returns = {}
        total_factor_contribution = 0

        for factor_name in reg_results['betas'].keys():
            if factor_name in merged.columns:
                # Daily factor contribution = beta * factor_return_t, then sum
                daily_contrib = merged[factor_name].values * reg_results['betas'][factor_name]
                contribution = float(daily_contrib.sum())
                cum_factor_return = float(merged[factor_name].sum())
                factor_cum_returns[factor_name] = cum_factor_return
                factor_contributions[factor_name] = contribution
                total_factor_contribution += contribution

        trading_days = len(merged)
        alpha_contribution = reg_results['alpha_daily'] * trading_days
        residual_contribution = total_return - total_factor_contribution - alpha_contribution

        # Percentage of return — safeguard against near-zero denominator
        denominator = abs(total_return) if abs(total_return) > 0.0001 else None
        if denominator is not None:
            factor_pct = {f: c / denominator * 100 for f, c in factor_contributions.items()}
            alpha_pct = alpha_contribution / denominator * 100
            residual_pct = residual_contribution / denominator * 100
            factor_explained_pct = total_factor_contribution / denominator * 100
        else:
            factor_pct = {f: None for f in factor_contributions}
            alpha_pct = None
            residual_pct = None
            factor_explained_pct = None

        # Factor display names
        factor_names_display = {}
        if model:
            for f_name, config in model.factors_config.items():
                factor_names_display[f_name] = config.get('name', f_name)

        # Build result
        result = {
            'mode': 'absolute',
            'total_return': total_return,
            'total_return_pct': float(total_return * 100),
            'use_excess_returns': use_excess_returns,
            'use_robust': use_robust,
            'risk_free_rate_annual': RISK_FREE_RATE_ANNUAL * 100,
            'factor_contributions': {
                f: {
                    'name': factor_names_display.get(f, f),
                    'category': model.factors_config.get(f, {}).get('category', 'style') if model else 'style',
                    'beta': reg_results['betas'].get(f, 0),
                    'beta_ci': reg_results['beta_ci'].get(f, {}),
                    'factor_return': factor_cum_returns.get(f, 0) * 100,
                    'contribution': c * 100,
                    'contribution_pct': factor_pct.get(f),
                    't_stat': reg_results['t_stats'].get(f, 0),
                    'p_value': reg_results['p_values'].get(f, 0),
                    'vif': reg_results['vif'].get(f, 1),
                }
                for f, c in factor_contributions.items()
            },
            'factor_explained': total_factor_contribution * 100,
            'factor_explained_pct': factor_explained_pct,
            'alpha_contribution': alpha_contribution * 100,
            'alpha_contribution_pct': alpha_pct,
            'residual_contribution': residual_contribution * 100,
            'residual_contribution_pct': residual_pct,
            'regression': {
                'r_squared': reg_results['r_squared'],
                'adj_r_squared': reg_results['adj_r_squared'],
                'alpha_daily': reg_results['alpha_daily'] * 100,
                'alpha_annualized': reg_results['alpha_annualized'] * 100,
                'alpha_ci': {
                    'lower': reg_results['alpha_ci']['lower'] * 100,
                    'upper': reg_results['alpha_ci']['upper'] * 100,
                },
                'alpha_ir': reg_results['alpha_ir'],
                'residual_std': reg_results['residual_std'],
                'residual_std_ann': reg_results['residual_std_ann'],
                'durbin_watson': reg_results['residual_diagnostics']['durbin_watson'],
                'dw_interpretation': reg_results['residual_diagnostics']['dw_interpretation'],
                'n_observations': reg_results['n_observations'],
            },
            'diagnostics': {
                'vif': reg_results['vif'],
                'max_vif': max(reg_results['vif'].values()) if reg_results['vif'] else 0,
                'multicollinearity_warning': reg_results['multicollinearity_warning'],
                'multicollinearity_severe': reg_results['multicollinearity_severe'],
                'factor_correlations': reg_results['factor_correlations'],
                'residual_diagnostics': reg_results['residual_diagnostics'],
                'outliers': reg_results['outliers'],
                'outlier_count': len(reg_results['outliers']),
            },
            'period': {
                'start_date': str(reg_results['start_date']),
                'end_date': str(reg_results['end_date']),
                'trading_days': trading_days,
            },
            'warnings': []
        }

        # Generate warnings
        if reg_results['n_observations'] < 60:
            result['warnings'].append({
                'type': 'low_observations',
                'message': f"Only {reg_results['n_observations']} observations. Results may be unreliable.",
                'severity': 'warning'
            })

        if reg_results['multicollinearity_severe']:
            result['warnings'].append({
                'type': 'multicollinearity',
                'message': 'Severe multicollinearity detected (VIF > 10). Betas may be unstable.',
                'severity': 'error'
            })
        elif reg_results['multicollinearity_warning']:
            result['warnings'].append({
                'type': 'multicollinearity',
                'message': 'Moderate multicollinearity detected (VIF > 5). Interpret betas with caution.',
                'severity': 'warning'
            })

        if len(reg_results['outliers']) > 5:
            result['warnings'].append({
                'type': 'outliers',
                'message': f"{len(reg_results['outliers'])} extreme return days detected. Consider using robust mode.",
                'severity': 'warning'
            })

        dw = reg_results['residual_diagnostics']['durbin_watson']
        if dw < 1.5 or dw > 2.5:
            result['warnings'].append({
                'type': 'autocorrelation',
                'message': f"Durbin-Watson = {dw:.2f}. Residual autocorrelation may affect standard errors.",
                'severity': 'warning'
            })

        return result

    def _compute_active_attribution(
        self,
        view_type: ViewType,
        view_id: int,
        model_code: str,
        benchmark_code: str,
        start_date: date,
        end_date: date,
        use_robust: bool = False
    ) -> Optional[Dict]:
        """
        Benchmark-relative mode: attribute active return (portfolio - benchmark) to factors.
        Uses a single direct active-return regression as the source of truth.
        All attribution uses arithmetic sum of daily returns.

        Side regressions for portfolio_beta and benchmark_beta are display-only helpers.
        """
        # Run the active return regression (source of truth)
        active_reg = self.run_active_return_regression(
            view_type, view_id, model_code, benchmark_code,
            start_date, end_date, use_robust=use_robust
        )

        if not active_reg:
            return None

        model = self.get_factor_model(model_code)
        factor_names = active_reg['factor_names']

        # ─── Side regressions for display-only portfolio/benchmark betas ───
        port_reg = self.run_factor_regression(
            view_type, view_id, model_code, start_date, end_date,
            use_excess_returns=False, use_robust=False
        )
        portfolio_betas = port_reg['betas'] if port_reg else {}

        # Benchmark regression
        benchmark_betas = {}
        try:
            benchmark_returns = self.get_benchmark_returns(benchmark_code, start_date, end_date)
            factor_df = self.get_factor_returns(model_code, start_date, end_date)
            if not benchmark_returns.empty and not factor_df.empty:
                bench_df = pd.DataFrame({'benchmark_return': benchmark_returns})
                merged_bench = bench_df.join(factor_df, how='inner').dropna()
                if len(merged_bench) >= 30:
                    y_bench = merged_bench['benchmark_return'].values
                    bench_factor_names = [c for c in merged_bench.columns if c != 'benchmark_return']
                    X_bench = merged_bench[bench_factor_names].values
                    X_bench_const = sm.add_constant(X_bench)
                    bench_model = sm.OLS(y_bench, X_bench_const)
                    bench_results = bench_model.fit()
                    benchmark_betas = {
                        f: float(bench_results.params[i+1])
                        for i, f in enumerate(bench_factor_names)
                    }
        except Exception as e:
            logger.warning(f"Benchmark regression for display failed: {e}")

        # ─── Build attribution from daily model components ───
        daily_active = active_reg['daily_active_returns']
        daily_factor_contribs = active_reg['daily_factor_contribs']
        daily_portfolio = active_reg['daily_portfolio_returns']
        daily_benchmark = active_reg['daily_benchmark_returns']
        daily_residuals = active_reg['daily_residuals']
        factor_data = active_reg['factor_df']

        trading_days = len(daily_active)

        # Arithmetic sum of daily returns
        total_active = float(np.sum(daily_active))
        total_portfolio = float(np.sum(daily_portfolio))
        total_benchmark = float(np.sum(daily_benchmark))

        # Per-factor contributions (arithmetic sum of daily fitted components)
        factor_contributions = {}
        factor_cum_returns = {}
        total_factor_contribution = 0.0

        for factor in factor_names:
            # Sum of daily (beta * factor_return_t)
            contribution = float(np.sum(daily_factor_contribs[factor]))
            # Sum of daily factor returns
            cum_factor_return = float(factor_data[factor].sum())
            factor_contributions[factor] = contribution
            factor_cum_returns[factor] = cum_factor_return
            total_factor_contribution += contribution

        # Alpha contribution = alpha_daily * trading_days
        alpha_contribution = active_reg['alpha_daily'] * trading_days

        # Residual = sum of daily residuals
        residual_contribution = float(np.sum(daily_residuals))

        # Verify reconciliation (should be exact)
        recon_check = total_factor_contribution + alpha_contribution + residual_contribution
        recon_diff = total_active - recon_check
        if abs(recon_diff) > 1e-10:
            logger.warning(f"Active attribution reconciliation drift: {recon_diff:.2e}")

        # % of active return — safeguard against near-zero denominator
        denominator = abs(total_active) if abs(total_active) > 0.0001 else None
        if denominator is not None:
            factor_pct = {f: c / denominator * 100 for f, c in factor_contributions.items()}
            alpha_pct = alpha_contribution / denominator * 100
            residual_pct = residual_contribution / denominator * 100
            factor_explained_pct = total_factor_contribution / denominator * 100
        else:
            factor_pct = {f: None for f in factor_contributions}
            alpha_pct = None
            residual_pct = None
            factor_explained_pct = None

        # Factor display names (with "(Active)" for market)
        factor_names_display = {}
        if model:
            for f_name, config in model.factors_config.items():
                display_name = config.get('name', f_name)
                if f_name == 'MKT':
                    display_name = 'Market (Active)'
                factor_names_display[f_name] = display_name

        # Build result
        result = {
            'mode': 'benchmark_relative',
            'benchmark_code': benchmark_code,
            'benchmark_name': BENCHMARK_CONFIGS.get(benchmark_code, {}).get('name', benchmark_code),
            # Arithmetic attribution-period returns
            'total_return': total_portfolio,
            'total_return_pct': float(total_portfolio * 100),
            'benchmark_return': total_benchmark,
            'benchmark_return_pct': float(total_benchmark * 100),
            'active_return': total_active,
            'active_return_pct': float(total_active * 100),
            'tracking_error': active_reg['tracking_error_ann'],
            'tracking_error_pct': float(active_reg['tracking_error_ann'] * 100),
            'use_excess_returns': False,  # always False in active mode
            'use_robust': use_robust,
            'risk_free_rate_annual': RISK_FREE_RATE_ANNUAL * 100,
            'factor_contributions': {
                f: {
                    'name': factor_names_display.get(f, f),
                    'category': model.factors_config.get(f, {}).get('category', 'style') if model else 'style',
                    'portfolio_beta': portfolio_betas.get(f, None),
                    'benchmark_beta': benchmark_betas.get(f, None),
                    'active_beta': active_reg['betas'].get(f, 0),
                    'beta': active_reg['betas'].get(f, 0),  # alias for backward compat
                    'beta_ci': active_reg['beta_ci'].get(f, {}),
                    'factor_return': factor_cum_returns.get(f, 0) * 100,
                    'contribution': factor_contributions[f] * 100,
                    'contribution_pct': factor_pct.get(f),
                    't_stat': active_reg['t_stats'].get(f, 0),
                    'p_value': active_reg['p_values'].get(f, 0),
                    'vif': active_reg['vif'].get(f, 1),
                }
                for f in factor_names if f in factor_contributions
            },
            'factor_explained': total_factor_contribution * 100,
            'factor_explained_pct': factor_explained_pct,
            'alpha_contribution': alpha_contribution * 100,
            'alpha_contribution_pct': alpha_pct,
            'residual_contribution': residual_contribution * 100,
            'residual_contribution_pct': residual_pct,
            'regression': {
                'r_squared': active_reg['r_squared'],
                'adj_r_squared': active_reg['adj_r_squared'],
                'alpha_daily': active_reg['alpha_daily'] * 100,
                'alpha_annualized': active_reg['alpha_annualized'] * 100,
                'alpha_ci': {
                    'lower': active_reg['alpha_ci']['lower'] * 100,
                    'upper': active_reg['alpha_ci']['upper'] * 100,
                },
                'alpha_ir': active_reg['alpha_ir'],
                'residual_std': active_reg['residual_std'],
                'residual_std_ann': active_reg['residual_std_ann'],
                'durbin_watson': active_reg['residual_diagnostics']['durbin_watson'],
                'dw_interpretation': active_reg['residual_diagnostics']['dw_interpretation'],
                'n_observations': active_reg['n_observations'],
            },
            'diagnostics': {
                'vif': active_reg['vif'],
                'max_vif': max(active_reg['vif'].values()) if active_reg['vif'] else 0,
                'multicollinearity_warning': active_reg['multicollinearity_warning'],
                'multicollinearity_severe': active_reg['multicollinearity_severe'],
                'factor_correlations': active_reg['factor_correlations'],
                'residual_diagnostics': active_reg['residual_diagnostics'],
                'outliers': active_reg['outliers'],
                'outlier_count': len(active_reg['outliers']),
            },
            'period': {
                'start_date': str(active_reg['start_date']),
                'end_date': str(active_reg['end_date']),
                'trading_days': trading_days,
            },
            'warnings': []
        }

        # Generate warnings
        if active_reg['n_observations'] < 60:
            result['warnings'].append({
                'type': 'low_observations',
                'message': f"Only {active_reg['n_observations']} observations. Results may be unreliable.",
                'severity': 'warning'
            })

        if active_reg['multicollinearity_severe']:
            result['warnings'].append({
                'type': 'multicollinearity',
                'message': 'Severe multicollinearity detected (VIF > 10). Active betas may be unstable.',
                'severity': 'error'
            })
        elif active_reg['multicollinearity_warning']:
            result['warnings'].append({
                'type': 'multicollinearity',
                'message': 'Moderate multicollinearity detected (VIF > 5). Interpret active betas with caution.',
                'severity': 'warning'
            })

        if len(active_reg['outliers']) > 5:
            result['warnings'].append({
                'type': 'outliers',
                'message': f"{len(active_reg['outliers'])} extreme active return days detected.",
                'severity': 'warning'
            })

        dw = active_reg['residual_diagnostics']['durbin_watson']
        if dw < 1.5 or dw > 2.5:
            result['warnings'].append({
                'type': 'autocorrelation',
                'message': f"Durbin-Watson = {dw:.2f}. Residual autocorrelation may affect standard errors.",
                'severity': 'warning'
            })

        return result

    def get_available_benchmarks(self) -> List[Dict]:
        """Get list of available benchmarks."""
        return [
            {'code': code, 'name': config['name'], 'symbol': config['symbol']}
            for code, config in BENCHMARK_CONFIGS.items()
        ]

    def check_factor_data_status(
        self,
        model_code: str,
        start_date: date,
        end_date: date
    ) -> Dict:
        """
        Check the availability and status of factor data.
        Returns detailed info about what data is available vs missing.
        """
        model = self.get_factor_model(model_code)
        if not model:
            return {
                'available': False,
                'error': f'Factor model {model_code} not found',
                'symbols': {}
            }

        all_symbols = set()
        for config in model.factors_config.values():
            all_symbols.add(config['symbol'])
            if config.get('spread_vs'):
                all_symbols.add(config['spread_vs'])

        symbol_status = {}
        total_expected_days = len(pd.bdate_range(start=start_date, end=end_date))

        for symbol in all_symbols:
            # Count available data points
            count = self.db.query(func.count(FactorProxySeries.id)).filter(
                and_(
                    FactorProxySeries.symbol == symbol,
                    FactorProxySeries.date >= start_date,
                    FactorProxySeries.date <= end_date
                )
            ).scalar() or 0

            # Get date range
            date_range = self.db.query(
                func.min(FactorProxySeries.date),
                func.max(FactorProxySeries.date)
            ).filter(
                and_(
                    FactorProxySeries.symbol == symbol,
                    FactorProxySeries.date >= start_date,
                    FactorProxySeries.date <= end_date
                )
            ).first()

            symbol_status[symbol] = {
                'available_days': count,
                'expected_days': total_expected_days,
                'coverage_pct': round(count / total_expected_days * 100, 1) if total_expected_days > 0 else 0,
                'earliest_date': str(date_range[0]) if date_range[0] else None,
                'latest_date': str(date_range[1]) if date_range[1] else None,
                'has_data': count > 0
            }

        # Overall status
        symbols_with_data = sum(1 for s in symbol_status.values() if s['has_data'])
        min_coverage = min((s['coverage_pct'] for s in symbol_status.values()), default=0)

        return {
            'available': symbols_with_data == len(all_symbols) and min_coverage > 50,
            'model_code': model_code,
            'symbols_total': len(all_symbols),
            'symbols_with_data': symbols_with_data,
            'min_coverage_pct': min_coverage,
            'period': {
                'start_date': str(start_date),
                'end_date': str(end_date),
                'expected_business_days': total_expected_days
            },
            'symbols': symbol_status,
            'message': self._generate_status_message(symbols_with_data, len(all_symbols), min_coverage)
        }

    def _generate_status_message(self, symbols_with_data: int, total_symbols: int, min_coverage: float) -> str:
        """Generate a human-readable status message."""
        if symbols_with_data == 0:
            return "No factor data available. External data sources may be unavailable. Try refreshing later."
        elif symbols_with_data < total_symbols:
            missing = total_symbols - symbols_with_data
            return f"Missing data for {missing} factor proxies. Results may be incomplete."
        elif min_coverage < 50:
            return f"Limited data coverage ({min_coverage:.0f}%). Results may be unreliable."
        elif min_coverage < 80:
            return f"Partial data coverage ({min_coverage:.0f}%). Some dates may be missing."
        else:
            return "Factor data available."
