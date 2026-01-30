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

# Benchmark configurations
BENCHMARK_CONFIGS = {
    'SPY': {'symbol': 'SPY', 'name': 'S&P 500'},
    'QQQ': {'symbol': 'QQQ', 'name': 'Nasdaq 100'},
    'IWM': {'symbol': 'IWM', 'name': 'Russell 2000'},
    'ACWI': {'symbol': 'ACWI', 'name': 'MSCI ACWI'},
}

# Risk-free rate proxy (we'll use 0 for now, could fetch from FRED)
RISK_FREE_RATE_ANNUAL = 0.05  # 5% annual, can be updated


class FactorBenchmarkingService:
    """
    Main service for factor benchmarking and attribution.
    """

    def __init__(self, db: Session, fred_api_key: Optional[str] = None):
        self.db = db
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
        """Determine which date ranges need to be fetched."""
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

        all_dates = pd.date_range(start=start_date, end=end_date, freq='B')
        all_dates_set = {d.date() for d in all_dates}
        missing_dates = sorted(all_dates_set - existing_set)

        if not missing_dates:
            return []

        ranges = []
        range_start = missing_dates[0]
        prev_date = missing_dates[0]

        for d in missing_dates[1:]:
            if (d - prev_date).days > 5:
                ranges.append((range_start, prev_date))
                range_start = d
            prev_date = d

        ranges.append((range_start, prev_date))
        return ranges

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
            source = 'stooq'
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
            df = self._get_cached_data(symbol, 'stooq', start_date, end_date)
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
        df = self._get_cached_data(symbol, 'stooq', start_date, end_date)

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
                vif_data[name] = np.nan
        return vif_data

    def _compute_factor_correlations(self, factor_df: pd.DataFrame) -> Dict:
        """Compute correlation matrix for factors."""
        corr_matrix = factor_df.corr()
        return {
            'matrix': corr_matrix.to_dict(),
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
            diagnostics['normality_ok'] = jb_pvalue > 0.05
        except Exception:
            diagnostics['normality_ok'] = None

        # Breusch-Pagan for heteroskedasticity
        try:
            X_with_const = sm.add_constant(X)
            bp_stat, bp_pvalue, _, _ = het_breuschpagan(residuals, X_with_const)
            diagnostics['breusch_pagan_stat'] = float(bp_stat)
            diagnostics['breusch_pagan_pvalue'] = float(bp_pvalue)
            diagnostics['homoskedasticity_ok'] = bp_pvalue > 0.05
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

        # Align dates
        merged = port_df.join(factor_df, how='inner')
        merged = merged.dropna()

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
            max_vif = max(vif.values()) if vif else 0
            multicollinearity_warning = max_vif > 5
            multicollinearity_severe = max_vif > 10

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

    def compute_benchmark_relative_attribution(
        self,
        view_type: ViewType,
        view_id: int,
        model_code: str,
        benchmark_code: str,
        start_date: date,
        end_date: date,
        use_excess_returns: bool = False
    ) -> Optional[Dict]:
        """
        Compute benchmark-relative (active) return attribution.
        """
        # Get portfolio regression results
        port_reg = self.run_factor_regression(
            view_type, view_id, model_code, start_date, end_date,
            use_excess_returns=use_excess_returns
        )
        if not port_reg:
            return None

        # Get benchmark returns
        benchmark_returns = self.get_benchmark_returns(benchmark_code, start_date, end_date)
        if benchmark_returns.empty:
            return None

        # Get factor returns
        factor_df = self.get_factor_returns(model_code, start_date, end_date)
        if factor_df.empty:
            return None

        # Align benchmark with factors
        bench_df = pd.DataFrame({'benchmark_return': benchmark_returns})
        merged_bench = bench_df.join(factor_df, how='inner').dropna()

        if len(merged_bench) < 30:
            return None

        # Run regression for benchmark
        rf_daily = RISK_FREE_RATE_ANNUAL / 252 if use_excess_returns else 0
        y_bench = merged_bench['benchmark_return'].values - rf_daily
        factor_names = [c for c in merged_bench.columns if c != 'benchmark_return']
        X_bench = merged_bench[factor_names].values
        X_bench_const = sm.add_constant(X_bench)

        try:
            bench_model = sm.OLS(y_bench, X_bench_const)
            bench_results = bench_model.fit()

            bench_betas = {
                f: float(bench_results.params[i+1])
                for i, f in enumerate(factor_names)
            }
            bench_alpha = float(bench_results.params[0]) * 252
        except Exception:
            return None

        # Calculate active betas
        active_betas = {
            f: port_reg['betas'].get(f, 0) - bench_betas.get(f, 0)
            for f in factor_names
        }

        # Get cumulative factor returns for the period
        factor_cum_returns = {}
        for f in factor_names:
            if f in factor_df.columns:
                factor_cum_returns[f] = float((1 + factor_df[f]).prod() - 1)

        # Active return decomposition
        # Active factor tilts contribution = sum(active_beta * factor_return)
        active_factor_contribution = {}
        total_active_factor = 0
        for f in factor_names:
            contrib = active_betas[f] * factor_cum_returns.get(f, 0)
            active_factor_contribution[f] = float(contrib * 100)
            total_active_factor += contrib

        # Get portfolio and benchmark cumulative returns
        # (need to recalculate to align periods)
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

        # Align all three
        combined = port_df.join(bench_df, how='inner').dropna()
        if use_excess_returns:
            combined['portfolio_return'] = combined['portfolio_return'] - rf_daily
            combined['benchmark_return'] = combined['benchmark_return'] - rf_daily

        total_portfolio_return = float((1 + combined['portfolio_return']).prod() - 1)
        total_benchmark_return = float((1 + combined['benchmark_return']).prod() - 1)
        active_return = total_portfolio_return - total_benchmark_return

        # Active alpha = portfolio alpha - benchmark alpha
        active_alpha = port_reg['alpha_annualized'] - bench_alpha

        # Active selection/residual
        active_selection = active_return - total_active_factor

        return {
            'portfolio_return': total_portfolio_return * 100,
            'benchmark_return': total_benchmark_return * 100,
            'active_return': active_return * 100,
            'benchmark_name': BENCHMARK_CONFIGS.get(benchmark_code, {}).get('name', benchmark_code),
            'portfolio_betas': port_reg['betas'],
            'benchmark_betas': bench_betas,
            'active_betas': active_betas,
            'active_factor_contributions': active_factor_contribution,
            'total_active_factor_contribution': total_active_factor * 100,
            'active_alpha': active_alpha,
            'active_selection': active_selection * 100,
            'factors': factor_names,
            'n_observations': len(combined),
            'period': {
                'start_date': str(combined.index.min()),
                'end_date': str(combined.index.max()),
            }
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
        Compute comprehensive return attribution with all enhancements.
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

        # Compute cumulative returns
        total_return = (1 + merged['portfolio_return']).prod() - 1

        # Compute factor contributions
        factor_contributions = {}
        factor_cum_returns = {}
        total_factor_contribution = 0

        for factor_name in reg_results['betas'].keys():
            if factor_name in merged.columns:
                cum_factor_return = (1 + merged[factor_name]).prod() - 1
                factor_cum_returns[factor_name] = cum_factor_return
                contribution = reg_results['betas'][factor_name] * cum_factor_return
                factor_contributions[factor_name] = contribution
                total_factor_contribution += contribution

        trading_days = len(merged)
        alpha_contribution = reg_results['alpha_daily'] * trading_days
        explained = total_factor_contribution + alpha_contribution
        residual_contribution = total_return - explained

        # Percentage of return
        denominator = abs(total_return) if abs(total_return) > 0.0001 else 1
        factor_pct = {f: c / denominator * 100 for f, c in factor_contributions.items()}
        alpha_pct = alpha_contribution / denominator * 100
        residual_pct = residual_contribution / denominator * 100
        factor_explained_pct = total_factor_contribution / denominator * 100

        # Factor display names
        factor_names_display = {}
        if model:
            for f_name, config in model.factors_config.items():
                factor_names_display[f_name] = config.get('name', f_name)

        # Build result
        result = {
            'total_return': float(total_return),
            'total_return_pct': float(total_return * 100),
            'use_excess_returns': use_excess_returns,
            'use_robust': use_robust,
            'risk_free_rate_annual': RISK_FREE_RATE_ANNUAL * 100,
            'factor_contributions': {
                f: {
                    'name': factor_names_display.get(f, f),
                    'beta': reg_results['betas'].get(f, 0),
                    'beta_ci': reg_results['beta_ci'].get(f, {}),
                    'factor_return': factor_cum_returns.get(f, 0) * 100,
                    'contribution': c * 100,
                    'contribution_pct': factor_pct.get(f, 0),
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

        # Rolling analysis (async-friendly: compute in separate call if needed)
        # For now, include basic rolling if requested

        # Benchmark-relative attribution
        if benchmark_code:
            bench_attr = self.compute_benchmark_relative_attribution(
                view_type, view_id, model_code, benchmark_code,
                start_date, end_date, use_excess_returns
            )
            if bench_attr:
                result['benchmark_attribution'] = bench_attr

        return result

    def get_available_benchmarks(self) -> List[Dict]:
        """Get list of available benchmarks."""
        return [
            {'code': code, 'name': config['name'], 'symbol': config['symbol']}
            for code, config in BENCHMARK_CONFIGS.items()
        ]
