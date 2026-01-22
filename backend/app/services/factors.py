import pandas as pd
import numpy as np
from typing import List, Dict, Optional
from datetime import date
from sqlalchemy.orm import Session
from sqlalchemy import and_
from sklearn.linear_model import LinearRegression
from app.models import (
    FactorSet, FactorReturn, FactorRegression,
    Security, PricesEOD, ReturnsEOD, ViewType
)
import logging

logger = logging.getLogger(__name__)


class FactorsEngine:
    """
    Implements STYLE7 factor analysis using ETF proxies.
    Factors: MKT, SIZE, VALUE, GROWTH, QUALITY, VOL, MOM
    """

    # Factor ETF mapping
    FACTOR_ETFS = {
        'SPY': 'Market (Beta)',
        'IWM': 'Size',
        'IVE': 'Value',
        'IVW': 'Growth',
        'QUAL': 'Quality',
        'SPLV': 'Low Volatility',
        'MTUM': 'Momentum'
    }

    FACTOR_NAMES = ['MKT', 'SIZE', 'VALUE', 'GROWTH', 'QUALITY', 'VOL', 'MOM']

    def __init__(self, db: Session):
        self.db = db

    def ensure_style7_factor_set(self):
        """Ensure STYLE7 factor set exists"""
        factor_set = self.db.query(FactorSet).filter(
            FactorSet.code == 'STYLE7'
        ).first()

        if not factor_set:
            factor_set = FactorSet(
                code='STYLE7',
                name='ETF Proxy Style Factors',
                factor_names=self.FACTOR_NAMES
            )
            self.db.add(factor_set)
            self.db.commit()

    def ensure_factor_etfs_exist(self) -> List[int]:
        """Ensure factor ETF securities exist"""
        security_ids = []

        for symbol in self.FACTOR_ETFS.keys():
            security = self.db.query(Security).filter(
                Security.symbol == symbol
            ).first()

            if not security:
                from app.models import AssetClass
                security = Security(
                    symbol=symbol,
                    asset_name=self.FACTOR_ETFS[symbol],
                    asset_class=AssetClass.ETF
                )
                self.db.add(security)
                self.db.flush()

            security_ids.append(security.id)

        self.db.commit()
        return security_ids

    def compute_factor_returns(
        self,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> int:
        """
        Compute daily factor returns.
        Factors are spreads vs SPY to reduce market collinearity:
        - MKT = return(SPY)
        - SIZE = return(IWM) - return(SPY)
        - VALUE = return(IVE) - return(SPY)
        - GROWTH = return(IVW) - return(SPY)
        - QUALITY = return(QUAL) - return(SPY)
        - VOL = return(SPLV) - return(SPY)
        - MOM = return(MTUM) - return(SPY)
        """
        if not end_date:
            end_date = date.today()

        # Get securities for factor ETFs
        securities = self.db.query(Security).filter(
            Security.symbol.in_(list(self.FACTOR_ETFS.keys()))
        ).all()

        security_map = {s.symbol: s.id for s in securities}

        if 'SPY' not in security_map:
            logger.error("SPY security not found")
            return 0

        # Get prices
        security_ids = list(security_map.values())

        prices_query = self.db.query(PricesEOD).filter(
            PricesEOD.security_id.in_(security_ids)
        )

        if start_date:
            prices_query = prices_query.filter(PricesEOD.date >= start_date)

        prices_query = prices_query.filter(PricesEOD.date <= end_date)

        prices = prices_query.all()

        if not prices:
            logger.warning("No prices found for factor ETFs")
            return 0

        # Convert to DataFrame
        prices_df = pd.DataFrame([
            {'date': p.date, 'security_id': p.security_id, 'close': p.close}
            for p in prices
        ])

        # Add symbol mapping
        id_to_symbol = {v: k for k, v in security_map.items()}
        prices_df['symbol'] = prices_df['security_id'].map(id_to_symbol)

        # Pivot to wide format
        prices_wide = prices_df.pivot(index='date', columns='symbol', values='close')

        # Compute returns
        returns_wide = prices_wide.pct_change()

        # Compute factor returns
        factor_returns = []

        for date_val in returns_wide.index[1:]:  # Skip first (NaN)
            if pd.isna(date_val):
                continue

            returns = returns_wide.loc[date_val]

            # Check if we have all required ETFs
            if 'SPY' not in returns or pd.isna(returns['SPY']):
                continue

            spy_return = returns['SPY']

            factors = {
                'MKT': spy_return,
                'SIZE': returns.get('IWM', 0) - spy_return if not pd.isna(returns.get('IWM')) else 0,
                'VALUE': returns.get('IVE', 0) - spy_return if not pd.isna(returns.get('IVE')) else 0,
                'GROWTH': returns.get('IVW', 0) - spy_return if not pd.isna(returns.get('IVW')) else 0,
                'QUALITY': returns.get('QUAL', 0) - spy_return if not pd.isna(returns.get('QUAL')) else 0,
                'VOL': returns.get('SPLV', 0) - spy_return if not pd.isna(returns.get('SPLV')) else 0,
                'MOM': returns.get('MTUM', 0) - spy_return if not pd.isna(returns.get('MTUM')) else 0,
            }

            factor_returns.append({
                'date': date_val,
                'factors': factors
            })

        # Store factor returns
        count = 0
        for row in factor_returns:
            existing = self.db.query(FactorReturn).filter(
                and_(
                    FactorReturn.factor_set_code == 'STYLE7',
                    FactorReturn.date == row['date']
                )
            ).first()

            if existing:
                if existing.factors_json != row['factors']:
                    existing.factors_json = row['factors']
            else:
                factor_return = FactorReturn(
                    factor_set_code='STYLE7',
                    date=row['date'],
                    factors_json=row['factors']
                )
                self.db.add(factor_return)
                count += 1

        self.db.commit()
        logger.info(f"Created {count} factor returns")
        return count

    def compute_factor_regression(
        self,
        view_type: ViewType,
        view_id: int,
        as_of_date: date,
        window: int = 252
    ) -> Optional[Dict]:
        """
        Run factor regression for a portfolio.
        y = portfolio returns
        X = [MKT, SIZE, VALUE, GROWTH, QUALITY, VOL, MOM]
        """
        # Get portfolio returns
        portfolio_returns = self.db.query(ReturnsEOD).filter(
            and_(
                ReturnsEOD.view_type == view_type,
                ReturnsEOD.view_id == view_id,
                ReturnsEOD.date <= as_of_date
            )
        ).order_by(ReturnsEOD.date.desc()).limit(window).all()

        if not portfolio_returns or len(portfolio_returns) < 60:
            return None

        # Get factor returns for same dates
        dates = [r.date for r in portfolio_returns]
        factor_returns = self.db.query(FactorReturn).filter(
            and_(
                FactorReturn.factor_set_code == 'STYLE7',
                FactorReturn.date.in_(dates)
            )
        ).all()

        if not factor_returns:
            return None

        # Convert to DataFrames
        port_df = pd.DataFrame([
            {'date': r.date, 'return': r.twr_return}
            for r in portfolio_returns
        ])

        # Build factor matrix
        factor_data = []
        for fr in factor_returns:
            row = {'date': fr.date}
            row.update(fr.factors_json)
            factor_data.append(row)

        factor_df = pd.DataFrame(factor_data)

        # Merge
        merged = port_df.merge(factor_df, on='date', how='inner')

        if len(merged) < 60:
            return None

        # Prepare regression data
        y = merged['return'].values
        X = merged[self.FACTOR_NAMES].values

        # Check for missing values
        if np.any(np.isnan(y)) or np.any(np.isnan(X)):
            # Remove rows with NaN
            valid_mask = ~(np.isnan(y) | np.any(np.isnan(X), axis=1))
            y = y[valid_mask]
            X = X[valid_mask]

            if len(y) < 60:
                return None

        # Run regression
        model = LinearRegression()
        model.fit(X, y)

        # Extract results
        betas = {
            factor: float(coef)
            for factor, coef in zip(self.FACTOR_NAMES, model.coef_)
        }

        alpha = float(model.intercept_) * 252  # Annualize
        r_squared = float(model.score(X, y))

        result = {
            'betas': betas,
            'alpha': alpha,
            'r_squared': r_squared
        }

        # Store regression
        existing = self.db.query(FactorRegression).filter(
            and_(
                FactorRegression.view_type == view_type,
                FactorRegression.view_id == view_id,
                FactorRegression.factor_set_code == 'STYLE7',
                FactorRegression.as_of_date == as_of_date,
                FactorRegression.window == window
            )
        ).first()

        if existing:
            existing.betas_json = betas
            existing.alpha = alpha
            existing.r_squared = r_squared
        else:
            regression = FactorRegression(
                view_type=view_type,
                view_id=view_id,
                factor_set_code='STYLE7',
                as_of_date=as_of_date,
                window=window,
                betas_json=betas,
                alpha=alpha,
                r_squared=r_squared
            )
            self.db.add(regression)

        self.db.commit()

        return result
