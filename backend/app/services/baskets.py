import pandas as pd
from typing import List, Dict, Optional
from datetime import date
from sqlalchemy.orm import Session
from sqlalchemy import and_
from app.models import (
    Basket, BasketConstituent, Security, PricesEOD,
    BenchmarkLevel, BenchmarkReturn
)
import logging

logger = logging.getLogger(__name__)


class BasketsEngine:
    """Manages custom baskets and computes basket levels"""

    def __init__(self, db: Session):
        self.db = db

    def create_basket(
        self,
        code: str,
        name: str,
        constituents: List[Dict[str, float]]
    ) -> Basket:
        """Create a custom basket"""
        # Validate weights sum to 1
        total_weight = sum(c['weight'] for c in constituents)
        if abs(total_weight - 1.0) > 0.001:
            raise ValueError("Constituent weights must sum to 1.0")

        # Create basket
        basket = Basket(code=code, name=name)
        self.db.add(basket)
        self.db.flush()

        # Add constituents
        for constituent in constituents:
            basket_constituent = BasketConstituent(
                basket_id=basket.id,
                symbol=constituent['symbol'].upper(),
                weight=constituent['weight']
            )
            self.db.add(basket_constituent)

        self.db.commit()
        self.db.refresh(basket)
        return basket

    def update_basket_constituents(
        self,
        basket_id: int,
        constituents: List[Dict[str, float]]
    ) -> bool:
        """Update basket constituents"""
        # Validate weights
        total_weight = sum(c['weight'] for c in constituents)
        if abs(total_weight - 1.0) > 0.001:
            raise ValueError("Constituent weights must sum to 1.0")

        # Delete existing constituents
        self.db.query(BasketConstituent).filter(
            BasketConstituent.basket_id == basket_id
        ).delete()

        # Add new constituents
        for constituent in constituents:
            basket_constituent = BasketConstituent(
                basket_id=basket_id,
                symbol=constituent['symbol'].upper(),
                weight=constituent['weight']
            )
            self.db.add(basket_constituent)

        self.db.commit()
        return True

    def compute_basket_returns(
        self,
        basket_code: str,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> int:
        """
        Compute basket returns as weighted average of constituent returns.
        Store as benchmark-like series for compatibility.
        """
        if not end_date:
            end_date = date.today()

        # Get basket
        basket = self.db.query(Basket).filter(Basket.code == basket_code).first()
        if not basket:
            logger.warning(f"Basket {basket_code} not found")
            return 0

        # Get constituents
        constituents = self.db.query(BasketConstituent).filter(
            BasketConstituent.basket_id == basket.id
        ).all()

        if not constituents:
            logger.warning(f"Basket {basket_code} has no constituents")
            return 0

        # Get securities
        symbols = [c.symbol for c in constituents]
        securities = self.db.query(Security).filter(
            Security.symbol.in_(symbols)
        ).all()

        security_map = {s.symbol: s.id for s in securities}

        # Build constituent data
        constituent_data = []
        for c in constituents:
            security_id = security_map.get(c.symbol)
            if security_id:
                constituent_data.append({
                    'symbol': c.symbol,
                    'security_id': security_id,
                    'weight': c.weight
                })

        if not constituent_data:
            logger.warning(f"No securities found for basket {basket_code}")
            return 0

        # Get prices for all constituents
        security_ids = [c['security_id'] for c in constituent_data]

        prices_query = self.db.query(PricesEOD).filter(
            PricesEOD.security_id.in_(security_ids)
        )

        if start_date:
            prices_query = prices_query.filter(PricesEOD.date >= start_date)

        prices_query = prices_query.filter(PricesEOD.date <= end_date)

        prices = prices_query.all()

        if not prices:
            logger.warning(f"No prices found for basket {basket_code}")
            return 0

        # Convert to DataFrame
        prices_df = pd.DataFrame([
            {'date': p.date, 'security_id': p.security_id, 'close': p.close}
            for p in prices
        ])

        # Pivot to wide format
        prices_wide = prices_df.pivot(index='date', columns='security_id', values='close')

        # Compute daily returns for each security
        returns_wide = prices_wide.pct_change()

        # Map security IDs to weights
        weight_map = {c['security_id']: c['weight'] for c in constituent_data}

        # Compute basket returns as weighted average
        basket_returns = []
        for date_val in returns_wide.index:
            if pd.isna(date_val):
                continue

            daily_returns = returns_wide.loc[date_val]
            weighted_return = sum(
                daily_returns.get(sec_id, 0) * weight
                for sec_id, weight in weight_map.items()
                if not pd.isna(daily_returns.get(sec_id))
            )

            basket_returns.append({
                'date': date_val,
                'return': weighted_return
            })

        # Compute index level
        index_level = 100.0
        basket_levels = [{'date': basket_returns[0]['date'], 'level': index_level}]

        for ret in basket_returns[1:]:
            index_level = index_level * (1 + ret['return'])
            basket_levels.append({'date': ret['date'], 'level': index_level})

        # Store as benchmark (for compatibility with benchmark API)
        count_levels = 0
        count_returns = 0

        for level_data in basket_levels:
            existing = self.db.query(BenchmarkLevel).filter(
                and_(
                    BenchmarkLevel.code == basket_code,
                    BenchmarkLevel.date == level_data['date']
                )
            ).first()

            if existing:
                if existing.level != level_data['level']:
                    existing.level = level_data['level']
            else:
                level = BenchmarkLevel(
                    code=basket_code,
                    date=level_data['date'],
                    level=level_data['level']
                )
                self.db.add(level)
                count_levels += 1

        for ret_data in basket_returns[1:]:  # Skip first (no return)
            existing = self.db.query(BenchmarkReturn).filter(
                and_(
                    BenchmarkReturn.code == basket_code,
                    BenchmarkReturn.date == ret_data['date']
                )
            ).first()

            if existing:
                if existing.return_value != ret_data['return']:
                    existing.return_value = ret_data['return']
            else:
                ret_obj = BenchmarkReturn(
                    code=basket_code,
                    date=ret_data['date'],
                    return_value=ret_data['return']
                )
                self.db.add(ret_obj)
                count_returns += 1

        self.db.commit()
        logger.info(f"Created {count_levels} levels and {count_returns} returns for basket {basket_code}")
        return count_levels + count_returns

    def compute_all_baskets(self) -> Dict[str, int]:
        """Compute returns for all baskets"""
        baskets = self.db.query(Basket).all()

        results = {
            'total_baskets': len(baskets),
            'updated': 0,
            'failed': 0
        }

        for basket in baskets:
            try:
                count = self.compute_basket_returns(basket.code)
                if count > 0:
                    results['updated'] += 1
            except Exception as e:
                logger.error(f"Failed to compute basket {basket.code}: {e}")
                results['failed'] += 1

        return results
