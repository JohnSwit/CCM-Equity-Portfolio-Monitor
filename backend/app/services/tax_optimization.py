"""
Tax Optimization Service - Handles tax lot tracking, gain/loss calculations,
wash sale detection, and tax-loss harvesting recommendations.
"""
from datetime import date, timedelta
from typing import List, Dict, Optional, Tuple
from sqlalchemy.orm import Session
from sqlalchemy import and_, or_, func
import logging

from app.models.models import (
    Transaction, TransactionType, Security, Account, PricesEOD,
    TaxLot, RealizedGain, WashSaleViolation
)

logger = logging.getLogger(__name__)

# Tax constants
SHORT_TERM_HOLDING_DAYS = 365
WASH_SALE_WINDOW_DAYS = 30


class TaxService:
    def __init__(self, db: Session):
        self.db = db

    def build_tax_lots_for_account(self, account_id: int) -> int:
        """
        Build or rebuild tax lots from transactions for an account.
        Uses FIFO method for matching sales to purchases.
        Returns number of lots created.
        """
        # Get all buy/sell transactions for the account, ordered by date
        transactions = self.db.query(Transaction).filter(
            and_(
                Transaction.account_id == account_id,
                Transaction.transaction_type.in_([
                    TransactionType.BUY,
                    TransactionType.SELL,
                    TransactionType.DIVIDEND_REINVEST
                ]),
                Transaction.security_id.isnot(None)
            )
        ).order_by(Transaction.trade_date, Transaction.id).all()

        # Group by security
        by_security: Dict[int, List[Transaction]] = {}
        for txn in transactions:
            if txn.security_id not in by_security:
                by_security[txn.security_id] = []
            by_security[txn.security_id].append(txn)

        lots_created = 0
        for security_id, txns in by_security.items():
            lots_created += self._process_security_transactions(account_id, security_id, txns)

        self.db.commit()
        return lots_created

    def _process_security_transactions(
        self, account_id: int, security_id: int, transactions: List[Transaction]
    ) -> int:
        """Process transactions for a single security, creating lots and realized gains."""
        # Delete existing lots and gains for this account/security
        self.db.query(RealizedGain).filter(
            and_(
                RealizedGain.account_id == account_id,
                RealizedGain.security_id == security_id
            )
        ).delete()
        self.db.query(TaxLot).filter(
            and_(
                TaxLot.account_id == account_id,
                TaxLot.security_id == security_id
            )
        ).delete()

        open_lots: List[TaxLot] = []
        lots_created = 0

        for txn in transactions:
            if txn.transaction_type in [TransactionType.BUY, TransactionType.DIVIDEND_REINVEST]:
                # Create a new tax lot
                shares = abs(txn.units) if txn.units else 0
                price = txn.price if txn.price else 0
                total_cost = shares * price + (txn.transaction_fee or 0)

                lot = TaxLot(
                    account_id=account_id,
                    security_id=security_id,
                    purchase_date=txn.trade_date,
                    purchase_transaction_id=txn.id,
                    original_shares=shares,
                    cost_basis_per_share=total_cost / shares if shares > 0 else 0,
                    total_cost_basis=total_cost,
                    remaining_shares=shares,
                    remaining_cost_basis=total_cost,
                    is_closed=False
                )
                self.db.add(lot)
                self.db.flush()
                open_lots.append(lot)
                lots_created += 1

            elif txn.transaction_type == TransactionType.SELL:
                # Match against open lots using FIFO
                shares_to_sell = abs(txn.units) if txn.units else 0
                sale_price = txn.price if txn.price else 0

                while shares_to_sell > 0 and open_lots:
                    lot = open_lots[0]
                    shares_from_lot = min(shares_to_sell, lot.remaining_shares)

                    # Calculate gain/loss
                    proceeds = shares_from_lot * sale_price - (txn.transaction_fee or 0) * (shares_from_lot / abs(txn.units))
                    cost_basis = shares_from_lot * lot.cost_basis_per_share
                    gain_loss = proceeds - cost_basis
                    holding_days = (txn.trade_date - lot.purchase_date).days
                    is_short_term = holding_days < SHORT_TERM_HOLDING_DAYS

                    # Check for wash sale
                    wash_sale_amount = self._check_wash_sale(
                        account_id, security_id, txn.trade_date, gain_loss
                    )

                    # Create realized gain record
                    realized = RealizedGain(
                        account_id=account_id,
                        security_id=security_id,
                        tax_lot_id=lot.id,
                        sale_transaction_id=txn.id,
                        sale_date=txn.trade_date,
                        purchase_date=lot.purchase_date,
                        shares_sold=shares_from_lot,
                        sale_price_per_share=sale_price,
                        cost_basis_per_share=lot.cost_basis_per_share,
                        proceeds=proceeds,
                        cost_basis=cost_basis,
                        gain_loss=gain_loss,
                        is_short_term=is_short_term,
                        holding_period_days=holding_days,
                        is_wash_sale=wash_sale_amount > 0,
                        wash_sale_disallowed=wash_sale_amount,
                        adjusted_gain_loss=gain_loss + wash_sale_amount,  # Loss is negative, disallowed makes it less negative
                        tax_year=txn.trade_date.year
                    )
                    self.db.add(realized)

                    # Update lot
                    lot.remaining_shares -= shares_from_lot
                    lot.remaining_cost_basis = lot.remaining_shares * lot.cost_basis_per_share

                    if lot.remaining_shares <= 0.0001:  # Float comparison tolerance
                        lot.is_closed = True
                        lot.closed_date = txn.trade_date
                        open_lots.pop(0)

                    shares_to_sell -= shares_from_lot

        return lots_created

    def _check_wash_sale(
        self, account_id: int, security_id: int, sale_date: date, gain_loss: float
    ) -> float:
        """
        Check if a sale would trigger wash sale rules.
        Returns the disallowed loss amount (0 if not a wash sale or not a loss).
        """
        if gain_loss >= 0:
            return 0.0  # Not a loss, no wash sale

        # Check for purchases within 30 days before or after the sale
        window_start = sale_date - timedelta(days=WASH_SALE_WINDOW_DAYS)
        window_end = sale_date + timedelta(days=WASH_SALE_WINDOW_DAYS)

        replacement_purchase = self.db.query(Transaction).filter(
            and_(
                Transaction.account_id == account_id,
                Transaction.security_id == security_id,
                Transaction.transaction_type.in_([TransactionType.BUY, TransactionType.DIVIDEND_REINVEST]),
                Transaction.trade_date >= window_start,
                Transaction.trade_date <= window_end,
                Transaction.trade_date != sale_date
            )
        ).first()

        if replacement_purchase:
            # Wash sale triggered - entire loss disallowed for simplicity
            # (In reality, it's proportional to replacement shares)
            return abs(gain_loss)

        return 0.0

    def get_tax_lots(
        self,
        account_id: Optional[int] = None,
        security_id: Optional[int] = None,
        include_closed: bool = False
    ) -> List[Dict]:
        """Get tax lots with current values."""
        query = self.db.query(TaxLot).join(Security).join(Account)

        if account_id:
            query = query.filter(TaxLot.account_id == account_id)
        if security_id:
            query = query.filter(TaxLot.security_id == security_id)
        if not include_closed:
            query = query.filter(TaxLot.is_closed == False)

        lots = query.order_by(TaxLot.purchase_date).all()

        result = []
        today = date.today()

        for lot in lots:
            # Get current price
            current_price = self._get_current_price(lot.security_id)
            current_value = lot.remaining_shares * current_price if current_price else None
            unrealized = (current_value - lot.remaining_cost_basis) if current_value else None
            unrealized_pct = (unrealized / lot.remaining_cost_basis * 100) if unrealized and lot.remaining_cost_basis else None
            holding_days = (today - lot.purchase_date).days

            result.append({
                "id": lot.id,
                "account_id": lot.account_id,
                "account_number": lot.account.account_number if lot.account else None,
                "security_id": lot.security_id,
                "symbol": lot.security.symbol if lot.security else None,
                "purchase_date": lot.purchase_date,
                "original_shares": lot.original_shares,
                "remaining_shares": lot.remaining_shares,
                "cost_basis_per_share": lot.cost_basis_per_share,
                "remaining_cost_basis": lot.remaining_cost_basis,
                "current_price": current_price,
                "current_value": current_value,
                "unrealized_gain_loss": unrealized,
                "unrealized_gain_loss_pct": unrealized_pct,
                "holding_period_days": holding_days,
                "is_short_term": holding_days < SHORT_TERM_HOLDING_DAYS,
                "wash_sale_adjustment": lot.wash_sale_adjustment
            })

        return result

    def get_realized_gains(
        self,
        account_id: Optional[int] = None,
        tax_year: Optional[int] = None
    ) -> Tuple[List[Dict], Dict]:
        """Get realized gains with summary."""
        query = self.db.query(RealizedGain).join(Security).join(Account)

        if account_id:
            query = query.filter(RealizedGain.account_id == account_id)
        if tax_year:
            query = query.filter(RealizedGain.tax_year == tax_year)

        gains = query.order_by(RealizedGain.sale_date.desc()).all()

        result = []
        summary = {
            "short_term_gains": 0.0,
            "short_term_losses": 0.0,
            "long_term_gains": 0.0,
            "long_term_losses": 0.0,
            "wash_sale_disallowed": 0.0
        }

        for g in gains:
            result.append({
                "id": g.id,
                "account_id": g.account_id,
                "account_number": g.account.account_number if g.account else None,
                "security_id": g.security_id,
                "symbol": g.security.symbol if g.security else None,
                "sale_date": g.sale_date,
                "purchase_date": g.purchase_date,
                "shares_sold": g.shares_sold,
                "sale_price_per_share": g.sale_price_per_share,
                "cost_basis_per_share": g.cost_basis_per_share,
                "proceeds": g.proceeds,
                "cost_basis": g.cost_basis,
                "gain_loss": g.gain_loss,
                "is_short_term": g.is_short_term,
                "holding_period_days": g.holding_period_days,
                "is_wash_sale": g.is_wash_sale,
                "wash_sale_disallowed": g.wash_sale_disallowed,
                "adjusted_gain_loss": g.adjusted_gain_loss,
                "tax_year": g.tax_year
            })

            if g.is_short_term:
                if g.adjusted_gain_loss >= 0:
                    summary["short_term_gains"] += g.adjusted_gain_loss
                else:
                    summary["short_term_losses"] += abs(g.adjusted_gain_loss)
            else:
                if g.adjusted_gain_loss >= 0:
                    summary["long_term_gains"] += g.adjusted_gain_loss
                else:
                    summary["long_term_losses"] += abs(g.adjusted_gain_loss)

            summary["wash_sale_disallowed"] += g.wash_sale_disallowed

        return result, summary

    def get_tax_summary(
        self,
        account_id: Optional[int] = None,
        tax_year: Optional[int] = None
    ) -> Dict:
        """Get comprehensive tax summary including unrealized gains."""
        if not tax_year:
            tax_year = date.today().year

        _, realized_summary = self.get_realized_gains(account_id, tax_year)

        # Calculate unrealized from open lots
        lots = self.get_tax_lots(account_id, include_closed=False)

        unrealized = {
            "short_term_gains": 0.0,
            "short_term_losses": 0.0,
            "long_term_gains": 0.0,
            "long_term_losses": 0.0
        }

        for lot in lots:
            if lot["unrealized_gain_loss"] is None:
                continue
            if lot["is_short_term"]:
                if lot["unrealized_gain_loss"] >= 0:
                    unrealized["short_term_gains"] += lot["unrealized_gain_loss"]
                else:
                    unrealized["short_term_losses"] += abs(lot["unrealized_gain_loss"])
            else:
                if lot["unrealized_gain_loss"] >= 0:
                    unrealized["long_term_gains"] += lot["unrealized_gain_loss"]
                else:
                    unrealized["long_term_losses"] += abs(lot["unrealized_gain_loss"])

        net_short_term = realized_summary["short_term_gains"] - realized_summary["short_term_losses"]
        net_long_term = realized_summary["long_term_gains"] - realized_summary["long_term_losses"]

        # Estimate tax (simplified)
        short_term_rate = 0.37  # Highest bracket
        long_term_rate = 0.20

        estimated_tax = max(0, net_short_term) * short_term_rate + max(0, net_long_term) * long_term_rate

        return {
            "tax_year": tax_year,
            "short_term_realized_gains": realized_summary["short_term_gains"],
            "short_term_realized_losses": realized_summary["short_term_losses"],
            "net_short_term": net_short_term,
            "long_term_realized_gains": realized_summary["long_term_gains"],
            "long_term_realized_losses": realized_summary["long_term_losses"],
            "net_long_term": net_long_term,
            "total_realized": net_short_term + net_long_term,
            "wash_sale_disallowed": realized_summary["wash_sale_disallowed"],
            "short_term_unrealized_gains": unrealized["short_term_gains"],
            "short_term_unrealized_losses": unrealized["short_term_losses"],
            "net_short_term_unrealized": unrealized["short_term_gains"] - unrealized["short_term_losses"],
            "long_term_unrealized_gains": unrealized["long_term_gains"],
            "long_term_unrealized_losses": unrealized["long_term_losses"],
            "net_long_term_unrealized": unrealized["long_term_gains"] - unrealized["long_term_losses"],
            "total_unrealized": (unrealized["short_term_gains"] - unrealized["short_term_losses"] +
                                unrealized["long_term_gains"] - unrealized["long_term_losses"]),
            "estimated_tax_liability": estimated_tax,
            "marginal_rate_short_term": short_term_rate,
            "marginal_rate_long_term": long_term_rate
        }

    def get_tax_loss_harvesting_candidates(
        self,
        account_id: Optional[int] = None,
        min_loss: float = 100.0
    ) -> List[Dict]:
        """Find positions with unrealized losses that could be harvested."""
        lots = self.get_tax_lots(account_id, include_closed=False)

        # Group by security
        by_security: Dict[int, List[Dict]] = {}
        for lot in lots:
            if lot["security_id"] not in by_security:
                by_security[lot["security_id"]] = []
            by_security[lot["security_id"]].append(lot)

        candidates = []
        today = date.today()
        wash_sale_restricted = []

        for security_id, security_lots in by_security.items():
            total_unrealized = sum(l["unrealized_gain_loss"] or 0 for l in security_lots)

            if total_unrealized >= -min_loss:
                continue  # Not enough loss to harvest

            symbol = security_lots[0]["symbol"]

            # Check for recent purchases (wash sale risk)
            recent_cutoff = today - timedelta(days=WASH_SALE_WINDOW_DAYS)
            recent_purchase = any(l["purchase_date"] >= recent_cutoff for l in security_lots)

            # Check if there are pending wash sale issues
            pending_wash = self._has_pending_wash_sale(account_id, security_id)

            short_term_loss = sum(
                (l["unrealized_gain_loss"] or 0)
                for l in security_lots
                if l["is_short_term"] and (l["unrealized_gain_loss"] or 0) < 0
            )
            long_term_loss = sum(
                (l["unrealized_gain_loss"] or 0)
                for l in security_lots
                if not l["is_short_term"] and (l["unrealized_gain_loss"] or 0) < 0
            )

            total_shares = sum(l["remaining_shares"] for l in security_lots)
            total_cost = sum(l["remaining_cost_basis"] for l in security_lots)
            total_value = sum(l["current_value"] or 0 for l in security_lots)

            candidate = {
                "symbol": symbol,
                "security_id": security_id,
                "total_shares": total_shares,
                "total_cost_basis": total_cost,
                "current_value": total_value,
                "unrealized_loss": total_unrealized,
                "unrealized_loss_pct": (total_unrealized / total_cost * 100) if total_cost else 0,
                "short_term_loss": short_term_loss,
                "long_term_loss": long_term_loss,
                "has_recent_purchase": recent_purchase,
                "has_pending_wash_sale": pending_wash,
                "wash_sale_window_end": (today + timedelta(days=WASH_SALE_WINDOW_DAYS)) if recent_purchase else None,
                "lots": security_lots
            }
            candidates.append(candidate)

            if recent_purchase or pending_wash:
                wash_sale_restricted.append(symbol)

        # Sort by loss amount (most negative first)
        candidates.sort(key=lambda x: x["unrealized_loss"])

        return candidates, wash_sale_restricted

    def _has_pending_wash_sale(self, account_id: Optional[int], security_id: int) -> bool:
        """Check if selling would trigger a wash sale."""
        today = date.today()
        window_start = today - timedelta(days=WASH_SALE_WINDOW_DAYS)

        query = self.db.query(Transaction).filter(
            and_(
                Transaction.security_id == security_id,
                Transaction.transaction_type.in_([TransactionType.BUY, TransactionType.DIVIDEND_REINVEST]),
                Transaction.trade_date >= window_start
            )
        )
        if account_id:
            query = query.filter(Transaction.account_id == account_id)

        return query.first() is not None

    def check_wash_sale(
        self,
        account_id: int,
        symbol: str,
        trade_date: Optional[date] = None
    ) -> Dict:
        """Check if a trade would trigger wash sale rules."""
        if not trade_date:
            trade_date = date.today()

        security = self.db.query(Security).filter(Security.symbol == symbol.upper()).first()
        if not security:
            return {
                "symbol": symbol,
                "would_trigger_wash_sale": False,
                "reason": "Security not found"
            }

        window_start = trade_date - timedelta(days=WASH_SALE_WINDOW_DAYS)
        window_end = trade_date + timedelta(days=WASH_SALE_WINDOW_DAYS)

        # Check for purchases in the window
        conflicting = self.db.query(Transaction).filter(
            and_(
                Transaction.account_id == account_id,
                Transaction.security_id == security.id,
                Transaction.transaction_type.in_([TransactionType.BUY, TransactionType.DIVIDEND_REINVEST]),
                Transaction.trade_date >= window_start,
                Transaction.trade_date <= window_end
            )
        ).all()

        if not conflicting:
            return {
                "symbol": symbol,
                "would_trigger_wash_sale": False,
                "safe_to_trade_date": None,
                "conflicting_transactions": []
            }

        # Find when it would be safe
        latest_purchase = max(t.trade_date for t in conflicting)
        safe_date = latest_purchase + timedelta(days=WASH_SALE_WINDOW_DAYS + 1)

        # Estimate disallowed loss
        lots = self.get_tax_lots(account_id, security.id, include_closed=False)
        total_unrealized = sum(l["unrealized_gain_loss"] or 0 for l in lots)
        disallowed = abs(total_unrealized) if total_unrealized < 0 else 0

        return {
            "symbol": symbol,
            "would_trigger_wash_sale": True,
            "reason": f"Purchase(s) within 30-day window",
            "safe_to_trade_date": safe_date,
            "conflicting_transactions": [
                {"date": str(t.trade_date), "shares": t.units, "price": t.price}
                for t in conflicting
            ],
            "disallowed_loss_estimate": disallowed
        }

    def analyze_trade_impact(
        self,
        account_id: int,
        symbol: str,
        shares: float,
        price_override: Optional[float] = None
    ) -> Dict:
        """Analyze tax impact of selling shares using different lot selection methods."""
        security = self.db.query(Security).filter(Security.symbol == symbol.upper()).first()
        if not security:
            raise ValueError(f"Security not found: {symbol}")

        lots = self.get_tax_lots(account_id, security.id, include_closed=False)
        if not lots:
            raise ValueError(f"No open lots for {symbol}")

        current_price = price_override or self._get_current_price(security.id)
        if not current_price:
            raise ValueError(f"No price available for {symbol}")

        proceeds = shares * current_price

        # FIFO - First In First Out
        fifo_result = self._calculate_sale_impact(lots, shares, current_price, "fifo")

        # LIFO - Last In First Out
        lifo_result = self._calculate_sale_impact(lots, shares, current_price, "lifo")

        # HIFO - Highest In First Out (maximize loss / minimize gain)
        hifo_result = self._calculate_sale_impact(lots, shares, current_price, "hifo")

        # LOFO - Lowest In First Out (minimize loss / maximize gain for tax deferral)
        lofo_result = self._calculate_sale_impact(lots, shares, current_price, "lofo")

        # Determine recommended method
        methods = {
            "fifo": fifo_result,
            "lifo": lifo_result,
            "hifo": hifo_result,
            "lofo": lofo_result
        }

        # Prefer method that results in lowest tax (most loss or least gain)
        best_method = min(methods.keys(), key=lambda m: methods[m]["estimated_tax"])

        return {
            "symbol": symbol,
            "action": "sell",
            "shares": shares,
            "estimated_proceeds": proceeds,
            "fifo_impact": fifo_result,
            "lifo_impact": lifo_result,
            "hifo_impact": hifo_result,
            "lofo_impact": lofo_result,
            "recommended_method": best_method,
            "recommended_lots": methods[best_method]["lots_used"],
            "tax_savings_vs_fifo": fifo_result["estimated_tax"] - methods[best_method]["estimated_tax"]
        }

    def _calculate_sale_impact(
        self,
        lots: List[Dict],
        shares: float,
        price: float,
        method: str
    ) -> Dict:
        """Calculate impact of selling using a specific lot selection method."""
        sorted_lots = self._sort_lots_for_method(lots, method)

        remaining = shares
        short_term_gain = 0.0
        long_term_gain = 0.0
        total_cost_basis = 0.0
        lots_used = []

        for lot in sorted_lots:
            if remaining <= 0:
                break

            available = lot["remaining_shares"]
            take = min(remaining, available)

            cost = take * lot["cost_basis_per_share"]
            proceeds = take * price
            gain = proceeds - cost

            total_cost_basis += cost

            if lot["is_short_term"]:
                short_term_gain += gain
            else:
                long_term_gain += gain

            lots_used.append(lot["id"])
            remaining -= take

        # Estimate tax
        st_tax = max(0, short_term_gain) * 0.37
        lt_tax = max(0, long_term_gain) * 0.20
        total_tax = st_tax + lt_tax

        return {
            "total_gain_loss": short_term_gain + long_term_gain,
            "short_term_gain_loss": short_term_gain,
            "long_term_gain_loss": long_term_gain,
            "total_cost_basis": total_cost_basis,
            "estimated_tax": total_tax,
            "lots_used": lots_used
        }

    def _sort_lots_for_method(self, lots: List[Dict], method: str) -> List[Dict]:
        """Sort lots based on selection method."""
        if method == "fifo":
            return sorted(lots, key=lambda x: x["purchase_date"])
        elif method == "lifo":
            return sorted(lots, key=lambda x: x["purchase_date"], reverse=True)
        elif method == "hifo":
            # Highest cost first (maximize loss / minimize gain)
            return sorted(lots, key=lambda x: x["cost_basis_per_share"], reverse=True)
        elif method == "lofo":
            # Lowest cost first (minimize loss / maximize gain)
            return sorted(lots, key=lambda x: x["cost_basis_per_share"])
        else:
            return lots

    def get_sell_suggestions(
        self,
        account_id: int,
        symbol: str,
        shares: float,
        objective: str = "minimize_tax"  # "minimize_tax", "harvest_loss", "defer_gains"
    ) -> List[Dict]:
        """Get specific lot sell suggestions based on tax objective."""
        security = self.db.query(Security).filter(Security.symbol == symbol.upper()).first()
        if not security:
            return []

        lots = self.get_tax_lots(account_id, security.id, include_closed=False)
        current_price = self._get_current_price(security.id)

        if not current_price:
            return []

        suggestions = []
        for lot in lots:
            gain_loss = (current_price - lot["cost_basis_per_share"]) * lot["remaining_shares"]

            # Score based on objective
            if objective == "minimize_tax":
                # Prefer losses over gains, long-term over short-term
                if gain_loss < 0:
                    score = 100 + abs(gain_loss)  # Losses are best
                elif lot["is_short_term"]:
                    score = -gain_loss * 2  # Short term gains are worst
                else:
                    score = -gain_loss  # Long term gains are better than short term

            elif objective == "harvest_loss":
                # Only consider losses, prefer larger losses
                if gain_loss >= 0:
                    score = -1000  # Don't want gains
                else:
                    score = abs(gain_loss)

            elif objective == "defer_gains":
                # Prefer lots closest to long-term status
                days_to_long_term = max(0, SHORT_TERM_HOLDING_DAYS - lot["holding_period_days"])
                if gain_loss > 0 and lot["is_short_term"]:
                    score = -days_to_long_term  # Closer to long-term is better
                else:
                    score = 0

            recommendation = "harvest_loss" if gain_loss < 0 else (
                "long_term_gain" if not lot["is_short_term"] else "short_term_gain"
            )

            suggestions.append({
                "lot_id": lot["id"],
                "symbol": lot["symbol"],
                "purchase_date": lot["purchase_date"],
                "shares_available": lot["remaining_shares"],
                "cost_basis_per_share": lot["cost_basis_per_share"],
                "current_price": current_price,
                "gain_loss_per_share": current_price - lot["cost_basis_per_share"],
                "total_gain_loss": gain_loss,
                "is_short_term": lot["is_short_term"],
                "holding_period_days": lot["holding_period_days"],
                "tax_efficiency_score": score,
                "recommendation": recommendation
            })

        suggestions.sort(key=lambda x: x["tax_efficiency_score"], reverse=True)
        return suggestions

    def _get_current_price(self, security_id: int) -> Optional[float]:
        """Get the most recent price for a security."""
        price = self.db.query(PricesEOD.close).filter(
            PricesEOD.security_id == security_id
        ).order_by(PricesEOD.date.desc()).first()

        return price[0] if price else None
