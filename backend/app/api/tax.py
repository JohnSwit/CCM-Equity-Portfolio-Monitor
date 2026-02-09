"""
Tax Optimization API endpoints - tax lot management, gain/loss tracking,
wash sale detection, and tax-loss harvesting recommendations.
"""
import logging
from fastapi import APIRouter, Depends, HTTPException, Query, Body
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import date
from pydantic import BaseModel

from app.core.database import get_db
from app.api.auth import get_current_user
from app.models.models import User, Account, Security, Transaction
from app.models.schemas import (
    TaxLotResponse, TaxLotListResponse, RealizedGainResponse, RealizedGainListResponse,
    TaxSummaryResponse, TaxLossHarvestingResponse, TaxLossHarvestingCandidate,
    WashSaleCheckResult, TradeImpactAnalysis, TaxLotSellSuggestion, SellOrderRequest
)
from app.services.tax_optimization import TaxService


class SimulateLotsRequest(BaseModel):
    lot_ids: List[int]

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/tax", tags=["tax"])


# ============== Tax Lot Management ==============

@router.post("/build-lots")
def build_tax_lots(
    account_id: Optional[int] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Build or rebuild tax lots from transactions.
    If account_id is provided, only build for that account.
    Otherwise, build for all accounts.
    """
    tax_service = TaxService(db)

    if account_id:
        accounts = [db.query(Account).filter(Account.id == account_id).first()]
        if not accounts[0]:
            raise HTTPException(status_code=404, detail="Account not found")
    else:
        accounts = db.query(Account).all()

    logger.info(f"Building tax lots for {len(accounts)} accounts")

    # Log transaction counts for debugging
    total_txns = db.query(Transaction).count()
    with_security = db.query(Transaction).filter(Transaction.security_id.isnot(None)).count()
    logger.info(f"Total transactions in DB: {total_txns}, with security_id: {with_security}")

    total_lots = 0
    results = []

    for account in accounts:
        try:
            # Log transaction count for this account
            acct_txns = db.query(Transaction).filter(Transaction.account_id == account.id).count()
            logger.info(f"Account {account.account_number}: {acct_txns} total transactions")

            lots_created = tax_service.build_tax_lots_for_account(account.id)
            total_lots += lots_created
            results.append({
                "account_id": account.id,
                "account_number": account.account_number,
                "lots_created": lots_created
            })
            logger.info(f"Account {account.account_number}: created {lots_created} lots")
        except Exception as e:
            logger.error(f"Error building lots for account {account.account_number}: {e}")
            results.append({
                "account_id": account.id,
                "account_number": account.account_number,
                "error": str(e)
            })

    logger.info(f"Total lots created: {total_lots}")
    return {
        "status": "success",
        "total_lots_created": total_lots,
        "accounts_processed": len(accounts),
        "details": results
    }


@router.get("/lots", response_model=TaxLotListResponse)
def get_tax_lots(
    account_id: Optional[int] = None,
    symbol: Optional[str] = None,
    include_closed: bool = False,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get tax lots with current values and unrealized gains."""
    tax_service = TaxService(db)

    security_id = None
    if symbol:
        security = db.query(Security).filter(Security.symbol == symbol.upper()).first()
        if security:
            security_id = security.id

    lots = tax_service.get_tax_lots(account_id, security_id, include_closed)

    total_cost = sum(l["remaining_cost_basis"] for l in lots)
    total_value = sum(l["current_value"] or 0 for l in lots)
    total_unrealized = sum(l["unrealized_gain_loss"] or 0 for l in lots)

    return TaxLotListResponse(
        lots=[TaxLotResponse(**l) for l in lots],
        total_cost_basis=total_cost,
        total_current_value=total_value,
        total_unrealized_gain_loss=total_unrealized
    )


@router.get("/lots/{symbol}")
def get_lots_by_symbol(
    symbol: str,
    account_id: Optional[int] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get tax lots for a specific symbol."""
    tax_service = TaxService(db)

    security = db.query(Security).filter(Security.symbol == symbol.upper()).first()
    if not security:
        raise HTTPException(status_code=404, detail="Security not found")

    lots = tax_service.get_tax_lots(account_id, security.id, include_closed=False)

    return {
        "symbol": symbol.upper(),
        "lots": lots,
        "total_shares": sum(l["remaining_shares"] for l in lots),
        "total_cost_basis": sum(l["remaining_cost_basis"] for l in lots),
        "total_current_value": sum(l["current_value"] or 0 for l in lots),
        "total_unrealized": sum(l["unrealized_gain_loss"] or 0 for l in lots)
    }


# ============== Realized Gains ==============

@router.get("/realized-gains", response_model=RealizedGainListResponse)
def get_realized_gains(
    account_id: Optional[int] = None,
    tax_year: Optional[int] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get realized gains/losses with summary."""
    tax_service = TaxService(db)

    if not tax_year:
        tax_year = date.today().year

    gains, summary_data = tax_service.get_realized_gains(account_id, tax_year)

    # Build full summary
    full_summary = tax_service.get_tax_summary(account_id, tax_year)

    return RealizedGainListResponse(
        gains=[RealizedGainResponse(**g) for g in gains],
        summary=TaxSummaryResponse(**full_summary)
    )


# ============== Tax Summary ==============

@router.get("/summary", response_model=TaxSummaryResponse)
def get_tax_summary(
    account_id: Optional[int] = None,
    tax_year: Optional[int] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get comprehensive tax summary including realized and unrealized gains."""
    tax_service = TaxService(db)
    summary = tax_service.get_tax_summary(account_id, tax_year)
    return TaxSummaryResponse(**summary)


# ============== Tax-Loss Harvesting ==============

@router.get("/harvest-candidates", response_model=TaxLossHarvestingResponse)
def get_harvest_candidates(
    account_id: Optional[int] = None,
    min_loss: float = 100.0,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Find positions with unrealized losses that could be harvested."""
    tax_service = TaxService(db)
    candidates, wash_restricted = tax_service.get_tax_loss_harvesting_candidates(account_id, min_loss)

    total_loss = sum(c["unrealized_loss"] for c in candidates)
    st_loss = sum(c["short_term_loss"] for c in candidates)
    lt_loss = sum(c["long_term_loss"] for c in candidates)

    return TaxLossHarvestingResponse(
        candidates=[TaxLossHarvestingCandidate(**c) for c in candidates],
        total_harvestable_loss=total_loss,
        short_term_harvestable=st_loss,
        long_term_harvestable=lt_loss,
        wash_sale_restricted=wash_restricted
    )


# ============== Wash Sale Check ==============

@router.get("/wash-sale-check", response_model=WashSaleCheckResult)
def check_wash_sale(
    account_id: int,
    symbol: str,
    trade_date: Optional[date] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Check if selling a security would trigger wash sale rules."""
    tax_service = TaxService(db)
    result = tax_service.check_wash_sale(account_id, symbol, trade_date)
    return WashSaleCheckResult(**result)


# ============== Trade Impact Analysis ==============

@router.get("/trade-impact", response_model=TradeImpactAnalysis)
def analyze_trade_impact(
    account_id: int,
    symbol: str,
    shares: float,
    price: Optional[float] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Analyze tax impact of selling shares using different lot selection methods."""
    tax_service = TaxService(db)

    try:
        result = tax_service.analyze_trade_impact(account_id, symbol, shares, price)
        return TradeImpactAnalysis(**result)
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/sell-suggestions", response_model=List[TaxLotSellSuggestion])
def get_sell_suggestions(
    account_id: int,
    symbol: str,
    shares: float,
    objective: str = "minimize_tax",
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get specific lot sell suggestions based on tax objective.

    Objectives:
    - minimize_tax: Minimize overall tax impact
    - harvest_loss: Maximize loss harvesting
    - defer_gains: Defer gains by prioritizing lots close to long-term status
    """
    if objective not in ["minimize_tax", "harvest_loss", "defer_gains"]:
        raise HTTPException(status_code=400, detail="Invalid objective. Use: minimize_tax, harvest_loss, defer_gains")

    tax_service = TaxService(db)
    suggestions = tax_service.get_sell_suggestions(account_id, symbol, shares, objective)

    return [TaxLotSellSuggestion(**s) for s in suggestions]


# ============== Multi-Lot Trade Simulation ==============

@router.post("/simulate-lots")
def simulate_selected_lots(
    request: SimulateLotsRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Simulate selling selected tax lots. Accepts a list of lot IDs and
    returns per-lot and aggregate tax impact analysis.
    """
    from app.models.models import TaxLot, PricesEOD

    if not request.lot_ids:
        raise HTTPException(status_code=400, detail="No lot IDs provided")

    tax_service = TaxService(db)
    today = date.today()

    # Fetch all selected lots with their relationships
    lots = db.query(TaxLot).filter(
        TaxLot.id.in_(request.lot_ids),
        TaxLot.is_closed == False
    ).all()

    if not lots:
        raise HTTPException(status_code=404, detail="No open lots found for the given IDs")

    missing_ids = set(request.lot_ids) - {lot.id for lot in lots}
    if missing_ids:
        logger.warning(f"Some lot IDs not found or already closed: {missing_ids}")

    # Build per-lot analysis
    lot_results = []
    totals = {
        "total_proceeds": 0.0,
        "total_cost_basis": 0.0,
        "total_gain_loss": 0.0,
        "short_term_gain_loss": 0.0,
        "long_term_gain_loss": 0.0,
        "estimated_tax": 0.0,
        "total_shares": 0.0,
        "lot_count": 0,
    }

    for lot in lots:
        current_price = tax_service._get_current_price(lot.security_id)
        if not current_price:
            lot_results.append({
                "lot_id": lot.id,
                "account_id": lot.account_id,
                "account_number": lot.account.account_number if lot.account else None,
                "security_id": lot.security_id,
                "symbol": lot.security.symbol if lot.security else None,
                "purchase_date": lot.purchase_date,
                "remaining_shares": lot.remaining_shares,
                "cost_basis_per_share": lot.cost_basis_per_share,
                "current_price": None,
                "proceeds": None,
                "cost_basis": lot.remaining_cost_basis,
                "gain_loss": None,
                "holding_period_days": (today - lot.purchase_date).days,
                "is_short_term": (today - lot.purchase_date).days < 365,
                "estimated_tax": None,
                "error": "No price available"
            })
            continue

        proceeds = lot.remaining_shares * current_price
        cost_basis = lot.remaining_cost_basis
        gain_loss = proceeds - cost_basis
        holding_days = (today - lot.purchase_date).days
        is_short_term = holding_days < 365

        # Estimate tax
        if gain_loss > 0:
            tax = gain_loss * (0.37 if is_short_term else 0.20)
        else:
            tax = 0.0

        lot_result = {
            "lot_id": lot.id,
            "account_id": lot.account_id,
            "account_number": lot.account.account_number if lot.account else None,
            "security_id": lot.security_id,
            "symbol": lot.security.symbol if lot.security else None,
            "purchase_date": lot.purchase_date,
            "remaining_shares": lot.remaining_shares,
            "cost_basis_per_share": lot.cost_basis_per_share,
            "current_price": current_price,
            "proceeds": proceeds,
            "cost_basis": cost_basis,
            "gain_loss": gain_loss,
            "gain_loss_pct": (gain_loss / cost_basis * 100) if cost_basis else 0,
            "holding_period_days": holding_days,
            "is_short_term": is_short_term,
            "estimated_tax": tax,
        }
        lot_results.append(lot_result)

        totals["total_proceeds"] += proceeds
        totals["total_cost_basis"] += cost_basis
        totals["total_gain_loss"] += gain_loss
        totals["total_shares"] += lot.remaining_shares
        totals["lot_count"] += 1
        totals["estimated_tax"] += tax
        if is_short_term:
            totals["short_term_gain_loss"] += gain_loss
        else:
            totals["long_term_gain_loss"] += gain_loss

    totals["gain_loss_pct"] = (
        (totals["total_gain_loss"] / totals["total_cost_basis"] * 100)
        if totals["total_cost_basis"] else 0
    )

    return {
        "lots": lot_results,
        "totals": totals,
        "missing_lot_ids": list(missing_ids) if missing_ids else [],
    }


# ============== Account List for Tax ==============

@router.get("/accounts")
def get_accounts_with_lots(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get accounts that have imported tax lot data (not transaction-built)."""
    from app.models.models import TaxLot
    from sqlalchemy import func

    results = db.query(
        Account.id,
        Account.account_number,
        Account.display_name,
        func.count(TaxLot.id).label("lot_count"),
        func.sum(TaxLot.remaining_shares).label("total_shares")
    ).outerjoin(TaxLot, and_(
        TaxLot.account_id == Account.id,
        TaxLot.is_closed == False,
        TaxLot.import_log_id.isnot(None)  # Only imported lots
    )).group_by(Account.id).order_by(Account.account_number).all()

    return [
        {
            "id": r.id,
            "account_number": r.account_number,
            "name": r.display_name,
            "lot_count": r.lot_count or 0,
            "total_shares": float(r.total_shares or 0)
        }
        for r in results
    ]


# Need to import and_ for the query
from sqlalchemy import and_
