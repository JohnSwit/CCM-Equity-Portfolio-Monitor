"""
Tax Optimization API endpoints - tax lot management, gain/loss tracking,
wash sale detection, and tax-loss harvesting recommendations.
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import date

from app.core.database import get_db
from app.api.auth import get_current_user
from app.models.models import User, Account, Security
from app.models.schemas import (
    TaxLotResponse, TaxLotListResponse, RealizedGainResponse, RealizedGainListResponse,
    TaxSummaryResponse, TaxLossHarvestingResponse, TaxLossHarvestingCandidate,
    WashSaleCheckResult, TradeImpactAnalysis, TaxLotSellSuggestion, SellOrderRequest
)
from app.services.tax_optimization import TaxService

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

    total_lots = 0
    results = []

    for account in accounts:
        try:
            lots_created = tax_service.build_tax_lots_for_account(account.id)
            total_lots += lots_created
            results.append({
                "account_id": account.id,
                "account_number": account.account_number,
                "lots_created": lots_created
            })
        except Exception as e:
            results.append({
                "account_id": account.id,
                "account_number": account.account_number,
                "error": str(e)
            })

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


# ============== Account List for Tax ==============

@router.get("/accounts")
def get_accounts_with_lots(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get accounts that have tax lot data."""
    from app.models.models import TaxLot
    from sqlalchemy import func

    results = db.query(
        Account.id,
        Account.account_number,
        Account.name,
        func.count(TaxLot.id).label("lot_count"),
        func.sum(TaxLot.remaining_shares).label("total_shares")
    ).outerjoin(TaxLot, and_(
        TaxLot.account_id == Account.id,
        TaxLot.is_closed == False
    )).group_by(Account.id).order_by(Account.account_number).all()

    return [
        {
            "id": r.id,
            "account_number": r.account_number,
            "name": r.name,
            "lot_count": r.lot_count or 0,
            "total_shares": float(r.total_shares or 0)
        }
        for r in results
    ]


# Need to import and_ for the query
from sqlalchemy import and_
