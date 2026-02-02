"""
API endpoints for New Funds allocation feature.
Allows allocating new capital based on S&P 500 industry weights.
"""
import io
import csv
from datetime import date
from typing import List, Optional, Dict, Any
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from fastapi.responses import StreamingResponse
from sqlalchemy.orm import Session
from sqlalchemy import desc
from pydantic import BaseModel

from app.core.database import get_db
from app.api.auth import get_current_user
from app.models import User, Account, Security, PricesEOD
import logging

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/new-funds", tags=["new-funds"])


class IndustryWeight(BaseModel):
    """Industry weight from uploaded CSV"""
    industry: str
    sp500_weight: float  # As decimal (0.05 = 5%)
    ccm_weight: float  # Defaults to sp500_weight
    adjustment_bps: int = 0  # Basis point adjustment
    proforma_weight: float = 0.0
    active_weight: float = 0.0  # Over/under weight vs S&P
    dollar_allocation: float = 0.0


class TickerAllocation(BaseModel):
    """Individual ticker allocation within an industry"""
    ticker: str
    industry: str
    pct_of_industry: float  # Percentage of industry allocation (0-100)
    dollar_amount: float = 0.0
    shares: int = 0
    price: float = 0.0


class AllocationRequest(BaseModel):
    """Request to calculate allocation"""
    total_amount: float
    account_id: int
    industries: List[IndustryWeight]
    ticker_allocations: List[TickerAllocation] = []


class SchwabExportRequest(BaseModel):
    """Request to generate Schwab CSV"""
    account_number: str
    allocations: List[TickerAllocation]


@router.post("/parse-industry-csv")
async def parse_industry_csv(
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Parse uploaded CSV file containing S&P 500 industry weights.

    Expected CSV format:
    Industry,Weight
    Information Technology,0.2850
    Health Care,0.1350
    ...

    Weight can be decimal (0.28) or percentage (28.0 or 28%)
    """
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="File must be a CSV")

    try:
        content = await file.read()
        text = content.decode('utf-8')

        # Parse CSV
        reader = csv.DictReader(io.StringIO(text))
        industries = []

        for row in reader:
            # Find industry column (flexible naming)
            industry_name = None
            weight_value = None

            for key, value in row.items():
                key_lower = key.lower().strip()
                if key_lower in ['industry', 'sector', 'industry group', 'gics industry', 'name']:
                    industry_name = value.strip()
                elif key_lower in ['weight', 'sp500 weight', 's&p weight', 'sp500_weight', 'pct', 'percentage', '%']:
                    # Parse weight - handle percentage or decimal
                    weight_str = value.strip().replace('%', '')
                    try:
                        weight_value = float(weight_str)
                        # If > 1, assume it's a percentage and convert to decimal
                        if weight_value > 1:
                            weight_value = weight_value / 100
                    except ValueError:
                        continue

            if industry_name and weight_value is not None:
                industries.append({
                    "industry": industry_name,
                    "sp500_weight": weight_value,
                    "ccm_weight": weight_value,  # Default to S&P weight
                    "adjustment_bps": 0,
                    "proforma_weight": weight_value,
                    "active_weight": 0.0,
                    "dollar_allocation": 0.0
                })

        if not industries:
            raise HTTPException(
                status_code=400,
                detail="Could not parse any industries from CSV. Expected columns: Industry/Sector, Weight/Pct"
            )

        # Normalize weights to sum to 1.0
        total_weight = sum(i["sp500_weight"] for i in industries)
        if abs(total_weight - 1.0) > 0.01:  # More than 1% off
            logger.info(f"Normalizing weights from {total_weight} to 1.0")
            for i in industries:
                i["sp500_weight"] = i["sp500_weight"] / total_weight
                i["ccm_weight"] = i["sp500_weight"]
                i["proforma_weight"] = i["sp500_weight"]

        logger.info(f"Parsed {len(industries)} industries from CSV")

        return {
            "success": True,
            "industries": industries,
            "total_weight": sum(i["sp500_weight"] for i in industries)
        }

    except Exception as e:
        logger.error(f"Error parsing CSV: {str(e)}")
        raise HTTPException(status_code=400, detail=f"Error parsing CSV: {str(e)}")


@router.get("/accounts")
async def get_accounts(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
) -> List[Dict[str, Any]]:
    """Get list of accounts for dropdown"""
    accounts = db.query(Account).filter(Account.is_active == True).all()
    return [
        {
            "id": a.id,
            "account_number": a.account_number,
            "name": a.name or a.account_number
        }
        for a in accounts
    ]


@router.post("/calculate-allocation")
async def calculate_allocation(
    request: AllocationRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Calculate allocation amounts based on weights and adjustments.
    """
    total_amount = request.total_amount
    industries = request.industries

    # Calculate pro-forma weights with basis point adjustments
    for industry in industries:
        # Pro-forma = CCM weight + adjustment (in bps, so /10000)
        industry.proforma_weight = industry.ccm_weight + (industry.adjustment_bps / 10000)
        industry.active_weight = industry.proforma_weight - industry.sp500_weight
        industry.dollar_allocation = total_amount * industry.proforma_weight

    # Normalize pro-forma weights if they don't sum to 1
    total_proforma = sum(i.proforma_weight for i in industries)
    if abs(total_proforma - 1.0) > 0.0001:
        for industry in industries:
            industry.proforma_weight = industry.proforma_weight / total_proforma
            industry.dollar_allocation = total_amount * industry.proforma_weight
            industry.active_weight = industry.proforma_weight - industry.sp500_weight

    return {
        "success": True,
        "total_amount": total_amount,
        "industries": [i.dict() for i in industries],
        "total_proforma_weight": sum(i.proforma_weight for i in industries)
    }


@router.post("/get-ticker-price")
async def get_ticker_price(
    ticker: str,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Get current price for a ticker to calculate share count.
    """
    # Normalize ticker
    ticker = ticker.upper().strip()

    # Find security
    security = db.query(Security).filter(Security.symbol == ticker).first()
    if not security:
        # Try with dot notation
        ticker_alt = ticker.replace('-', '.')
        security = db.query(Security).filter(Security.symbol == ticker_alt).first()

    if not security:
        raise HTTPException(status_code=404, detail=f"Ticker {ticker} not found in database")

    # Get latest price
    latest_price = db.query(PricesEOD).filter(
        PricesEOD.security_id == security.id
    ).order_by(desc(PricesEOD.date)).first()

    if not latest_price:
        raise HTTPException(status_code=404, detail=f"No price data for {ticker}")

    return {
        "ticker": ticker,
        "price": float(latest_price.close),
        "price_date": latest_price.date.isoformat(),
        "security_name": security.asset_name
    }


@router.post("/calculate-shares")
async def calculate_shares(
    ticker: str,
    dollar_amount: float,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Calculate number of shares to buy for a given dollar amount.
    """
    price_data = await get_ticker_price(ticker, db, current_user)
    price = price_data["price"]

    # Calculate shares (round down to whole shares)
    shares = int(dollar_amount / price)
    actual_amount = shares * price

    return {
        "ticker": ticker,
        "price": price,
        "dollar_amount": dollar_amount,
        "shares": shares,
        "actual_amount": actual_amount,
        "remainder": dollar_amount - actual_amount
    }


@router.post("/generate-schwab-csv")
async def generate_schwab_csv(
    request: SchwabExportRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
) -> StreamingResponse:
    """
    Generate Schwab upload CSV file.

    Format:
    Column A: Account number
    Column B: B (Buy)
    Column C: Number of shares
    Column D: Ticker
    Column E: M (Market order)
    """
    account_number = request.account_number
    allocations = request.allocations

    # Create CSV in memory
    output = io.StringIO()
    writer = csv.writer(output)

    # No header row for Schwab format
    for alloc in allocations:
        if alloc.shares > 0:
            writer.writerow([
                account_number,  # Column A: Account number
                "B",             # Column B: Buy
                alloc.shares,    # Column C: Number of shares
                alloc.ticker,    # Column D: Ticker
                "M"              # Column E: Market order
            ])

    output.seek(0)

    # Return as downloadable CSV
    filename = f"schwab_allocation_{account_number}_{date.today().isoformat()}.csv"

    return StreamingResponse(
        iter([output.getvalue()]),
        media_type="text/csv",
        headers={
            "Content-Disposition": f"attachment; filename={filename}"
        }
    )


@router.post("/validate-allocation")
async def validate_allocation(
    request: AllocationRequest,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
) -> Dict[str, Any]:
    """
    Validate that allocation is complete and balanced.
    """
    total_amount = request.total_amount
    ticker_allocations = request.ticker_allocations

    # Calculate total allocated to tickers
    total_allocated = sum(a.dollar_amount for a in ticker_allocations)
    remaining = total_amount - total_allocated

    # Group allocations by industry
    industry_allocations = {}
    for alloc in ticker_allocations:
        if alloc.industry not in industry_allocations:
            industry_allocations[alloc.industry] = []
        industry_allocations[alloc.industry].append(alloc)

    # Check each industry's allocation totals
    industry_status = []
    for industry in request.industries:
        industry_allocs = industry_allocations.get(industry.industry, [])
        industry_total = sum(a.dollar_amount for a in industry_allocs)
        industry_target = industry.dollar_allocation

        industry_status.append({
            "industry": industry.industry,
            "target": industry_target,
            "allocated": industry_total,
            "remaining": industry_target - industry_total,
            "complete": abs(industry_target - industry_total) < 1.0  # Within $1
        })

    return {
        "total_amount": total_amount,
        "total_allocated": total_allocated,
        "remaining": remaining,
        "pct_allocated": (total_allocated / total_amount * 100) if total_amount > 0 else 0,
        "industry_status": industry_status,
        "is_complete": remaining < 1.0  # Within $1 of fully allocated
    }
