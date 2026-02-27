"""
Active Coverage API endpoints - manage analyst coverage and Excel model integration
"""
from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import and_, func
from typing import List, Optional
from datetime import date

from app.core.database import get_db
from app.api.auth import get_current_user
from app.models.models import (
    User, Analyst, ActiveCoverage, CoverageModelData, CoverageDocument, CoverageModelSnapshot,
    Security, PositionsEOD, PricesEOD, Group, GroupMember, ViewType
)
from app.models.schemas import (
    AnalystResponse, AnalystCreate,
    ActiveCoverageCreate, ActiveCoverageUpdate, ActiveCoverageResponse,
    ActiveCoverageListResponse, CoverageModelDataResponse, MetricEstimates, MarginEstimates,
    CoverageDocumentResponse, CoverageModelSnapshotResponse, CoverageModelSnapshotDiff
)

router = APIRouter(prefix="/coverage", tags=["coverage"])


# ============== Analyst Endpoints ==============

@router.get("/analysts", response_model=List[AnalystResponse])
def list_analysts(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """List all analysts"""
    analysts = db.query(Analyst).filter(Analyst.is_active == True).all()
    return analysts


@router.post("/analysts", response_model=AnalystResponse)
def create_analyst(
    data: AnalystCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Create a new analyst"""
    existing = db.query(Analyst).filter(Analyst.name == data.name).first()
    if existing:
        raise HTTPException(status_code=400, detail="Analyst already exists")

    analyst = Analyst(name=data.name)
    db.add(analyst)
    db.commit()
    db.refresh(analyst)
    return analyst


@router.delete("/analysts/{analyst_id}")
def delete_analyst(
    analyst_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Deactivate an analyst"""
    analyst = db.query(Analyst).filter(Analyst.id == analyst_id).first()
    if not analyst:
        raise HTTPException(status_code=404, detail="Analyst not found")

    analyst.is_active = False
    db.commit()
    return {"status": "success"}


# ============== Coverage Endpoints ==============

def _get_firm_group(db: Session):
    """Get the firm-level group"""
    from app.models.models import GroupType
    return db.query(Group).filter(Group.type == GroupType.FIRM).first()


def _get_firm_account_ids(db: Session) -> List[int]:
    """Get all account IDs in the firm"""
    firm_group = _get_firm_group(db)
    if not firm_group:
        return []

    members = db.query(GroupMember.member_id).filter(
        GroupMember.group_id == firm_group.id,
        GroupMember.member_type == "account"
    ).all()
    return [m.member_id for m in members]


def _get_ticker_holdings(db: Session, ticker: str, account_ids: List[int], as_of_date: date = None):
    """Get holdings data for a ticker across firm accounts"""
    if not as_of_date:
        as_of_date = date.today()

    # Find the security
    security = db.query(Security).filter(Security.symbol == ticker.upper()).first()
    if not security:
        return None, None, None

    # Get latest position date
    latest_pos_date = db.query(func.max(PositionsEOD.date)).filter(
        and_(
            PositionsEOD.account_id.in_(account_ids),
            PositionsEOD.security_id == security.id,
            PositionsEOD.date <= as_of_date
        )
    ).scalar()

    if not latest_pos_date:
        return None, None, None

    # Get total shares
    total_shares = db.query(func.sum(PositionsEOD.shares)).filter(
        and_(
            PositionsEOD.account_id.in_(account_ids),
            PositionsEOD.security_id == security.id,
            PositionsEOD.date == latest_pos_date
        )
    ).scalar() or 0

    # Get current price
    latest_price = db.query(PricesEOD.close).filter(
        and_(
            PricesEOD.security_id == security.id,
            PricesEOD.date <= as_of_date
        )
    ).order_by(PricesEOD.date.desc()).first()

    current_price = latest_price[0] if latest_price else None
    market_value = (total_shares * current_price) if current_price else None

    return market_value, current_price, total_shares


def _get_total_firm_value(db: Session, account_ids: List[int], as_of_date: date = None):
    """Calculate total firm portfolio value"""
    if not as_of_date:
        as_of_date = date.today()

    # Get latest position date across all accounts
    latest_pos_date = db.query(func.max(PositionsEOD.date)).filter(
        and_(
            PositionsEOD.account_id.in_(account_ids),
            PositionsEOD.date <= as_of_date
        )
    ).scalar()

    if not latest_pos_date:
        return 0

    # Get all positions on that date with prices
    from sqlalchemy.orm import aliased
    positions = db.query(
        PositionsEOD.security_id,
        func.sum(PositionsEOD.shares).label('total_shares')
    ).filter(
        and_(
            PositionsEOD.account_id.in_(account_ids),
            PositionsEOD.date == latest_pos_date
        )
    ).group_by(PositionsEOD.security_id).all()

    total_value = 0
    for pos in positions:
        price = db.query(PricesEOD.close).filter(
            and_(
                PricesEOD.security_id == pos.security_id,
                PricesEOD.date <= as_of_date
            )
        ).order_by(PricesEOD.date.desc()).first()
        if price:
            total_value += pos.total_shares * price[0]

    return total_value


def _build_coverage_response(
    db: Session,
    coverage: ActiveCoverage,
    account_ids: List[int],
    total_firm_value: float,
    include_model_data: bool = True
) -> dict:
    """Build a coverage response with portfolio and model data"""
    # Get portfolio holdings
    market_value, current_price, _ = _get_ticker_holdings(db, coverage.ticker, account_ids)
    weight_pct = (market_value / total_firm_value * 100) if market_value and total_firm_value else None

    # Build model data response
    model_data = None
    if include_model_data:
        cached_data = db.query(CoverageModelData).filter(
            CoverageModelData.coverage_id == coverage.id
        ).first()

        if cached_data:
            model_data = _build_model_data_response(cached_data, current_price)

    # Get documents
    documents = db.query(CoverageDocument).filter(
        CoverageDocument.coverage_id == coverage.id
    ).order_by(CoverageDocument.uploaded_at.desc()).all()

    # Get snapshots
    snapshots = db.query(CoverageModelSnapshot).filter(
        CoverageModelSnapshot.coverage_id == coverage.id
    ).order_by(CoverageModelSnapshot.created_at.desc()).all()

    return {
        "id": coverage.id,
        "ticker": coverage.ticker,
        "primary_analyst": coverage.primary_analyst,
        "secondary_analyst": coverage.secondary_analyst,
        "model_path": coverage.model_path,
        "model_share_link": coverage.model_share_link,
        "notes": coverage.notes,
        "is_active": coverage.is_active,
        "model_updated": coverage.model_updated or False,
        "thesis": coverage.thesis,
        "bull_case": coverage.bull_case,
        "bear_case": coverage.bear_case,
        "alert": coverage.alert,
        "has_alert": bool(coverage.alert and coverage.alert.strip()),
        "market_value": market_value,
        "weight_pct": weight_pct,
        "current_price": current_price,
        "model_data": model_data,
        "documents": documents,
        "snapshots": snapshots,
        "created_at": coverage.created_at,
        "updated_at": coverage.updated_at
    }


def _build_model_data_response(data: CoverageModelData, current_price: float = None) -> dict:
    """Build model data response with calculated fields"""
    # Calculate upside percentages
    ccm_upside = None
    street_upside = None
    ccm_vs_street_diff = None

    if current_price and current_price > 0:
        if data.ccm_fair_value:
            ccm_upside = ((data.ccm_fair_value / current_price) - 1) * 100
        if data.street_price_target:
            street_upside = ((data.street_price_target / current_price) - 1) * 100

    if data.ccm_fair_value and data.street_price_target and data.street_price_target > 0:
        ccm_vs_street_diff = ((data.ccm_fair_value / data.street_price_target) - 1) * 100

    # Build metric estimates with growth rates
    def build_metric(prefix: str, revenue_data=None):
        minus1 = getattr(data, f"{prefix}_ccm_minus1yr")
        yr1 = getattr(data, f"{prefix}_ccm_1yr")
        yr2 = getattr(data, f"{prefix}_ccm_2yr")
        yr3 = getattr(data, f"{prefix}_ccm_3yr")
        st_minus1 = getattr(data, f"{prefix}_street_minus1yr")
        st_yr1 = getattr(data, f"{prefix}_street_1yr")
        st_yr2 = getattr(data, f"{prefix}_street_2yr")
        st_yr3 = getattr(data, f"{prefix}_street_3yr")

        # Calculate growth rates
        def calc_growth(curr, prev):
            if curr is not None and prev is not None and prev != 0:
                return ((curr / prev) - 1) * 100
            return None

        # Calculate CCM vs Street diff
        def calc_diff(ccm, street):
            if ccm is not None and street is not None and street != 0:
                return ((ccm / street) - 1) * 100
            return None

        return MetricEstimates(
            ccm_minus1yr=minus1,
            ccm_1yr=yr1,
            ccm_2yr=yr2,
            ccm_3yr=yr3,
            street_minus1yr=st_minus1,
            street_1yr=st_yr1,
            street_2yr=st_yr2,
            street_3yr=st_yr3,
            growth_ccm_1yr=calc_growth(yr1, minus1),
            growth_ccm_2yr=calc_growth(yr2, yr1),
            growth_ccm_3yr=calc_growth(yr3, yr2),
            growth_street_1yr=calc_growth(st_yr1, st_minus1),
            growth_street_2yr=calc_growth(st_yr2, st_yr1),
            growth_street_3yr=calc_growth(st_yr3, st_yr2),
            diff_1yr_pct=calc_diff(yr1, st_yr1),
            diff_2yr_pct=calc_diff(yr2, st_yr2),
            diff_3yr_pct=calc_diff(yr3, st_yr3)
        )

    # Build margin estimates (EBITDA margin = EBITDA / Revenue)
    def build_margin(numerator_prefix: str):
        def calc_margin(num, rev):
            if num is not None and rev is not None and rev != 0:
                return (num / rev) * 100
            return None

        return MarginEstimates(
            ccm_minus1yr=calc_margin(
                getattr(data, f"{numerator_prefix}_ccm_minus1yr"),
                data.revenue_ccm_minus1yr
            ),
            ccm_1yr=calc_margin(
                getattr(data, f"{numerator_prefix}_ccm_1yr"),
                data.revenue_ccm_1yr
            ),
            ccm_2yr=calc_margin(
                getattr(data, f"{numerator_prefix}_ccm_2yr"),
                data.revenue_ccm_2yr
            ),
            ccm_3yr=calc_margin(
                getattr(data, f"{numerator_prefix}_ccm_3yr"),
                data.revenue_ccm_3yr
            ),
            street_minus1yr=calc_margin(
                getattr(data, f"{numerator_prefix}_street_minus1yr"),
                data.revenue_street_minus1yr
            ),
            street_1yr=calc_margin(
                getattr(data, f"{numerator_prefix}_street_1yr"),
                data.revenue_street_1yr
            ),
            street_2yr=calc_margin(
                getattr(data, f"{numerator_prefix}_street_2yr"),
                data.revenue_street_2yr
            ),
            street_3yr=calc_margin(
                getattr(data, f"{numerator_prefix}_street_3yr"),
                data.revenue_street_3yr
            )
        )

    return CoverageModelDataResponse(
        irr_3yr=data.irr_3yr,
        ccm_fair_value=data.ccm_fair_value,
        street_price_target=data.street_price_target,
        current_price=current_price,
        ccm_upside_pct=ccm_upside,
        street_upside_pct=street_upside,
        ccm_vs_street_diff_pct=ccm_vs_street_diff,
        revenue=build_metric("revenue"),
        ebitda=build_metric("ebitda"),
        eps=build_metric("eps"),
        fcf=build_metric("fcf"),
        ebitda_margin=build_margin("ebitda"),
        fcf_margin=build_margin("fcf"),
        data_as_of=data.data_as_of,
        last_refreshed=data.last_refreshed
    )


@router.get("", response_model=ActiveCoverageListResponse)
def list_coverage(
    active_only: bool = True,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """List all active coverage tickers with portfolio and model data"""
    query = db.query(ActiveCoverage)
    if active_only:
        query = query.filter(ActiveCoverage.is_active == True)

    coverages = query.order_by(ActiveCoverage.ticker).all()

    account_ids = _get_firm_account_ids(db)
    total_firm_value = _get_total_firm_value(db, account_ids) if account_ids else 0

    coverage_responses = [
        _build_coverage_response(db, c, account_ids, total_firm_value)
        for c in coverages
    ]

    return ActiveCoverageListResponse(
        coverages=coverage_responses,
        total_firm_value=total_firm_value
    )


@router.get("/{coverage_id}", response_model=ActiveCoverageResponse)
def get_coverage(
    coverage_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get a single coverage item with full details"""
    coverage = db.query(ActiveCoverage).filter(ActiveCoverage.id == coverage_id).first()
    if not coverage:
        raise HTTPException(status_code=404, detail="Coverage not found")

    account_ids = _get_firm_account_ids(db)
    total_firm_value = _get_total_firm_value(db, account_ids) if account_ids else 0

    return _build_coverage_response(db, coverage, account_ids, total_firm_value)


@router.post("", response_model=ActiveCoverageResponse)
def create_coverage(
    data: ActiveCoverageCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Add a new ticker to active coverage"""
    # Check if ticker already exists
    existing = db.query(ActiveCoverage).filter(
        ActiveCoverage.ticker == data.ticker.upper()
    ).first()

    if existing:
        if existing.is_active:
            raise HTTPException(status_code=400, detail="Ticker already in coverage")
        # Reactivate inactive coverage and update fields
        existing.is_active = True
        existing.primary_analyst_id = data.primary_analyst_id
        existing.secondary_analyst_id = data.secondary_analyst_id
        existing.model_path = data.model_path
        existing.model_share_link = data.model_share_link
        existing.notes = data.notes
        db.commit()
        db.refresh(existing)
        coverage = existing
    else:
        coverage = ActiveCoverage(
            ticker=data.ticker.upper(),
            primary_analyst_id=data.primary_analyst_id,
            secondary_analyst_id=data.secondary_analyst_id,
            model_path=data.model_path,
            model_share_link=data.model_share_link,
            notes=data.notes
        )
        db.add(coverage)
        db.commit()
        db.refresh(coverage)

    account_ids = _get_firm_account_ids(db)
    total_firm_value = _get_total_firm_value(db, account_ids) if account_ids else 0

    return _build_coverage_response(db, coverage, account_ids, total_firm_value)


@router.put("/{coverage_id}", response_model=ActiveCoverageResponse)
def update_coverage(
    coverage_id: int,
    data: ActiveCoverageUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Update coverage details (analysts, model path, etc.)"""
    coverage = db.query(ActiveCoverage).filter(ActiveCoverage.id == coverage_id).first()
    if not coverage:
        raise HTTPException(status_code=404, detail="Coverage not found")

    if data.primary_analyst_id is not None:
        coverage.primary_analyst_id = data.primary_analyst_id
    if data.secondary_analyst_id is not None:
        coverage.secondary_analyst_id = data.secondary_analyst_id
    if data.model_path is not None:
        coverage.model_path = data.model_path
    if data.model_share_link is not None:
        coverage.model_share_link = data.model_share_link
    if data.notes is not None:
        coverage.notes = data.notes
    if data.is_active is not None:
        coverage.is_active = data.is_active
    if data.model_updated is not None:
        coverage.model_updated = data.model_updated
    if data.thesis is not None:
        coverage.thesis = data.thesis
    if data.bull_case is not None:
        coverage.bull_case = data.bull_case
    if data.bear_case is not None:
        coverage.bear_case = data.bear_case
    if data.alert is not None:
        coverage.alert = data.alert

    db.commit()
    db.refresh(coverage)

    account_ids = _get_firm_account_ids(db)
    total_firm_value = _get_total_firm_value(db, account_ids) if account_ids else 0

    return _build_coverage_response(db, coverage, account_ids, total_firm_value)


@router.delete("/{coverage_id}")
def delete_coverage(
    coverage_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Remove a ticker from active coverage (soft delete)"""
    coverage = db.query(ActiveCoverage).filter(ActiveCoverage.id == coverage_id).first()
    if not coverage:
        raise HTTPException(status_code=404, detail="Coverage not found")

    coverage.is_active = False
    db.commit()
    return {"status": "success"}


@router.post("/{coverage_id}/refresh-model-data")
def refresh_model_data(
    coverage_id: int,
    create_snapshot: bool = True,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Refresh model data from the linked Excel file.
    This endpoint triggers a read of the Excel model's API tab.
    Creates a snapshot of the previous data for diff comparison.
    """
    coverage = db.query(ActiveCoverage).filter(ActiveCoverage.id == coverage_id).first()
    if not coverage:
        raise HTTPException(status_code=404, detail="Coverage not found")

    if not coverage.model_path:
        raise HTTPException(status_code=400, detail="No model path configured for this coverage")

    # Import the Excel parsing service
    from app.services.excel_model_parser import parse_excel_model
    from datetime import datetime

    try:
        # Get existing data for snapshot
        cached = db.query(CoverageModelData).filter(
            CoverageModelData.coverage_id == coverage_id
        ).first()

        # Create snapshot of existing data before updating
        if create_snapshot and cached:
            snapshot = CoverageModelSnapshot(
                coverage_id=coverage_id,
                ccm_fair_value=cached.ccm_fair_value,
                street_price_target=cached.street_price_target,
                irr_3yr=cached.irr_3yr,
                revenue_ccm_1yr=cached.revenue_ccm_1yr,
                revenue_ccm_2yr=cached.revenue_ccm_2yr,
                revenue_ccm_3yr=cached.revenue_ccm_3yr,
                ebitda_ccm_1yr=cached.ebitda_ccm_1yr,
                ebitda_ccm_2yr=cached.ebitda_ccm_2yr,
                ebitda_ccm_3yr=cached.ebitda_ccm_3yr,
                fcf_ccm_1yr=cached.fcf_ccm_1yr,
                fcf_ccm_2yr=cached.fcf_ccm_2yr,
                fcf_ccm_3yr=cached.fcf_ccm_3yr,
                eps_ccm_1yr=cached.eps_ccm_1yr,
                eps_ccm_2yr=cached.eps_ccm_2yr,
                eps_ccm_3yr=cached.eps_ccm_3yr,
            )
            db.add(snapshot)

        model_data = parse_excel_model(coverage.model_path)

        # Update or create cached data
        if not cached:
            cached = CoverageModelData(coverage_id=coverage_id)
            db.add(cached)

        # Update all fields
        for key, value in model_data.items():
            if hasattr(cached, key):
                setattr(cached, key, value)

        cached.last_refreshed = datetime.utcnow()

        db.commit()
        db.refresh(cached)

        return {"status": "success", "message": "Model data refreshed", "snapshot_created": create_snapshot and cached is not None}

    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Excel model file not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error parsing model: {str(e)}")


@router.post("/refresh-all-models")
def refresh_all_models(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Refresh model data for all coverage items with linked Excel models"""
    coverages = db.query(ActiveCoverage).filter(
        ActiveCoverage.is_active == True,
        ActiveCoverage.model_path != None
    ).all()

    results = {"success": 0, "failed": 0, "errors": []}

    from app.services.excel_model_parser import parse_excel_model
    from datetime import datetime

    for coverage in coverages:
        try:
            model_data = parse_excel_model(coverage.model_path)

            cached = db.query(CoverageModelData).filter(
                CoverageModelData.coverage_id == coverage.id
            ).first()

            if not cached:
                cached = CoverageModelData(coverage_id=coverage.id)
                db.add(cached)

            for key, value in model_data.items():
                if hasattr(cached, key):
                    setattr(cached, key, value)

            cached.last_refreshed = datetime.utcnow()
            results["success"] += 1

        except Exception as e:
            results["failed"] += 1
            results["errors"].append({"ticker": coverage.ticker, "error": str(e)})

    db.commit()
    return results


# ============== Initialization Endpoint ==============

@router.post("/init-analysts")
def init_analysts(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Initialize default analysts (Max, John, Brodie)"""
    default_analysts = ["Max", "John", "Brodie"]
    created = []

    for name in default_analysts:
        existing = db.query(Analyst).filter(Analyst.name == name).first()
        if not existing:
            analyst = Analyst(name=name)
            db.add(analyst)
            created.append(name)

    db.commit()
    return {"status": "success", "created": created}


# ============== Document Endpoints ==============

@router.get("/{coverage_id}/documents", response_model=List[CoverageDocumentResponse])
def list_documents(
    coverage_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """List all documents for a coverage item"""
    coverage = db.query(ActiveCoverage).filter(ActiveCoverage.id == coverage_id).first()
    if not coverage:
        raise HTTPException(status_code=404, detail="Coverage not found")

    documents = db.query(CoverageDocument).filter(
        CoverageDocument.coverage_id == coverage_id
    ).order_by(CoverageDocument.uploaded_at.desc()).all()

    return documents


@router.post("/{coverage_id}/documents", response_model=CoverageDocumentResponse)
def add_document(
    coverage_id: int,
    file_name: str,
    file_path: str,
    file_type: Optional[str] = None,
    file_size: Optional[int] = None,
    description: Optional[str] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Add a document reference to a coverage item"""
    coverage = db.query(ActiveCoverage).filter(ActiveCoverage.id == coverage_id).first()
    if not coverage:
        raise HTTPException(status_code=404, detail="Coverage not found")

    document = CoverageDocument(
        coverage_id=coverage_id,
        file_name=file_name,
        file_path=file_path,
        file_type=file_type,
        file_size=file_size,
        description=description
    )
    db.add(document)
    db.commit()
    db.refresh(document)

    return document


@router.delete("/{coverage_id}/documents/{document_id}")
def delete_document(
    coverage_id: int,
    document_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Delete a document reference"""
    document = db.query(CoverageDocument).filter(
        CoverageDocument.id == document_id,
        CoverageDocument.coverage_id == coverage_id
    ).first()

    if not document:
        raise HTTPException(status_code=404, detail="Document not found")

    db.delete(document)
    db.commit()
    return {"status": "success"}


# ============== Snapshot Endpoints ==============

@router.get("/{coverage_id}/snapshots", response_model=List[CoverageModelSnapshotResponse])
def list_snapshots(
    coverage_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """List all model snapshots for a coverage item"""
    coverage = db.query(ActiveCoverage).filter(ActiveCoverage.id == coverage_id).first()
    if not coverage:
        raise HTTPException(status_code=404, detail="Coverage not found")

    snapshots = db.query(CoverageModelSnapshot).filter(
        CoverageModelSnapshot.coverage_id == coverage_id
    ).order_by(CoverageModelSnapshot.created_at.desc()).all()

    return snapshots


@router.get("/{coverage_id}/snapshots/{snapshot_id}/diff")
def get_snapshot_diff(
    coverage_id: int,
    snapshot_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get diff between a snapshot and current model data"""
    snapshot = db.query(CoverageModelSnapshot).filter(
        CoverageModelSnapshot.id == snapshot_id,
        CoverageModelSnapshot.coverage_id == coverage_id
    ).first()

    if not snapshot:
        raise HTTPException(status_code=404, detail="Snapshot not found")

    current_data = db.query(CoverageModelData).filter(
        CoverageModelData.coverage_id == coverage_id
    ).first()

    if not current_data:
        raise HTTPException(status_code=404, detail="No current model data")

    # Calculate diffs for key metrics
    def calc_diff(field_name: str, old_val, new_val):
        change = None
        change_pct = None
        if old_val is not None and new_val is not None:
            change = new_val - old_val
            if old_val != 0:
                change_pct = (change / abs(old_val)) * 100
        return {
            "field": field_name,
            "old_value": old_val,
            "new_value": new_val,
            "change": change,
            "change_pct": change_pct
        }

    diffs = [
        calc_diff("CCM Fair Value", snapshot.ccm_fair_value, current_data.ccm_fair_value),
        calc_diff("Street Price Target", snapshot.street_price_target, current_data.street_price_target),
        calc_diff("3Y IRR", snapshot.irr_3yr, current_data.irr_3yr),
        calc_diff("Revenue 1Y", snapshot.revenue_ccm_1yr, current_data.revenue_ccm_1yr),
        calc_diff("Revenue 2Y", snapshot.revenue_ccm_2yr, current_data.revenue_ccm_2yr),
        calc_diff("Revenue 3Y", snapshot.revenue_ccm_3yr, current_data.revenue_ccm_3yr),
        calc_diff("EBITDA 1Y", snapshot.ebitda_ccm_1yr, current_data.ebitda_ccm_1yr),
        calc_diff("EBITDA 2Y", snapshot.ebitda_ccm_2yr, current_data.ebitda_ccm_2yr),
        calc_diff("EBITDA 3Y", snapshot.ebitda_ccm_3yr, current_data.ebitda_ccm_3yr),
        calc_diff("FCF 1Y", snapshot.fcf_ccm_1yr, current_data.fcf_ccm_1yr),
        calc_diff("FCF 2Y", snapshot.fcf_ccm_2yr, current_data.fcf_ccm_2yr),
        calc_diff("FCF 3Y", snapshot.fcf_ccm_3yr, current_data.fcf_ccm_3yr),
        calc_diff("EPS 1Y", snapshot.eps_ccm_1yr, current_data.eps_ccm_1yr),
        calc_diff("EPS 2Y", snapshot.eps_ccm_2yr, current_data.eps_ccm_2yr),
        calc_diff("EPS 3Y", snapshot.eps_ccm_3yr, current_data.eps_ccm_3yr),
    ]

    return {
        "snapshot_id": snapshot_id,
        "snapshot_date": snapshot.created_at,
        "diffs": diffs
    }


@router.delete("/{coverage_id}/snapshots/{snapshot_id}")
def delete_snapshot(
    coverage_id: int,
    snapshot_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Delete a model snapshot"""
    snapshot = db.query(CoverageModelSnapshot).filter(
        CoverageModelSnapshot.id == snapshot_id,
        CoverageModelSnapshot.coverage_id == coverage_id
    ).first()

    if not snapshot:
        raise HTTPException(status_code=404, detail="Snapshot not found")

    db.delete(snapshot)
    db.commit()
    return {"status": "success"}
