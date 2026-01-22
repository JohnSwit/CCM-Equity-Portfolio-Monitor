from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import and_, func, desc
from datetime import date, datetime
from typing import Optional
from app.core.database import get_db
from app.api.auth import get_current_user
from app.models import (
    User, PortfolioValueEOD, ReturnsEOD, RiskEOD,
    BenchmarkMetric, FactorRegression, ViewType, Account, Group,
    PositionsEOD, Security, PricesEOD
)
from app.models.schemas import (
    SummaryResponse, ReturnDataPoint, HoldingsResponse, HoldingRow,
    RiskResponse, BenchmarkMetricResponse, FactorResponse, FactorExposure,
    UnpricedInstrument
)
from app.services.returns import ReturnsEngine
from app.services.positions import PositionsEngine

router = APIRouter(prefix="/analytics", tags=["analytics"])


def parse_view_type(view_type_str: str) -> ViewType:
    """Parse view type from string"""
    mapping = {
        'account': ViewType.ACCOUNT,
        'group': ViewType.GROUP,
        'firm': ViewType.FIRM
    }
    return mapping.get(view_type_str, ViewType.ACCOUNT)


def get_db_view_type(vt: ViewType) -> ViewType:
    """
    Convert view type for database queries.
    FIRM views are stored as GROUP in the database.
    """
    return ViewType.GROUP if vt == ViewType.FIRM else vt


@router.get("/summary", response_model=SummaryResponse)
def get_summary(
    view_type: str = Query(...),
    view_id: int = Query(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get summary analytics for a view"""
    vt = parse_view_type(view_type)
    db_vt = get_db_view_type(vt)

    # Get latest value
    latest_value = db.query(PortfolioValueEOD).filter(
        and_(
            PortfolioValueEOD.view_type == db_vt,
            PortfolioValueEOD.view_id == view_id
        )
    ).order_by(desc(PortfolioValueEOD.date)).first()

    if not latest_value:
        raise HTTPException(status_code=404, detail="No data found for this view")

    # Get view name
    view_name = ""
    if vt == ViewType.ACCOUNT:
        account = db.query(Account).filter(Account.id == view_id).first()
        view_name = account.display_name if account else f"Account {view_id}"
    else:  # GROUP or FIRM
        group = db.query(Group).filter(Group.id == view_id).first()
        view_name = group.name if group else f"Group {view_id}"

    # Compute period returns
    returns_engine = ReturnsEngine(db)
    period_returns = returns_engine.compute_period_returns(db_vt, view_id, latest_value.date)

    return SummaryResponse(
        view_type=view_type,
        view_id=view_id,
        view_name=view_name,
        total_value=latest_value.total_value,
        as_of_date=latest_value.date,
        data_last_updated=latest_value.created_at,
        return_1m=period_returns.get('1M'),
        return_3m=period_returns.get('3M'),
        return_ytd=period_returns.get('YTD'),
        return_1y=period_returns.get('1Y'),
        return_3y=period_returns.get('3Y'),
        return_inception=period_returns.get('inception')
    )


@router.get("/returns")
def get_returns(
    view_type: str = Query(...),
    view_id: int = Query(...),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get returns series for a view"""
    vt = parse_view_type(view_type)
    db_vt = get_db_view_type(vt)

    query = db.query(ReturnsEOD).filter(
        and_(
            ReturnsEOD.view_type == db_vt,
            ReturnsEOD.view_id == view_id
        )
    )

    if start_date:
        query = query.filter(ReturnsEOD.date >= start_date)
    if end_date:
        query = query.filter(ReturnsEOD.date <= end_date)

    returns = query.order_by(ReturnsEOD.date).all()

    return [
        ReturnDataPoint(
            date=r.date,
            return_value=r.twr_return,
            index_value=r.twr_index
        )
        for r in returns
    ]


@router.get("/holdings", response_model=HoldingsResponse)
def get_holdings(
    view_type: str = Query(...),
    view_id: int = Query(...),
    as_of_date: Optional[date] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get holdings for a view"""
    vt = parse_view_type(view_type)
    db_vt = get_db_view_type(vt)

    if not as_of_date:
        # Get latest date
        latest = db.query(PortfolioValueEOD.date).filter(
            and_(
                PortfolioValueEOD.view_type == db_vt,
                PortfolioValueEOD.view_id == view_id
            )
        ).order_by(desc(PortfolioValueEOD.date)).first()

        if not latest:
            raise HTTPException(status_code=404, detail="No data found")

        as_of_date = latest[0]

    # Get holdings
    if vt == ViewType.ACCOUNT:
        engine = PositionsEngine(db)
        holdings_data = engine.get_holdings_as_of(view_id, as_of_date)
    else:
        # For groups, aggregate holdings from member accounts
        from app.services.groups import GroupsEngine
        groups_engine = GroupsEngine(db)
        account_ids = groups_engine.get_group_account_ids(view_id)

        # Aggregate positions
        positions_engine = PositionsEngine(db)
        all_holdings = {}

        for account_id in account_ids:
            holdings = positions_engine.get_holdings_as_of(account_id, as_of_date)
            for holding in holdings:
                symbol = holding['symbol']
                if symbol not in all_holdings:
                    all_holdings[symbol] = holding.copy()
                else:
                    all_holdings[symbol]['shares'] += holding['shares']
                    all_holdings[symbol]['market_value'] += holding['market_value']

        holdings_data = list(all_holdings.values())

    # Compute total and weights
    total_value = sum(h['market_value'] for h in holdings_data if h['has_price'])

    holdings = []
    for h in holdings_data:
        if h['has_price']:
            weight = h['market_value'] / total_value if total_value > 0 else 0
            holdings.append(HoldingRow(
                symbol=h['symbol'],
                asset_name=h['asset_name'],
                shares=h['shares'],
                price=h['price'],
                market_value=h['market_value'],
                weight=weight
            ))

    return HoldingsResponse(
        as_of_date=as_of_date,
        holdings=sorted(holdings, key=lambda x: x.market_value, reverse=True),
        total_value=total_value
    )


@router.get("/risk", response_model=RiskResponse)
def get_risk(
    view_type: str = Query(...),
    view_id: int = Query(...),
    as_of_date: Optional[date] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get risk metrics for a view"""
    vt = parse_view_type(view_type)
    db_vt = get_db_view_type(vt)

    if not as_of_date:
        # Get latest date
        latest = db.query(RiskEOD.date).filter(
            and_(
                RiskEOD.view_type == db_vt,
                RiskEOD.view_id == view_id
            )
        ).order_by(desc(RiskEOD.date)).first()

        if not latest:
            raise HTTPException(status_code=404, detail="No risk data found")

        as_of_date = latest[0]

    risk = db.query(RiskEOD).filter(
        and_(
            RiskEOD.view_type == db_vt,
            RiskEOD.view_id == view_id,
            RiskEOD.date == as_of_date
        )
    ).first()

    if not risk:
        raise HTTPException(status_code=404, detail="No risk data found")

    return RiskResponse(
        as_of_date=risk.date,
        vol_21d=risk.vol_21d,
        vol_63d=risk.vol_63d,
        max_drawdown_1y=risk.max_drawdown_1y,
        var_95_1d_hist=risk.var_95_1d_hist
    )


@router.get("/benchmark", response_model=BenchmarkMetricResponse)
def get_benchmark_metrics(
    view_type: str = Query(...),
    view_id: int = Query(...),
    benchmark: str = Query(...),
    window: int = Query(252),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get benchmark metrics for a view"""
    vt = parse_view_type(view_type)
    db_vt = get_db_view_type(vt)

    # Get latest metrics
    metrics = db.query(BenchmarkMetric).filter(
        and_(
            BenchmarkMetric.view_type == db_vt,
            BenchmarkMetric.view_id == view_id,
            BenchmarkMetric.benchmark_code == benchmark
        )
    ).order_by(desc(BenchmarkMetric.as_of_date)).first()

    if not metrics:
        raise HTTPException(status_code=404, detail="No benchmark metrics found")

    # Calculate excess return
    excess_return_252 = None
    if metrics.beta_252 is not None and metrics.alpha_252 is not None:
        # This is approximate; ideally we'd compute from actual returns
        excess_return_252 = metrics.alpha_252

    return BenchmarkMetricResponse(
        benchmark_code=metrics.benchmark_code,
        as_of_date=metrics.as_of_date,
        beta_252=metrics.beta_252,
        alpha_252=metrics.alpha_252,
        te_252=metrics.te_252,
        corr_252=metrics.corr_252,
        excess_return_252=excess_return_252
    )


@router.get("/factors", response_model=FactorResponse)
def get_factor_exposures(
    view_type: str = Query(...),
    view_id: int = Query(...),
    factor_set: str = Query("STYLE7"),
    window: int = Query(252),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get factor exposures for a view"""
    vt = parse_view_type(view_type)
    db_vt = get_db_view_type(vt)

    # Get latest regression
    regression = db.query(FactorRegression).filter(
        and_(
            FactorRegression.view_type == db_vt,
            FactorRegression.view_id == view_id,
            FactorRegression.factor_set_code == factor_set,
            FactorRegression.window == window
        )
    ).order_by(desc(FactorRegression.as_of_date)).first()

    if not regression:
        raise HTTPException(status_code=404, detail="No factor data found")

    exposures = [
        FactorExposure(factor_name=name, beta=beta)
        for name, beta in regression.betas_json.items()
    ]

    return FactorResponse(
        factor_set_code=regression.factor_set_code,
        as_of_date=regression.as_of_date,
        window=regression.window,
        exposures=exposures,
        alpha=regression.alpha,
        r_squared=regression.r_squared
    )


@router.get("/unpriced-instruments")
def get_unpriced_instruments(
    as_of_date: Optional[date] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get securities with positions but no prices"""
    engine = PositionsEngine(db)
    unpriced = engine.get_unpriced_securities(as_of_date)

    return [
        UnpricedInstrument(
            symbol=u['symbol'],
            asset_name=u['asset_name'],
            asset_class=u['asset_class'],
            last_seen_date=u['last_seen_date'],
            position_count=1
        )
        for u in unpriced
    ]
