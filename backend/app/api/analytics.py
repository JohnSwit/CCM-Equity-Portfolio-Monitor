from fastapi import APIRouter, Depends, HTTPException, Query
from sqlalchemy.orm import Session
from sqlalchemy import and_, func, desc
from datetime import date, datetime
from typing import Optional
from app.core.database import get_db
from app.api.auth import get_current_user
from app.models import (
    User, PortfolioValueEOD, ReturnsEOD, RiskEOD,
    BenchmarkMetric, BenchmarkReturn, FactorRegression, ViewType, Account, Group,
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


@router.get("/benchmark-returns")
def get_benchmark_returns(
    benchmark_codes: str = Query(...),  # Comma-separated: "SPY,QQQ,INDU"
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get benchmark returns series with computed cumulative index.
    Returns data for multiple benchmarks in a format ready for charting.
    """
    codes = [c.strip() for c in benchmark_codes.split(',')]
    result = {}

    for code in codes:
        query = db.query(BenchmarkReturn).filter(BenchmarkReturn.code == code)

        if start_date:
            query = query.filter(BenchmarkReturn.date >= start_date)
        if end_date:
            query = query.filter(BenchmarkReturn.date <= end_date)

        returns = query.order_by(BenchmarkReturn.date).all()

        # Compute cumulative index starting at 1.0
        cumulative_index = 1.0
        data_points = []
        for r in returns:
            cumulative_index *= (1 + r.return_value)
            data_points.append({
                'date': r.date,
                'return_value': r.return_value,
                'index_value': cumulative_index
            })

        result[code] = data_points

    return result


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
                    # Track total cost for weighted average across accounts
                    if holding['avg_cost'] is not None:
                        all_holdings[symbol]['_total_cost'] = holding['avg_cost'] * holding['shares']
                        all_holdings[symbol]['_total_cost_shares'] = holding['shares']
                    else:
                        all_holdings[symbol]['_total_cost'] = 0
                        all_holdings[symbol]['_total_cost_shares'] = 0
                else:
                    all_holdings[symbol]['shares'] += holding['shares']
                    all_holdings[symbol]['market_value'] += holding['market_value']
                    # Aggregate 1D gains
                    if holding['gain_1d'] is not None:
                        if all_holdings[symbol]['gain_1d'] is not None:
                            all_holdings[symbol]['gain_1d'] += holding['gain_1d']
                        else:
                            all_holdings[symbol]['gain_1d'] = holding['gain_1d']
                    # Aggregate unrealized gains
                    if holding['unr_gain'] is not None:
                        if all_holdings[symbol]['unr_gain'] is not None:
                            all_holdings[symbol]['unr_gain'] += holding['unr_gain']
                        else:
                            all_holdings[symbol]['unr_gain'] = holding['unr_gain']
                    # Track cost basis for weighted average
                    if holding['avg_cost'] is not None:
                        all_holdings[symbol]['_total_cost'] += holding['avg_cost'] * holding['shares']
                        all_holdings[symbol]['_total_cost_shares'] += holding['shares']

        # Recalculate avg_cost and percentage gains for aggregated holdings
        for symbol, h in all_holdings.items():
            if h['_total_cost_shares'] > 0:
                h['avg_cost'] = h['_total_cost'] / h['_total_cost_shares']
                if h['price'] is not None and h['avg_cost'] > 0:
                    h['unr_gain_pct'] = (h['price'] - h['avg_cost']) / h['avg_cost']
            # Recalculate 1D gain pct based on aggregated values
            if h.get('gain_1d') is not None and h['market_value'] > 0:
                # Estimate based on market value change
                prev_value = h['market_value'] - h['gain_1d']
                if prev_value > 0:
                    h['gain_1d_pct'] = h['gain_1d'] / prev_value

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
                weight=weight,
                avg_cost=h.get('avg_cost'),
                gain_1d_pct=h.get('gain_1d_pct'),
                gain_1d=h.get('gain_1d'),
                unr_gain_pct=h.get('unr_gain_pct'),
                unr_gain=h.get('unr_gain')
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


# ============================================================================
# Factor Benchmarking + Attribution Endpoints
# ============================================================================

@router.get("/factor-models")
def get_factor_models(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get available factor models"""
    from app.services.factor_benchmarking import FactorBenchmarkingService

    service = FactorBenchmarkingService(db)
    service.ensure_default_models()

    return service.get_available_models()


@router.get("/factor-benchmarking")
def get_factor_benchmarking(
    view_type: str = Query(...),
    view_id: int = Query(...),
    model_code: str = Query("US_CORE"),
    period: str = Query("1Y"),  # 1M, 3M, 6M, YTD, 1Y, ALL
    use_excess_returns: bool = Query(False),
    use_robust: bool = Query(False),
    benchmark_code: Optional[str] = Query(None),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get factor benchmarking and attribution analysis.

    Returns:
    - Factor betas (exposures) with confidence intervals
    - Alpha (daily and annualized) with CI and Information Ratio
    - R-squared and adjusted R-squared
    - Diagnostics (VIF, correlations, residual tests)
    - Return attribution by factor
    - Optional benchmark-relative attribution
    """
    from app.services.factor_benchmarking import FactorBenchmarkingService
    from datetime import timedelta

    vt = parse_view_type(view_type)
    db_vt = get_db_view_type(vt)

    # Get latest portfolio data date
    latest = db.query(PortfolioValueEOD.date).filter(
        and_(
            PortfolioValueEOD.view_type == db_vt,
            PortfolioValueEOD.view_id == view_id
        )
    ).order_by(desc(PortfolioValueEOD.date)).first()

    if not latest:
        raise HTTPException(status_code=404, detail="No portfolio data found")

    end_date = latest[0]

    # Calculate start date based on period
    if period == '1M':
        start_date = end_date - timedelta(days=30)
    elif period == '3M':
        start_date = end_date - timedelta(days=90)
    elif period == '6M':
        start_date = end_date - timedelta(days=180)
    elif period == 'YTD':
        start_date = date(end_date.year, 1, 1)
    elif period == '1Y':
        start_date = end_date - timedelta(days=365)
    elif period == 'ALL':
        # Get earliest portfolio data
        earliest = db.query(PortfolioValueEOD.date).filter(
            and_(
                PortfolioValueEOD.view_type == db_vt,
                PortfolioValueEOD.view_id == view_id
            )
        ).order_by(PortfolioValueEOD.date).first()
        start_date = earliest[0] if earliest else end_date - timedelta(days=365)
    else:
        start_date = end_date - timedelta(days=365)

    service = FactorBenchmarkingService(db)
    service.ensure_default_models()

    # Check if we need to refresh factor data
    # For now, always try to get latest data
    try:
        service.refresh_factor_data(model_code, start_date, end_date)
    except Exception as e:
        # Log but continue - we might have cached data
        import logging
        logging.warning(f"Failed to refresh factor data: {e}")

    # Compute attribution with new parameters
    result = service.compute_attribution(
        db_vt, view_id, model_code, start_date, end_date,
        use_excess_returns=use_excess_returns,
        use_robust=use_robust,
        benchmark_code=benchmark_code
    )

    if not result:
        raise HTTPException(
            status_code=404,
            detail="Could not compute factor analysis. Ensure sufficient data is available."
        )

    return result


@router.post("/refresh-factor-data")
def refresh_factor_data(
    model_code: str = Query("US_CORE"),
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Refresh factor proxy data from external sources.
    Only fetches missing dates.
    """
    from app.services.factor_benchmarking import FactorBenchmarkingService
    from datetime import timedelta

    if not end_date:
        end_date = date.today()
    if not start_date:
        start_date = end_date - timedelta(days=365 * 2)  # Default 2 years

    service = FactorBenchmarkingService(db)
    service.ensure_default_models()

    try:
        results = service.refresh_factor_data(model_code, start_date, end_date)
        return {
            "status": "success",
            "model_code": model_code,
            "start_date": str(start_date),
            "end_date": str(end_date),
            "rows_fetched": results
        }
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.get("/factor-rolling-analysis")
def get_factor_rolling_analysis(
    view_type: str = Query(...),
    view_id: int = Query(...),
    model_code: str = Query("US_CORE"),
    period: str = Query("1Y"),
    window_days: int = Query(63),  # 30, 63, 126, 252
    use_excess_returns: bool = Query(False),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get rolling factor analysis (betas, alpha, R-squared over time).
    """
    from app.services.factor_benchmarking import FactorBenchmarkingService
    from datetime import timedelta

    vt = parse_view_type(view_type)
    db_vt = get_db_view_type(vt)

    # Get latest portfolio data date
    latest = db.query(PortfolioValueEOD.date).filter(
        and_(
            PortfolioValueEOD.view_type == db_vt,
            PortfolioValueEOD.view_id == view_id
        )
    ).order_by(desc(PortfolioValueEOD.date)).first()

    if not latest:
        raise HTTPException(status_code=404, detail="No portfolio data found")

    end_date = latest[0]

    # Calculate start date based on period (need extra for rolling window)
    if period == '1M':
        start_date = end_date - timedelta(days=30 + window_days)
    elif period == '3M':
        start_date = end_date - timedelta(days=90 + window_days)
    elif period == '6M':
        start_date = end_date - timedelta(days=180 + window_days)
    elif period == 'YTD':
        start_date = date(end_date.year, 1, 1) - timedelta(days=window_days)
    elif period == '1Y':
        start_date = end_date - timedelta(days=365 + window_days)
    else:
        start_date = end_date - timedelta(days=365 + window_days)

    service = FactorBenchmarkingService(db)
    service.ensure_default_models()

    # Refresh factor data
    try:
        service.refresh_factor_data(model_code, start_date, end_date)
    except Exception as e:
        import logging
        logging.warning(f"Failed to refresh factor data: {e}")

    result = service.compute_rolling_analysis(
        db_vt, view_id, model_code, start_date, end_date,
        window_days=window_days,
        use_excess_returns=use_excess_returns
    )

    if not result:
        raise HTTPException(
            status_code=404,
            detail="Could not compute rolling analysis. Ensure sufficient data is available."
        )

    return result


@router.get("/factor-contribution-over-time")
def get_factor_contribution_over_time(
    view_type: str = Query(...),
    view_id: int = Query(...),
    model_code: str = Query("US_CORE"),
    period: str = Query("1Y"),
    frequency: str = Query("M"),  # M=monthly, Q=quarterly
    use_excess_returns: bool = Query(False),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get factor contributions over time (monthly or quarterly breakdown).
    """
    from app.services.factor_benchmarking import FactorBenchmarkingService
    from datetime import timedelta

    vt = parse_view_type(view_type)
    db_vt = get_db_view_type(vt)

    # Get latest portfolio data date
    latest = db.query(PortfolioValueEOD.date).filter(
        and_(
            PortfolioValueEOD.view_type == db_vt,
            PortfolioValueEOD.view_id == view_id
        )
    ).order_by(desc(PortfolioValueEOD.date)).first()

    if not latest:
        raise HTTPException(status_code=404, detail="No portfolio data found")

    end_date = latest[0]

    # Calculate start date based on period
    if period == '1M':
        start_date = end_date - timedelta(days=30)
    elif period == '3M':
        start_date = end_date - timedelta(days=90)
    elif period == '6M':
        start_date = end_date - timedelta(days=180)
    elif period == 'YTD':
        start_date = date(end_date.year, 1, 1)
    elif period == '1Y':
        start_date = end_date - timedelta(days=365)
    elif period == '2Y':
        start_date = end_date - timedelta(days=365 * 2)
    else:
        start_date = end_date - timedelta(days=365)

    service = FactorBenchmarkingService(db)
    service.ensure_default_models()

    # Refresh factor data
    try:
        service.refresh_factor_data(model_code, start_date, end_date)
    except Exception as e:
        import logging
        logging.warning(f"Failed to refresh factor data: {e}")

    result = service.compute_contribution_over_time(
        db_vt, view_id, model_code, start_date, end_date,
        frequency=frequency,
        use_excess_returns=use_excess_returns
    )

    if not result:
        raise HTTPException(
            status_code=404,
            detail="Could not compute contribution analysis. Ensure sufficient data is available."
        )

    return result


@router.get("/available-benchmarks")
def get_available_benchmarks(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get list of available benchmarks for comparison.
    """
    from app.services.factor_benchmarking import FactorBenchmarkingService

    service = FactorBenchmarkingService(db)
    return service.get_available_benchmarks()
