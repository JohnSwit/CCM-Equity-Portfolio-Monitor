from fastapi import APIRouter, Depends, Query
from sqlalchemy.orm import Session
from typing import Optional, List
from datetime import date, timedelta
from app.core.database import get_db
from app.api.auth import get_current_user
from app.models import User, ViewType
from app.services.portfolio_statistics import PortfolioStatisticsEngine
import logging

router = APIRouter(prefix="/portfolio-stats", tags=["portfolio-statistics"])
logger = logging.getLogger(__name__)


def parse_view_type(view_type_str: str) -> ViewType:
    """Parse view type from string"""
    mapping = {
        'account': ViewType.ACCOUNT,
        'group': ViewType.GROUP,
        'firm': ViewType.FIRM
    }
    return mapping.get(view_type_str.lower(), ViewType.ACCOUNT)


@router.get("/contribution-to-returns")
def get_contribution_to_returns(
    view_type: str,
    view_id: int,
    start_date: Optional[date] = None,
    end_date: Optional[date] = None,
    top_n: int = Query(20, ge=1, le=100),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get each holding's contribution to total portfolio return.
    Shows which positions contributed most/least to P&L.
    """
    vt = parse_view_type(view_type)
    engine = PortfolioStatisticsEngine(db)

    if not end_date:
        end_date = date.today()
    if not start_date:
        start_date = end_date - timedelta(days=90)  # Default to 90 days

    return engine.get_contribution_to_returns(vt, view_id, start_date, end_date, top_n)


@router.get("/volatility-metrics")
def get_volatility_metrics(
    view_type: str,
    view_id: int,
    benchmark: str = Query('SPY', description="Benchmark code"),
    window: int = Query(252, ge=20, le=1000, description="Number of trading days"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get volatility and risk metrics:
    - Annualized volatility
    - Tracking error
    - Information ratio
    - Downside deviation and Sortino ratio
    - Skewness and kurtosis
    """
    vt = parse_view_type(view_type)
    engine = PortfolioStatisticsEngine(db)
    return engine.get_volatility_metrics(vt, view_id, benchmark, window)


@router.get("/drawdown-analysis")
def get_drawdown_analysis(
    view_type: str,
    view_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get drawdown analysis:
    - Max drawdown and date
    - Time to recovery
    - Current drawdown
    - Ulcer index
    - Historical drawdown periods
    """
    vt = parse_view_type(view_type)
    engine = PortfolioStatisticsEngine(db)
    return engine.get_drawdown_analysis(vt, view_id)


@router.get("/var-cvar")
def get_var_cvar(
    view_type: str,
    view_id: int,
    confidence_levels: str = Query('95,99', description="Comma-separated confidence levels (e.g., '95,99')"),
    window: int = Query(252, ge=20, le=1000),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get Value at Risk (VaR) and Conditional VaR (CVaR/Expected Shortfall).
    Tail risk metrics at specified confidence levels.
    """
    vt = parse_view_type(view_type)
    engine = PortfolioStatisticsEngine(db)

    conf_levels = [float(c.strip()) / 100 for c in confidence_levels.split(',')]
    return engine.get_var_cvar(vt, view_id, conf_levels, window)


@router.get("/factor-analysis")
def get_factor_analysis(
    view_type: str,
    view_id: int,
    as_of_date: Optional[date] = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get factor analysis:
    - Factor exposures/tilts
    - Alpha
    - R-squared
    - Factor risk vs idiosyncratic risk
    """
    vt = parse_view_type(view_type)
    engine = PortfolioStatisticsEngine(db)
    return engine.get_factor_analysis(vt, view_id, as_of_date)


@router.get("/comprehensive")
def get_comprehensive_statistics(
    view_type: str,
    view_id: int,
    benchmark: str = Query('SPY'),
    window: int = Query(252, ge=20, le=1000),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get comprehensive portfolio statistics in one call.
    Combines volatility, drawdown, VaR, and factor analysis.
    """
    vt = parse_view_type(view_type)
    engine = PortfolioStatisticsEngine(db)

    return {
        'volatility_metrics': engine.get_volatility_metrics(vt, view_id, benchmark, window),
        'drawdown_analysis': engine.get_drawdown_analysis(vt, view_id),
        'var_cvar': engine.get_var_cvar(vt, view_id, [0.95, 0.99], window),
        'factor_analysis': engine.get_factor_analysis(vt, view_id)
    }
