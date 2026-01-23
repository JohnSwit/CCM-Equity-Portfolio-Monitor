"""
API endpoints for data management (classifications, benchmarks, factor returns).
"""
from fastapi import APIRouter, Depends, HTTPException, BackgroundTasks
from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import Optional
from datetime import date, datetime

from app.core.database import get_db
from app.services.data_sourcing import ClassificationService, BenchmarkService, FactorReturnsService
from app.models.sector_models import SectorClassification, BenchmarkConstituent, FactorReturns
from app.models import Security

router = APIRouter(prefix="/data-management", tags=["data-management"])


@router.post("/refresh-classifications")
async def refresh_classifications(
    limit: Optional[int] = None,
    db: Session = Depends(get_db)
):
    """
    Refresh security classifications from external sources.

    Args:
        limit: Maximum number of securities to refresh (None for all)
    """
    service = ClassificationService(db)

    # Run synchronously to ensure proper database session handling and error visibility
    result = await service.refresh_all_classifications(limit)
    return result


@router.post("/refresh-classification/{security_id}")
async def refresh_single_classification(
    security_id: int,
    db: Session = Depends(get_db)
):
    """
    Refresh classification for a single security.
    """
    service = ClassificationService(db)
    result = await service.refresh_classification(security_id)

    if not result:
        raise HTTPException(status_code=404, detail="Security not found or classification failed")

    return result


@router.post("/refresh-benchmark")
async def refresh_sp500_benchmark(
    db: Session = Depends(get_db)
):
    """
    Refresh S&P 500 benchmark constituent holdings.
    """
    service = BenchmarkService(db)
    result = await service.refresh_benchmark("SP500")

    if not result.get("success"):
        raise HTTPException(
            status_code=500,
            detail=f"Failed to refresh S&P 500: {result.get('error')}"
        )

    return result


@router.post("/refresh-factor-returns")
async def refresh_factor_returns(
    start_date: Optional[str] = None,
    db: Session = Depends(get_db)
):
    """
    Refresh factor returns from Kenneth French Data Library.

    Args:
        start_date: Start date (YYYY-MM-DD format, defaults to 5 years ago)
    """
    service = FactorReturnsService(db)

    parsed_date = None
    if start_date:
        try:
            parsed_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")

    # Run synchronously to ensure proper database session handling and error visibility
    result = await service.refresh_factor_returns(parsed_date)
    return result


@router.get("/status")
async def get_data_status(db: Session = Depends(get_db)):
    """
    Get status of all data sources (classifications, benchmarks, factors).

    Returns coverage statistics and last updated timestamps.
    """
    # Classification coverage
    total_securities = db.query(func.count(Security.id)).scalar()
    classified_securities = db.query(func.count(SectorClassification.id)).scalar()
    classification_coverage = (classified_securities / total_securities * 100) if total_securities > 0 else 0

    last_classification_update = db.query(func.max(SectorClassification.updated_at)).scalar()

    # Classification by source
    classification_sources = db.query(
        SectorClassification.source,
        func.count(SectorClassification.id)
    ).group_by(SectorClassification.source).all()

    # S&P 500 benchmark status
    latest_date = db.query(func.max(BenchmarkConstituent.as_of_date)).filter(
        BenchmarkConstituent.benchmark_code == "SP500"
    ).scalar()

    sp500_count = db.query(func.count(BenchmarkConstituent.id)).filter(
        BenchmarkConstituent.benchmark_code == "SP500",
        BenchmarkConstituent.as_of_date == latest_date
    ).scalar() if latest_date else 0

    benchmark_status = {
        "SP500": {
            "as_of_date": latest_date.isoformat() if latest_date else None,
            "constituent_count": sp500_count,
        }
    }

    # Factor returns status
    factor_date_range = db.query(
        func.min(FactorReturns.date).label("start_date"),
        func.max(FactorReturns.date).label("end_date"),
        func.count(func.distinct(FactorReturns.date)).label("trading_days")
    ).first()

    factor_counts = db.query(
        FactorReturns.factor_name,
        func.count(FactorReturns.id)
    ).group_by(FactorReturns.factor_name).all()

    last_factor_update = db.query(func.max(FactorReturns.updated_at)).scalar()

    return {
        "classifications": {
            "total_securities": total_securities,
            "classified_securities": classified_securities,
            "coverage_percent": round(classification_coverage, 2),
            "last_updated": last_classification_update.isoformat() if last_classification_update else None,
            "sources": {source: count for source, count in classification_sources},
        },
        "benchmarks": benchmark_status,
        "factor_returns": {
            "start_date": factor_date_range.start_date.isoformat() if factor_date_range.start_date else None,
            "end_date": factor_date_range.end_date.isoformat() if factor_date_range.end_date else None,
            "trading_days": factor_date_range.trading_days or 0,
            "factors": {factor: count for factor, count in factor_counts},
            "last_updated": last_factor_update.isoformat() if last_factor_update else None,
        },
        "data_readiness": {
            "brinson_attribution_ready": classification_coverage > 50 and any(
                benchmark_status[b]["constituent_count"] > 0 for b in benchmark_status
            ),
            "factor_attribution_ready": factor_date_range.trading_days > 0 if factor_date_range else False,
        }
    }


@router.post("/recompute-analytics")
async def recompute_analytics(db: Session = Depends(get_db)):
    """
    Recompute all analytics including positions, returns, and factor regressions.
    This is needed after loading new data to calculate factor exposures.
    """
    from app.workers.jobs import recompute_analytics_job

    try:
        print("=== ANALYTICS RECOMPUTATION STARTED ===")
        await recompute_analytics_job(db)
        print("=== ANALYTICS RECOMPUTATION COMPLETE ===")
        return {
            "success": True,
            "message": "Analytics recomputed successfully. Factor analysis should now show data."
        }
    except Exception as e:
        print(f"ERROR recomputing analytics: {str(e)}")
        raise HTTPException(
            status_code=500,
            detail=f"Failed to recompute analytics: {str(e)}"
        )


@router.get("/missing-classifications")
async def get_missing_classifications(
    limit: int = 100,
    db: Session = Depends(get_db)
):
    """
    Get list of securities without classifications.
    """
    unclassified = db.query(Security).outerjoin(SectorClassification).filter(
        SectorClassification.id.is_(None)
    ).limit(limit).all()

    return {
        "count": len(unclassified),
        "securities": [
            {
                "id": sec.id,
                "symbol": sec.symbol,
                "name": sec.asset_name,
            }
            for sec in unclassified
        ]
    }
