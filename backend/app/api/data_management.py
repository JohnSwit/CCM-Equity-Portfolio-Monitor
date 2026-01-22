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
from app.models.security import Security

router = APIRouter(prefix="/data-management", tags=["data-management"])


@router.post("/refresh-classifications")
async def refresh_classifications(
    limit: Optional[int] = None,
    background: bool = False,
    background_tasks: BackgroundTasks = None,
    db: Session = Depends(get_db)
):
    """
    Refresh security classifications from external sources.

    Args:
        limit: Maximum number of securities to refresh (None for all)
        background: Run in background (returns immediately)
    """
    service = ClassificationService(db)

    if background and background_tasks:
        background_tasks.add_task(service.refresh_all_classifications, limit)
        return {
            "status": "started",
            "message": "Classification refresh started in background"
        }

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


@router.post("/refresh-benchmark/{benchmark_code}")
async def refresh_benchmark(
    benchmark_code: str,
    db: Session = Depends(get_db)
):
    """
    Refresh benchmark constituent holdings.

    Args:
        benchmark_code: Benchmark code (SPY, QQQ, INDU)
    """
    if benchmark_code not in ["SPY", "QQQ", "INDU"]:
        raise HTTPException(
            status_code=400,
            detail=f"Unsupported benchmark: {benchmark_code}. Supported: SPY, QQQ, INDU"
        )

    service = BenchmarkService(db)
    result = await service.refresh_benchmark(benchmark_code)

    if not result.get("success"):
        raise HTTPException(
            status_code=500,
            detail=f"Failed to refresh benchmark: {result.get('error')}"
        )

    return result


@router.post("/refresh-all-benchmarks")
async def refresh_all_benchmarks(
    background: bool = False,
    background_tasks: BackgroundTasks = None,
    db: Session = Depends(get_db)
):
    """
    Refresh all supported benchmarks (SPY, QQQ, INDU).
    """
    async def refresh_all():
        service = BenchmarkService(db)
        results = {}
        for code in ["SPY", "QQQ", "INDU"]:
            results[code] = await service.refresh_benchmark(code)
        return results

    if background and background_tasks:
        background_tasks.add_task(refresh_all)
        return {
            "status": "started",
            "message": "Benchmark refresh started in background"
        }

    results = await refresh_all()
    return results


@router.post("/refresh-factor-returns")
async def refresh_factor_returns(
    start_date: Optional[str] = None,
    background: bool = False,
    background_tasks: BackgroundTasks = None,
    db: Session = Depends(get_db)
):
    """
    Refresh factor returns from Kenneth French Data Library.

    Args:
        start_date: Start date (YYYY-MM-DD format, defaults to 5 years ago)
        background: Run in background
    """
    service = FactorReturnsService(db)

    parsed_date = None
    if start_date:
        try:
            parsed_date = datetime.strptime(start_date, "%Y-%m-%d").date()
        except ValueError:
            raise HTTPException(status_code=400, detail="Invalid date format. Use YYYY-MM-DD")

    if background and background_tasks:
        background_tasks.add_task(service.refresh_factor_returns, parsed_date)
        return {
            "status": "started",
            "message": "Factor returns refresh started in background"
        }

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

    # Benchmark status
    benchmark_status = {}
    for benchmark_code in ["SPY", "QQQ", "INDU"]:
        latest_date = db.query(func.max(BenchmarkConstituent.as_of_date)).filter(
            BenchmarkConstituent.benchmark_code == benchmark_code
        ).scalar()

        count = db.query(func.count(BenchmarkConstituent.id)).filter(
            BenchmarkConstituent.benchmark_code == benchmark_code,
            BenchmarkConstituent.as_of_date == latest_date
        ).scalar() if latest_date else 0

        benchmark_status[benchmark_code] = {
            "as_of_date": latest_date.isoformat() if latest_date else None,
            "constituent_count": count,
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
                "ticker": sec.ticker,
                "name": sec.name,
            }
            for sec in unclassified
        ]
    }
