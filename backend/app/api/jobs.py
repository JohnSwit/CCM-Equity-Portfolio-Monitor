from fastapi import APIRouter, Depends, HTTPException, Query, status
from sqlalchemy.orm import Session
from datetime import datetime
import asyncio
from app.core.database import get_db
from app.api.auth import get_current_user
from app.models import User
from app.models.schemas import JobRunResponse

router = APIRouter(prefix="/jobs", tags=["jobs"])


def get_admin_user(current_user: User = Depends(get_current_user)) -> User:
    """Verify that the current user is an admin"""
    if not current_user.is_admin:
        raise HTTPException(
            status_code=status.HTTP_403_FORBIDDEN,
            detail="Admin privileges required"
        )
    return current_user


@router.post("/run", response_model=JobRunResponse)
async def run_job(
    job_name: str = Query(..., regex="^(market_data_update|recompute_analytics)$"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_admin_user)
):
    """
    Trigger a job manually:
    - market_data_update: Fetch latest market data
    - recompute_analytics: Recompute positions, returns, and analytics
    """
    started_at = datetime.utcnow()

    try:
        if job_name == "market_data_update":
            from app.workers.jobs import market_data_update_job
            await market_data_update_job(db)
            message = "Market data update completed successfully"

        elif job_name == "recompute_analytics":
            from app.workers.jobs import recompute_analytics_job
            await recompute_analytics_job(db)
            message = "Analytics recomputation completed successfully"

        else:
            raise HTTPException(status_code=400, detail="Invalid job name")

        return JobRunResponse(
            status="success",
            message=message,
            started_at=started_at
        )

    except Exception as e:
        return JobRunResponse(
            status="failed",
            message=f"Job failed: {str(e)}",
            started_at=started_at
        )
