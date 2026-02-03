"""
Idea Pipeline API endpoints - manage research ideas before they become active coverage
"""
import os
import uuid
import shutil
from fastapi import APIRouter, Depends, HTTPException, UploadFile, File
from fastapi.responses import FileResponse
from sqlalchemy.orm import Session
from typing import List, Optional
from datetime import datetime

from app.core.database import get_db
from app.api.auth import get_current_user
from app.models.models import (
    User, Analyst, IdeaPipeline, IdeaPipelineModelData, IdeaPipelineDocument
)
from app.models.schemas import (
    AnalystResponse,
    IdeaPipelineCreate, IdeaPipelineUpdate, IdeaPipelineResponse,
    IdeaPipelineListResponse, IdeaPipelineDocumentResponse,
    CoverageModelDataResponse, MetricEstimates, MarginEstimates
)

router = APIRouter(prefix="/ideas", tags=["ideas"])

# Directory for uploaded documents
UPLOAD_DIR = "/app/uploads/ideas"
os.makedirs(UPLOAD_DIR, exist_ok=True)


def _build_model_data_response(data: IdeaPipelineModelData) -> CoverageModelDataResponse:
    """Build model data response with calculated fields (reuses coverage structure)"""

    def calc_growth(curr, prev):
        if curr is not None and prev is not None and prev != 0:
            return ((curr / prev) - 1) * 100
        return None

    def calc_diff(ccm, street):
        if ccm is not None and street is not None and street != 0:
            return ((ccm / street) - 1) * 100
        return None

    def build_metric(prefix: str):
        minus1 = getattr(data, f"{prefix}_ccm_minus1yr")
        yr1 = getattr(data, f"{prefix}_ccm_1yr")
        yr2 = getattr(data, f"{prefix}_ccm_2yr")
        yr3 = getattr(data, f"{prefix}_ccm_3yr")
        st_minus1 = getattr(data, f"{prefix}_street_minus1yr")
        st_yr1 = getattr(data, f"{prefix}_street_1yr")
        st_yr2 = getattr(data, f"{prefix}_street_2yr")
        st_yr3 = getattr(data, f"{prefix}_street_3yr")

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
        current_price=None,  # Ideas don't have current price from portfolio
        ccm_upside_pct=None,
        street_upside_pct=None,
        ccm_vs_street_diff_pct=(
            ((data.ccm_fair_value / data.street_price_target) - 1) * 100
            if data.ccm_fair_value and data.street_price_target and data.street_price_target > 0
            else None
        ),
        revenue=build_metric("revenue"),
        ebitda=build_metric("ebitda"),
        eps=build_metric("eps"),
        fcf=build_metric("fcf"),
        ebitda_margin=build_margin("ebitda"),
        fcf_margin=build_margin("fcf"),
        data_as_of=data.data_as_of,
        last_refreshed=data.last_refreshed
    )


def _build_idea_response(db: Session, idea: IdeaPipeline) -> dict:
    """Build an idea response with model data and documents"""
    # Get model data if available
    model_data = None
    cached_data = db.query(IdeaPipelineModelData).filter(
        IdeaPipelineModelData.idea_id == idea.id
    ).first()
    if cached_data:
        model_data = _build_model_data_response(cached_data)

    # Get documents
    documents = db.query(IdeaPipelineDocument).filter(
        IdeaPipelineDocument.idea_id == idea.id
    ).order_by(IdeaPipelineDocument.uploaded_at.desc()).all()

    return {
        "id": idea.id,
        "ticker": idea.ticker,
        "primary_analyst": idea.primary_analyst,
        "secondary_analyst": idea.secondary_analyst,
        "model_path": idea.model_path,
        "model_share_link": idea.model_share_link,
        "initial_review_complete": idea.initial_review_complete,
        "deep_dive_complete": idea.deep_dive_complete,
        "model_complete": idea.model_complete,
        "writeup_complete": idea.writeup_complete,
        "thesis": idea.thesis,
        "next_steps": idea.next_steps,
        "notes": idea.notes,
        "is_active": idea.is_active,
        "model_data": model_data,
        "documents": documents,
        "created_at": idea.created_at,
        "updated_at": idea.updated_at
    }


# ============== Idea CRUD Endpoints ==============

@router.get("", response_model=IdeaPipelineListResponse)
def list_ideas(
    active_only: bool = True,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """List all ideas in the pipeline"""
    query = db.query(IdeaPipeline)
    if active_only:
        query = query.filter(IdeaPipeline.is_active == True)

    ideas = query.order_by(IdeaPipeline.ticker).all()

    return IdeaPipelineListResponse(
        ideas=[_build_idea_response(db, idea) for idea in ideas]
    )


@router.get("/{idea_id}", response_model=IdeaPipelineResponse)
def get_idea(
    idea_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get a single idea with full details"""
    idea = db.query(IdeaPipeline).filter(IdeaPipeline.id == idea_id).first()
    if not idea:
        raise HTTPException(status_code=404, detail="Idea not found")

    return _build_idea_response(db, idea)


@router.post("", response_model=IdeaPipelineResponse)
def create_idea(
    data: IdeaPipelineCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Add a new ticker to the idea pipeline"""
    # Check if ticker already exists
    existing = db.query(IdeaPipeline).filter(
        IdeaPipeline.ticker == data.ticker.upper()
    ).first()

    if existing:
        if existing.is_active:
            raise HTTPException(status_code=400, detail="Ticker already in idea pipeline")
        # Reactivate inactive idea and update fields
        existing.is_active = True
        existing.primary_analyst_id = data.primary_analyst_id
        existing.secondary_analyst_id = data.secondary_analyst_id
        existing.model_path = data.model_path
        existing.model_share_link = data.model_share_link
        existing.thesis = data.thesis
        existing.next_steps = data.next_steps
        existing.notes = data.notes
        db.commit()
        db.refresh(existing)
        idea = existing
    else:
        idea = IdeaPipeline(
            ticker=data.ticker.upper(),
            primary_analyst_id=data.primary_analyst_id,
            secondary_analyst_id=data.secondary_analyst_id,
            model_path=data.model_path,
            model_share_link=data.model_share_link,
            thesis=data.thesis,
            next_steps=data.next_steps,
            notes=data.notes
        )
        db.add(idea)
        db.commit()
        db.refresh(idea)

    return _build_idea_response(db, idea)


@router.put("/{idea_id}", response_model=IdeaPipelineResponse)
def update_idea(
    idea_id: int,
    data: IdeaPipelineUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Update idea details"""
    idea = db.query(IdeaPipeline).filter(IdeaPipeline.id == idea_id).first()
    if not idea:
        raise HTTPException(status_code=404, detail="Idea not found")

    # Update fields if provided
    if data.primary_analyst_id is not None:
        idea.primary_analyst_id = data.primary_analyst_id
    if data.secondary_analyst_id is not None:
        idea.secondary_analyst_id = data.secondary_analyst_id
    if data.model_path is not None:
        idea.model_path = data.model_path
    if data.model_share_link is not None:
        idea.model_share_link = data.model_share_link
    if data.initial_review_complete is not None:
        idea.initial_review_complete = data.initial_review_complete
    if data.deep_dive_complete is not None:
        idea.deep_dive_complete = data.deep_dive_complete
    if data.model_complete is not None:
        idea.model_complete = data.model_complete
    if data.writeup_complete is not None:
        idea.writeup_complete = data.writeup_complete
    if data.thesis is not None:
        idea.thesis = data.thesis
    if data.next_steps is not None:
        idea.next_steps = data.next_steps
    if data.notes is not None:
        idea.notes = data.notes
    if data.is_active is not None:
        idea.is_active = data.is_active

    db.commit()
    db.refresh(idea)

    return _build_idea_response(db, idea)


@router.delete("/{idea_id}")
def delete_idea(
    idea_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Remove an idea from the pipeline (soft delete)"""
    idea = db.query(IdeaPipeline).filter(IdeaPipeline.id == idea_id).first()
    if not idea:
        raise HTTPException(status_code=404, detail="Idea not found")

    idea.is_active = False
    db.commit()
    return {"status": "success"}


# ============== Model Data Endpoints ==============

@router.post("/{idea_id}/refresh-model-data")
def refresh_model_data(
    idea_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Refresh model data from the linked Excel file"""
    idea = db.query(IdeaPipeline).filter(IdeaPipeline.id == idea_id).first()
    if not idea:
        raise HTTPException(status_code=404, detail="Idea not found")

    if not idea.model_path:
        raise HTTPException(status_code=400, detail="No model path configured for this idea")

    from app.services.excel_model_parser import parse_excel_model

    try:
        model_data = parse_excel_model(idea.model_path)

        # Update or create cached data
        cached = db.query(IdeaPipelineModelData).filter(
            IdeaPipelineModelData.idea_id == idea_id
        ).first()

        if not cached:
            cached = IdeaPipelineModelData(idea_id=idea_id)
            db.add(cached)

        # Update all fields
        for key, value in model_data.items():
            if hasattr(cached, key):
                setattr(cached, key, value)

        cached.last_refreshed = datetime.utcnow()

        db.commit()
        db.refresh(cached)

        return {"status": "success", "message": "Model data refreshed"}

    except FileNotFoundError:
        raise HTTPException(status_code=404, detail="Excel model file not found")
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error parsing model: {str(e)}")


# ============== Document Endpoints ==============

@router.get("/{idea_id}/documents", response_model=List[IdeaPipelineDocumentResponse])
def list_documents(
    idea_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """List all documents for an idea"""
    idea = db.query(IdeaPipeline).filter(IdeaPipeline.id == idea_id).first()
    if not idea:
        raise HTTPException(status_code=404, detail="Idea not found")

    documents = db.query(IdeaPipelineDocument).filter(
        IdeaPipelineDocument.idea_id == idea_id
    ).order_by(IdeaPipelineDocument.uploaded_at.desc()).all()

    return documents


@router.post("/{idea_id}/documents", response_model=IdeaPipelineDocumentResponse)
async def upload_document(
    idea_id: int,
    file: UploadFile = File(...),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Upload a document (PDF, Word, etc.) for an idea"""
    idea = db.query(IdeaPipeline).filter(IdeaPipeline.id == idea_id).first()
    if not idea:
        raise HTTPException(status_code=404, detail="Idea not found")

    # Validate file type
    allowed_extensions = {'.pdf', '.doc', '.docx', '.xls', '.xlsx', '.ppt', '.pptx', '.txt'}
    file_ext = os.path.splitext(file.filename)[1].lower()
    if file_ext not in allowed_extensions:
        raise HTTPException(
            status_code=400,
            detail=f"File type not allowed. Allowed types: {', '.join(allowed_extensions)}"
        )

    # Create idea-specific upload directory
    idea_upload_dir = os.path.join(UPLOAD_DIR, str(idea_id))
    os.makedirs(idea_upload_dir, exist_ok=True)

    # Generate unique filename
    unique_filename = f"{uuid.uuid4().hex}{file_ext}"
    file_path = os.path.join(idea_upload_dir, unique_filename)

    # Save file
    try:
        with open(file_path, "wb") as buffer:
            shutil.copyfileobj(file.file, buffer)
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error saving file: {str(e)}")

    # Get file size
    file_size = os.path.getsize(file_path)

    # Create database record
    document = IdeaPipelineDocument(
        idea_id=idea_id,
        filename=unique_filename,
        original_filename=file.filename,
        file_path=file_path,
        file_type=file_ext[1:],  # Remove the dot
        file_size=file_size,
        uploaded_by_id=current_user.id
    )
    db.add(document)
    db.commit()
    db.refresh(document)

    return document


@router.get("/{idea_id}/documents/{document_id}/download")
def download_document(
    idea_id: int,
    document_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Download a document"""
    document = db.query(IdeaPipelineDocument).filter(
        IdeaPipelineDocument.id == document_id,
        IdeaPipelineDocument.idea_id == idea_id
    ).first()

    if not document:
        raise HTTPException(status_code=404, detail="Document not found")

    if not os.path.exists(document.file_path):
        raise HTTPException(status_code=404, detail="File not found on server")

    return FileResponse(
        document.file_path,
        filename=document.original_filename,
        media_type="application/octet-stream"
    )


@router.delete("/{idea_id}/documents/{document_id}")
def delete_document(
    idea_id: int,
    document_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Delete a document"""
    document = db.query(IdeaPipelineDocument).filter(
        IdeaPipelineDocument.id == document_id,
        IdeaPipelineDocument.idea_id == idea_id
    ).first()

    if not document:
        raise HTTPException(status_code=404, detail="Document not found")

    # Delete file from disk
    if os.path.exists(document.file_path):
        try:
            os.remove(document.file_path)
        except Exception:
            pass  # File deletion failed, but continue with DB deletion

    # Delete database record
    db.delete(document)
    db.commit()

    return {"status": "success"}
