from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from typing import List
from app.core.database import get_db
from app.api.auth import get_current_user
from app.models import User, Basket, BasketConstituent
from app.models.schemas import BasketCreate, BasketUpdate, BasketResponse
from app.services.baskets import BasketsEngine

router = APIRouter(prefix="/baskets", tags=["baskets"])


@router.get("/", response_model=List[BasketResponse])
def get_baskets(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get all baskets"""
    baskets = db.query(Basket).all()

    result = []
    for basket in baskets:
        constituents = db.query(BasketConstituent).filter(
            BasketConstituent.basket_id == basket.id
        ).all()

        result.append({
            'id': basket.id,
            'code': basket.code,
            'name': basket.name,
            'constituents': [
                {'symbol': c.symbol, 'weight': c.weight}
                for c in constituents
            ]
        })

    return result


@router.post("/", response_model=BasketResponse)
def create_basket(
    basket_data: BasketCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Create a new basket"""
    # Check if code already exists
    existing = db.query(Basket).filter(Basket.code == basket_data.code).first()
    if existing:
        raise HTTPException(status_code=400, detail="Basket code already exists")

    engine = BasketsEngine(db)

    try:
        basket = engine.create_basket(
            basket_data.code,
            basket_data.name,
            [{'symbol': c.symbol, 'weight': c.weight} for c in basket_data.constituents]
        )

        constituents = db.query(BasketConstituent).filter(
            BasketConstituent.basket_id == basket.id
        ).all()

        return {
            'id': basket.id,
            'code': basket.code,
            'name': basket.name,
            'constituents': [
                {'symbol': c.symbol, 'weight': c.weight}
                for c in constituents
            ]
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.put("/{basket_id}", response_model=BasketResponse)
def update_basket(
    basket_id: int,
    basket_data: BasketUpdate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Update a basket"""
    basket = db.query(Basket).filter(Basket.id == basket_id).first()
    if not basket:
        raise HTTPException(status_code=404, detail="Basket not found")

    engine = BasketsEngine(db)

    try:
        if basket_data.name:
            basket.name = basket_data.name

        if basket_data.constituents:
            engine.update_basket_constituents(
                basket_id,
                [{'symbol': c.symbol, 'weight': c.weight} for c in basket_data.constituents]
            )

        db.commit()
        db.refresh(basket)

        constituents = db.query(BasketConstituent).filter(
            BasketConstituent.basket_id == basket.id
        ).all()

        return {
            'id': basket.id,
            'code': basket.code,
            'name': basket.name,
            'constituents': [
                {'symbol': c.symbol, 'weight': c.weight}
                for c in constituents
            ]
        }
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.post("/{basket_id}/constituents")
def update_basket_constituents(
    basket_id: int,
    constituents: List[dict],
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Update basket constituents"""
    basket = db.query(Basket).filter(Basket.id == basket_id).first()
    if not basket:
        raise HTTPException(status_code=404, detail="Basket not found")

    engine = BasketsEngine(db)

    try:
        engine.update_basket_constituents(basket_id, constituents)
        return {'updated': True}
    except ValueError as e:
        raise HTTPException(status_code=400, detail=str(e))
