from fastapi import APIRouter, Depends, HTTPException
from sqlalchemy.orm import Session
from sqlalchemy import func
from typing import List
from app.core.database import get_db
from app.api.auth import get_current_user
from app.models import User, Account, Group, GroupMember, GroupType
from app.models.schemas import (
    AccountResponse, GroupResponse, GroupCreate, GroupMemberAdd
)
from app.services.groups import GroupsEngine

router = APIRouter(tags=["views"])


@router.get("/accounts", response_model=List[AccountResponse])
def get_accounts(
    search: str = None,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get all accounts with optional search"""
    query = db.query(Account)

    if search:
        query = query.filter(
            (Account.account_number.ilike(f"%{search}%")) |
            (Account.display_name.ilike(f"%{search}%"))
        )

    accounts = query.order_by(Account.display_name).all()

    return accounts


@router.get("/groups", response_model=List[GroupResponse])
def get_groups(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get all groups"""
    groups = db.query(
        Group,
        func.count(GroupMember.id).label('member_count')
    ).outerjoin(
        GroupMember, Group.id == GroupMember.group_id
    ).group_by(Group.id).all()

    return [
        {
            'id': g.id,
            'name': g.name,
            'type': g.type.value,
            'member_count': member_count
        }
        for g, member_count in groups
    ]


@router.post("/groups", response_model=GroupResponse)
def create_group(
    group_data: GroupCreate,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Create a new group"""
    engine = GroupsEngine(db)
    group = engine.create_group(group_data.name, group_data.type)

    return {
        'id': group.id,
        'name': group.name,
        'type': group.type.value,
        'member_count': 0
    }


@router.post("/groups/{group_id}/members")
def add_group_members(
    group_id: int,
    members: GroupMemberAdd,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Add accounts to a group"""
    group = db.query(Group).filter(Group.id == group_id).first()
    if not group:
        raise HTTPException(status_code=404, detail="Group not found")

    engine = GroupsEngine(db)
    count = engine.add_accounts_to_group(group_id, members.account_ids)

    return {'added': count}


@router.delete("/groups/{group_id}/members/{account_id}")
def remove_group_member(
    group_id: int,
    account_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Remove an account from a group"""
    engine = GroupsEngine(db)
    success = engine.remove_account_from_group(group_id, account_id)

    if not success:
        raise HTTPException(status_code=404, detail="Member not found in group")

    return {'removed': True}


@router.get("/views")
def get_all_views(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get all views (accounts + groups + firm)"""
    accounts = db.query(Account).order_by(Account.display_name).all()
    groups = db.query(Group).order_by(Group.name).all()

    views = []

    # Add accounts
    for account in accounts:
        views.append({
            'view_type': 'account',
            'view_id': account.id,
            'view_name': account.display_name,
            'account_number': account.account_number
        })

    # Add groups
    for group in groups:
        views.append({
            'view_type': 'group' if group.type != GroupType.FIRM else 'firm',
            'view_id': group.id,
            'view_name': group.name,
            'group_type': group.type.value
        })

    return views
