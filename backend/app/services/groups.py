import pandas as pd
from typing import List, Dict, Optional
from datetime import date
from sqlalchemy.orm import Session
from sqlalchemy import and_, func
from app.models import (
    Group, GroupMember, Account, PortfolioValueEOD,
    ReturnsEOD, ViewType, GroupType
)
import logging

logger = logging.getLogger(__name__)


class GroupsEngine:
    """Manages groups and computes group-level rollups"""

    def __init__(self, db: Session):
        self.db = db

    def create_group(self, name: str, group_type: GroupType) -> Group:
        """Create a new group"""
        group = Group(name=name, type=group_type)
        self.db.add(group)
        self.db.commit()
        self.db.refresh(group)
        return group

    def add_accounts_to_group(self, group_id: int, account_ids: List[int]) -> int:
        """Add accounts to a group"""
        count = 0
        for account_id in account_ids:
            # Check if already exists
            existing = self.db.query(GroupMember).filter(
                and_(
                    GroupMember.group_id == group_id,
                    GroupMember.member_type == 'account',
                    GroupMember.member_id == account_id
                )
            ).first()

            if not existing:
                member = GroupMember(
                    group_id=group_id,
                    member_type='account',
                    member_id=account_id
                )
                self.db.add(member)
                count += 1

        self.db.commit()
        return count

    def remove_account_from_group(self, group_id: int, account_id: int) -> bool:
        """Remove an account from a group"""
        member = self.db.query(GroupMember).filter(
            and_(
                GroupMember.group_id == group_id,
                GroupMember.member_type == 'account',
                GroupMember.member_id == account_id
            )
        ).first()

        if member:
            self.db.delete(member)
            self.db.commit()
            return True

        return False

    def get_group_account_ids(self, group_id: int) -> List[int]:
        """Get all account IDs in a group"""
        members = self.db.query(GroupMember.member_id).filter(
            and_(
                GroupMember.group_id == group_id,
                GroupMember.member_type == 'account'
            )
        ).all()

        return [m[0] for m in members]

    def ensure_firm_group(self) -> Group:
        """Ensure firm group exists and contains all accounts"""
        firm_group = self.db.query(Group).filter(
            Group.type == GroupType.FIRM
        ).first()

        if not firm_group:
            firm_group = Group(name='Firm', type=GroupType.FIRM)
            self.db.add(firm_group)
            self.db.commit()
            self.db.refresh(firm_group)

        # Add all accounts to firm group
        all_accounts = self.db.query(Account.id).all()
        account_ids = [a[0] for a in all_accounts]

        if account_ids:
            self.add_accounts_to_group(firm_group.id, account_ids)

        return firm_group

    def compute_group_values(
        self,
        group_id: int,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> int:
        """Compute daily portfolio values for a group (sum of member values)"""
        if not end_date:
            end_date = date.today()

        # Get member accounts
        account_ids = self.get_group_account_ids(group_id)

        if not account_ids:
            return 0

        # Get values for all member accounts
        values_query = self.db.query(
            PortfolioValueEOD.date,
            func.sum(PortfolioValueEOD.total_value).label('total_value')
        ).filter(
            and_(
                PortfolioValueEOD.view_type == ViewType.ACCOUNT,
                PortfolioValueEOD.view_id.in_(account_ids),
                PortfolioValueEOD.date <= end_date
            )
        )

        if start_date:
            values_query = values_query.filter(PortfolioValueEOD.date >= start_date)

        values = values_query.group_by(PortfolioValueEOD.date).all()

        if not values:
            return 0

        # Store group values
        count = 0
        for value_row in values:
            existing = self.db.query(PortfolioValueEOD).filter(
                and_(
                    PortfolioValueEOD.view_type == ViewType.GROUP,
                    PortfolioValueEOD.view_id == group_id,
                    PortfolioValueEOD.date == value_row.date
                )
            ).first()

            if existing:
                if existing.total_value != value_row.total_value:
                    existing.total_value = value_row.total_value
            else:
                value = PortfolioValueEOD(
                    view_type=ViewType.GROUP,
                    view_id=group_id,
                    date=value_row.date,
                    total_value=value_row.total_value
                )
                self.db.add(value)
                count += 1

        self.db.commit()
        logger.info(f"Created {count} portfolio values for group {group_id}")
        return count

    def compute_group_returns(
        self,
        group_id: int,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None
    ) -> int:
        """
        Compute group returns using value-weighted member returns.
        r_group_t = Σ (w_a_{t-1} × r_a_t)
        where w_a_{t-1} = value_a_{t-1} / group_value_{t-1}
        """
        if not end_date:
            end_date = date.today()

        # Get member accounts
        account_ids = self.get_group_account_ids(group_id)

        if not account_ids:
            return 0

        # Get account values
        values = self.db.query(
            PortfolioValueEOD
        ).filter(
            and_(
                PortfolioValueEOD.view_type == ViewType.ACCOUNT,
                PortfolioValueEOD.view_id.in_(account_ids)
            )
        )

        if start_date:
            values = values.filter(PortfolioValueEOD.date >= start_date)

        values = values.filter(PortfolioValueEOD.date <= end_date).all()

        # Get account returns
        returns = self.db.query(
            ReturnsEOD
        ).filter(
            and_(
                ReturnsEOD.view_type == ViewType.ACCOUNT,
                ReturnsEOD.view_id.in_(account_ids)
            )
        )

        if start_date:
            returns = returns.filter(ReturnsEOD.date >= start_date)

        returns = returns.filter(ReturnsEOD.date <= end_date).all()

        # Convert to DataFrames
        values_df = pd.DataFrame([
            {
                'date': v.date,
                'account_id': v.view_id,
                'value': v.total_value
            }
            for v in values
        ])

        returns_df = pd.DataFrame([
            {
                'date': r.date,
                'account_id': r.view_id,
                'return': r.twr_return
            }
            for r in returns
        ])

        if values_df.empty or returns_df.empty:
            return 0

        # Pivot to wide format
        values_wide = values_df.pivot(index='date', columns='account_id', values='value').fillna(0)
        returns_wide = returns_df.pivot(index='date', columns='account_id', values='return').fillna(0)

        # Align dates
        all_dates = sorted(set(values_wide.index) & set(returns_wide.index))

        if not all_dates:
            return 0

        values_wide = values_wide.loc[all_dates]
        returns_wide = returns_wide.loc[all_dates]

        # Compute group returns
        group_returns = []
        index_value = 1.0  # Match account-level convention (1.0 = start)

        for i, current_date in enumerate(all_dates):
            if i == 0:
                continue  # No return for first date

            prev_date = all_dates[i-1]

            # Weights based on previous day's values
            prev_values = values_wide.loc[prev_date]
            total_value = prev_values.sum()

            if total_value == 0:
                continue

            weights = prev_values / total_value

            # Current day's returns
            current_returns = returns_wide.loc[current_date]

            # Value-weighted return
            group_return = (weights * current_returns).sum()

            # Update index
            index_value = index_value * (1 + group_return)

            group_returns.append({
                'date': current_date,
                'twr_return': group_return,
                'twr_index': index_value
            })

        # Store returns
        count = 0
        for row in group_returns:
            existing = self.db.query(ReturnsEOD).filter(
                and_(
                    ReturnsEOD.view_type == ViewType.GROUP,
                    ReturnsEOD.view_id == group_id,
                    ReturnsEOD.date == row['date']
                )
            ).first()

            if existing:
                if existing.twr_return != row['twr_return']:
                    existing.twr_return = row['twr_return']
                    existing.twr_index = row['twr_index']
            else:
                ret = ReturnsEOD(
                    view_type=ViewType.GROUP,
                    view_id=group_id,
                    date=row['date'],
                    twr_return=row['twr_return'],
                    twr_index=row['twr_index']
                )
                self.db.add(ret)
                count += 1

        self.db.commit()
        logger.info(f"Created {count} returns for group {group_id}")
        return count

    def compute_all_groups(self) -> Dict[str, int]:
        """Compute values and returns for all groups including firm"""
        # Ensure firm group exists
        self.ensure_firm_group()

        groups = self.db.query(Group).all()

        results = {
            'total_groups': len(groups),
            'updated': 0,
            'failed': 0
        }

        for group in groups:
            try:
                self.compute_group_values(group.id)
                self.compute_group_returns(group.id)
                results['updated'] += 1
            except Exception as e:
                logger.error(f"Failed to compute group {group.id}: {e}")
                results['failed'] += 1

        return results
