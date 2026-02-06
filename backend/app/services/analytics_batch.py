"""
Optimized Analytics Batch Processing Service

Designed for large datasets (100+ accounts, 20+ years, 250K+ transactions):
- Bulk upsert operations using PostgreSQL ON CONFLICT
- Batch processing with incremental commits
- Progress tracking and observability
- Incremental computation (only changed data)
- Memory-efficient chunked processing
"""
import logging
from datetime import date, datetime, timedelta
from typing import Dict, List, Optional, Set, Tuple, Any
import pandas as pd
import numpy as np
from sqlalchemy.orm import Session
from sqlalchemy import and_, func, text
from sqlalchemy.dialects.postgresql import insert

from app.models.models import (
    Account, Transaction, TransactionType, Security,
    PositionsEOD, PricesEOD, PortfolioValueEOD, ReturnsEOD,
    ViewType, AccountInception
)
from app.core.database import SessionLocal

logger = logging.getLogger(__name__)


class AnalyticsProgress:
    """Track progress of analytics computation"""

    def __init__(self, total_steps: int = 0, description: str = ""):
        self.total_steps = total_steps
        self.completed_steps = 0
        self.current_step_name = ""
        self.description = description
        self.started_at = datetime.utcnow()
        self.step_started_at = None
        self.errors: List[Dict] = []
        self.metrics: Dict[str, Any] = {}

    def start_step(self, step_name: str):
        self.current_step_name = step_name
        self.step_started_at = datetime.utcnow()
        logger.info(f"[{self.completed_steps + 1}/{self.total_steps}] Starting: {step_name}")

    def complete_step(self, metrics: Optional[Dict] = None):
        self.completed_steps += 1
        duration = (datetime.utcnow() - self.step_started_at).total_seconds() if self.step_started_at else 0
        logger.info(f"[{self.completed_steps}/{self.total_steps}] Completed: {self.current_step_name} ({duration:.1f}s)")
        if metrics:
            self.metrics[self.current_step_name] = metrics

    def add_error(self, error: str, context: Optional[Dict] = None):
        self.errors.append({
            "step": self.current_step_name,
            "error": error,
            "context": context,
            "timestamp": datetime.utcnow().isoformat()
        })
        logger.error(f"Error in {self.current_step_name}: {error}")

    def progress_percent(self) -> float:
        return (self.completed_steps / self.total_steps * 100) if self.total_steps > 0 else 0

    def to_dict(self) -> Dict:
        return {
            "total_steps": self.total_steps,
            "completed_steps": self.completed_steps,
            "progress_percent": round(self.progress_percent(), 1),
            "current_step": self.current_step_name,
            "started_at": self.started_at.isoformat(),
            "elapsed_seconds": (datetime.utcnow() - self.started_at).total_seconds(),
            "errors_count": len(self.errors),
            "metrics": self.metrics
        }


class BatchAnalyticsService:
    """
    High-performance analytics computation service.

    Key optimizations:
    1. Bulk upsert using PostgreSQL INSERT ... ON CONFLICT
    2. Chunked processing to limit memory usage
    3. Pre-compute and cache lookups
    4. Only compute changed data when possible
    """

    # Processing configuration
    POSITION_BATCH_SIZE = 10000  # Positions per bulk insert
    ACCOUNT_BATCH_SIZE = 10     # Accounts to process before commit
    MIN_DATE = date(2000, 1, 1)  # Default start date for historical data

    def __init__(self, db: Session):
        self.db = db
        self.progress = AnalyticsProgress()

        # Caches
        self._trading_dates: Optional[List[date]] = None
        self._price_cache: Dict[Tuple[int, date], float] = {}

    def run_full_analytics(
        self,
        account_ids: Optional[List[int]] = None,
        start_date: Optional[date] = None,
        end_date: Optional[date] = None,
        skip_positions: bool = False,
        skip_values: bool = False,
        skip_returns: bool = False,
        force_full_rebuild: bool = False
    ) -> Dict[str, Any]:
        """
        Run full analytics computation with progress tracking.

        Args:
            account_ids: Specific accounts to process (None = all with transactions)
            start_date: Start date for computation
            end_date: End date (default: today)
            skip_positions: Skip position building
            skip_values: Skip portfolio value computation
            skip_returns: Skip returns computation
            force_full_rebuild: Force full rebuild, bypass incremental skip logic

        Returns:
            Summary of computation results
        """
        if not end_date:
            end_date = date.today()

        # Get accounts - those with transactions OR inception data (to skip truly orphaned accounts)
        if account_ids:
            accounts = self.db.query(Account).filter(Account.id.in_(account_ids)).all()
        else:
            # Get accounts that have at least one transaction OR have inception data
            from sqlalchemy import or_
            accounts_with_txns = self.db.query(Transaction.account_id).distinct().subquery()
            accounts_with_inception = self.db.query(AccountInception.account_id).distinct().subquery()
            accounts = self.db.query(Account).filter(
                or_(
                    Account.id.in_(accounts_with_txns),
                    Account.id.in_(accounts_with_inception)
                )
            ).all()

        if not accounts:
            return {"status": "no_accounts", "accounts_processed": 0}

        # Calculate steps
        steps = 0
        if not skip_positions:
            steps += 1  # Build positions
        if not skip_values:
            steps += 1  # Compute values
        if not skip_returns:
            steps += 1  # Compute returns

        self.progress = AnalyticsProgress(total_steps=steps, description="Full analytics computation")

        logger.info(f"Starting analytics for {len(accounts)} accounts from {start_date} to {end_date}")

        results = {
            "accounts_count": len(accounts),
            "date_range": {"start": str(start_date), "end": str(end_date)},
            "positions_created": 0,
            "values_created": 0,
            "returns_created": 0,
            "errors": []
        }

        try:
            # Step 1: Build positions
            if not skip_positions:
                self.progress.start_step("Building positions")
                pos_result = self._build_all_positions_bulk(
                    accounts, start_date, end_date, force_full_rebuild=force_full_rebuild
                )
                results["positions_created"] = pos_result.get("total_positions", 0)
                results["positions_skipped"] = pos_result.get("accounts_skipped", 0)
                self.progress.complete_step(pos_result)

            # Step 2: Compute portfolio values
            if not skip_values:
                self.progress.start_step("Computing portfolio values")
                val_result = self._compute_all_values_bulk(accounts, start_date, end_date)
                results["values_created"] = val_result.get("total_values", 0)
                self.progress.complete_step(val_result)

            # Step 3: Compute returns
            if not skip_returns:
                self.progress.start_step("Computing returns")
                ret_result = self._compute_all_returns_bulk(accounts, start_date, end_date)
                results["returns_created"] = ret_result.get("total_returns", 0)
                self.progress.complete_step(ret_result)

            results["status"] = "completed"
            results["progress"] = self.progress.to_dict()

        except Exception as e:
            logger.error(f"Analytics computation failed: {e}", exc_info=True)
            results["status"] = "failed"
            results["error"] = str(e)
            results["progress"] = self.progress.to_dict()

        return results

    def _build_all_positions_bulk(
        self,
        accounts: List[Account],
        start_date: Optional[date],
        end_date: date,
        force_full_rebuild: bool = False
    ) -> Dict[str, Any]:
        """
        Build positions for all accounts using bulk operations.

        Optimizations:
        1. Skip accounts with no transactions
        2. Incremental updates - only process from last transaction date for accounts with existing positions
        3. Batch processing with periodic commits

        Args:
            force_full_rebuild: If True, rebuild all positions regardless of existing data
        """
        total_positions = 0
        accounts_processed = 0
        accounts_skipped = 0

        # Get trading calendar once
        trading_dates = self._get_trading_calendar(start_date or self.MIN_DATE, end_date)

        # Pre-fetch account transaction counts and last transaction dates for filtering
        account_txn_info = {}
        txn_stats = self.db.query(
            Transaction.account_id,
            func.count(Transaction.id).label('txn_count'),
            func.max(Transaction.trade_date).label('last_txn_date')
        ).filter(
            Transaction.trade_date.isnot(None),
            Transaction.security_id.isnot(None)
        ).group_by(Transaction.account_id).all()

        for stat in txn_stats:
            account_txn_info[stat.account_id] = {
                'txn_count': stat.txn_count,
                'last_txn_date': stat.last_txn_date
            }

        # Pre-fetch last position dates for incremental updates
        position_stats = self.db.query(
            PositionsEOD.account_id,
            func.max(PositionsEOD.date).label('last_pos_date')
        ).group_by(PositionsEOD.account_id).all()

        last_position_dates = {stat.account_id: stat.last_pos_date for stat in position_stats}

        for account in accounts:
            try:
                # Skip accounts with no transactions
                if account.id not in account_txn_info:
                    accounts_skipped += 1
                    continue

                info = account_txn_info[account.id]
                last_pos_date = last_position_dates.get(account.id)

                # Determine if we need full rebuild or incremental
                if force_full_rebuild:
                    # Force full rebuild - process all accounts from the beginning
                    incremental_start = None  # None means full rebuild
                elif last_pos_date and info['last_txn_date']:
                    if last_pos_date >= info['last_txn_date'] and last_pos_date >= end_date - timedelta(days=5):
                        # Positions are up to date, skip
                        accounts_skipped += 1
                        continue
                    elif last_pos_date >= info['last_txn_date']:
                        # Just need to extend positions to end_date (no new transactions)
                        # Use incremental start date
                        incremental_start = last_pos_date - timedelta(days=1)
                    else:
                        # New transactions since last position - rebuild from that date
                        incremental_start = start_date
                else:
                    incremental_start = start_date

                count = self._build_positions_for_account_bulk(
                    account.id, trading_dates, incremental_start, end_date
                )
                total_positions += count
                accounts_processed += 1

                # Commit periodically
                if accounts_processed % self.ACCOUNT_BATCH_SIZE == 0:
                    self.db.commit()
                    logger.info(f"Positions: processed {accounts_processed}/{len(accounts)} accounts ({accounts_skipped} skipped)")

            except Exception as e:
                self.progress.add_error(str(e), {"account_id": account.id})
                self.db.rollback()

        self.db.commit()

        logger.info(f"Positions complete: {accounts_processed} processed, {accounts_skipped} skipped")

        return {
            "accounts_processed": accounts_processed,
            "accounts_skipped": accounts_skipped,
            "total_positions": total_positions
        }

    def _build_positions_for_account_bulk(
        self,
        account_id: int,
        trading_dates: List[date],
        start_date: Optional[date],
        end_date: date
    ) -> int:
        """
        Build positions for a single account using bulk insert.

        Uses vectorized pandas operations with proper forward-fill to handle
        transactions that occur on non-trading days. Supports inception data
        as starting positions.
        """
        # Check for inception data
        inception = self.db.query(AccountInception).filter(
            AccountInception.account_id == account_id
        ).first()

        inception_positions = {}  # security_id -> shares at inception
        if inception:
            for pos in inception.positions:
                if pos.shares > 0:
                    inception_positions[pos.security_id] = pos.shares

        # Get all transactions for this account, excluding options (no reliable prices)
        query = self.db.query(
            Transaction.security_id,
            Transaction.trade_date,
            Transaction.transaction_type,
            Transaction.units
        ).join(Security, Transaction.security_id == Security.id).filter(
            and_(
                Transaction.account_id == account_id,
                Transaction.trade_date.isnot(None),
                Transaction.security_id.isnot(None),
                Transaction.trade_date <= end_date,
                Security.is_option == False  # Exclude options
            )
        )

        transactions = query.order_by(Transaction.trade_date).all()

        # If no transactions and no inception, nothing to build
        if not transactions and not inception_positions:
            return 0

        # Filter trading dates
        trading_dates_filtered = [d for d in trading_dates if d <= end_date]

        # If we have inception, include inception date in calendar
        if inception:
            # Make sure we include dates from inception onwards
            if not start_date or start_date > inception.inception_date:
                start_date = inception.inception_date

        if start_date:
            trading_dates_filtered = [d for d in trading_dates_filtered if d >= start_date]

        # Ensure we have at least end_date if no trading dates
        if not trading_dates_filtered:
            trading_dates_filtered = [end_date]

        # Convert to DataFrame for vectorized operations
        if transactions:
            txn_df = pd.DataFrame(transactions, columns=['security_id', 'trade_date', 'txn_type', 'units'])

            # Calculate deltas
            def calc_delta(row):
                return self._get_transaction_delta(row['txn_type'], row['units'] or 0)

            txn_df['delta'] = txn_df.apply(calc_delta, axis=1)

            # Filter out zero deltas
            txn_df = txn_df[txn_df['delta'] != 0]

            if not txn_df.empty:
                # Group by security and date, sum deltas
                daily_changes = txn_df.groupby(['security_id', 'trade_date'])['delta'].sum().reset_index()
                # Get unique securities from transactions
                transaction_securities = set(daily_changes['security_id'].unique())
            else:
                daily_changes = pd.DataFrame(columns=['security_id', 'trade_date', 'delta'])
                transaction_securities = set()
        else:
            daily_changes = pd.DataFrame(columns=['security_id', 'trade_date', 'delta'])
            transaction_securities = set()

        # Combine transaction securities with inception securities
        all_securities = transaction_securities.union(set(inception_positions.keys()))

        if not all_securities:
            return 0

        positions_to_insert = []

        # Process each security separately to handle forward-fill correctly
        for security_id in all_securities:
            starting_shares = inception_positions.get(security_id, 0.0)
            sec_changes = daily_changes[daily_changes['security_id'] == security_id].copy() if not daily_changes.empty else pd.DataFrame()

            # Create a series with the trading calendar as index (as Timestamps)
            trading_index = pd.DatetimeIndex([pd.Timestamp(d) for d in trading_dates_filtered])

            if not sec_changes.empty:
                # Convert trade_date to Timestamp for consistent index type
                sec_changes['trade_date'] = pd.to_datetime(sec_changes['trade_date'])
                sec_changes = sec_changes.set_index('trade_date')['delta']

                # Compute cumulative position at each transaction date, starting from inception
                cumulative_positions = starting_shares + sec_changes.cumsum()

                # Combine transaction dates with trading dates, then forward-fill
                all_dates = cumulative_positions.index.union(trading_index).sort_values()

                # Set inception date value if we have inception
                if inception and inception_positions.get(security_id):
                    inception_ts = pd.Timestamp(inception.inception_date)
                    if inception_ts not in cumulative_positions.index:
                        cumulative_positions[inception_ts] = starting_shares
                        cumulative_positions = cumulative_positions.sort_index()

                full_positions = cumulative_positions.reindex(all_dates)

                # Forward fill, but first set inception value
                if inception and inception_positions.get(security_id):
                    inception_ts = pd.Timestamp(inception.inception_date)
                    if inception_ts in full_positions.index and pd.isna(full_positions[inception_ts]):
                        full_positions[inception_ts] = starting_shares

                full_positions = full_positions.ffill().fillna(starting_shares if starting_shares else 0)

                # Filter to only trading dates
                full_positions = full_positions.reindex(trading_index)
                # Fill any remaining NaN with starting_shares
                full_positions = full_positions.fillna(starting_shares if starting_shares else 0)
            else:
                # No transactions for this security, just use inception position for all dates
                full_positions = pd.Series(starting_shares, index=trading_index)

            # Add to insert list
            for trade_date, shares in full_positions.items():
                if shares != 0:
                    # Convert Timestamp back to date for database
                    date_val = trade_date.date() if hasattr(trade_date, 'date') else trade_date
                    positions_to_insert.append({
                        "account_id": account_id,
                        "security_id": int(security_id),
                        "date": date_val,
                        "shares": float(shares)
                    })

        if not positions_to_insert:
            return 0

        # Bulk upsert positions
        return self._bulk_upsert_positions(positions_to_insert)

    def _bulk_upsert_positions(self, positions: List[Dict]) -> int:
        """Bulk upsert positions using PostgreSQL ON CONFLICT"""
        if not positions:
            return 0

        # Process in batches to avoid memory issues
        total_inserted = 0

        for i in range(0, len(positions), self.POSITION_BATCH_SIZE):
            batch = positions[i:i + self.POSITION_BATCH_SIZE]

            stmt = insert(PositionsEOD).values(batch)
            stmt = stmt.on_conflict_do_update(
                index_elements=['account_id', 'security_id', 'date'],
                set_={'shares': stmt.excluded.shares}
            )

            self.db.execute(stmt)
            total_inserted += len(batch)

        return total_inserted

    def _compute_all_values_bulk(
        self,
        accounts: List[Account],
        start_date: Optional[date],
        end_date: date
    ) -> Dict[str, Any]:
        """Compute portfolio values for all accounts"""
        total_values = 0
        accounts_processed = 0

        for account in accounts:
            try:
                count = self._compute_values_for_account_bulk(
                    account.id, start_date, end_date
                )
                total_values += count
                accounts_processed += 1

                if accounts_processed % self.ACCOUNT_BATCH_SIZE == 0:
                    self.db.commit()
                    logger.info(f"Values: processed {accounts_processed}/{len(accounts)} accounts")

            except Exception as e:
                self.progress.add_error(str(e), {"account_id": account.id})
                self.db.rollback()

        self.db.commit()

        return {
            "accounts_processed": accounts_processed,
            "total_values": total_values
        }

    def _compute_values_for_account_bulk(
        self,
        account_id: int,
        start_date: Optional[date],
        end_date: date
    ) -> int:
        """
        Compute portfolio values using vectorized operations.

        Uses forward-filled prices to handle missing price data correctly.
        Missing prices on a given day will use the last known price instead of 0.
        """
        # Get positions and prices separately for proper forward-fill handling
        # Join with securities to exclude options (no reliable prices)
        query = """
            SELECT
                p.date,
                p.security_id,
                p.shares,
                pr.close as price
            FROM positions_eod p
            JOIN securities s ON p.security_id = s.id
            LEFT JOIN prices_eod pr ON p.security_id = pr.security_id AND p.date = pr.date
            WHERE p.account_id = :account_id
              AND p.date <= :end_date
              AND (s.is_option = false OR s.is_option IS NULL)
        """
        params = {"account_id": account_id, "end_date": end_date}

        if start_date:
            query += " AND p.date >= :start_date"
            params["start_date"] = start_date

        query += " ORDER BY p.date, p.security_id"

        result = self.db.execute(text(query), params)
        rows = result.fetchall()

        if not rows:
            return 0

        # Convert to DataFrame
        df = pd.DataFrame(rows, columns=['date', 'security_id', 'shares', 'price'])

        # Pivot prices and forward-fill missing values
        price_wide = df.pivot_table(
            index='date', columns='security_id', values='price', aggfunc='first'
        )
        # Forward fill, then backward fill for any missing at start, then 0 for truly missing
        price_wide = price_wide.ffill().bfill().fillna(0)

        # Pivot positions
        pos_wide = df.pivot_table(
            index='date', columns='security_id', values='shares', aggfunc='sum'
        ).fillna(0)

        # Align columns
        common_securities = list(set(pos_wide.columns) & set(price_wide.columns))
        if not common_securities:
            return 0

        pos_wide = pos_wide[common_securities]
        price_wide = price_wide[common_securities]

        # Compute portfolio values: shares * prices, summed across securities
        portfolio_values = (pos_wide * price_wide).sum(axis=1)

        # Prepare for bulk upsert
        values_to_insert = [
            {
                "view_type": ViewType.ACCOUNT,
                "view_id": account_id,
                "date": trade_date,
                "total_value": float(value)
            }
            for trade_date, value in portfolio_values.items()
            if value is not None and value > 0
        ]

        if not values_to_insert:
            return 0

        # Bulk upsert
        stmt = insert(PortfolioValueEOD).values(values_to_insert)
        stmt = stmt.on_conflict_do_update(
            index_elements=['view_type', 'view_id', 'date'],
            set_={'total_value': stmt.excluded.total_value}
        )
        self.db.execute(stmt)

        return len(values_to_insert)

    def _compute_all_returns_bulk(
        self,
        accounts: List[Account],
        start_date: Optional[date],
        end_date: date
    ) -> Dict[str, Any]:
        """Compute returns for all accounts"""
        total_returns = 0
        accounts_processed = 0

        for account in accounts:
            try:
                count = self._compute_returns_for_account_bulk(
                    account.id, start_date, end_date
                )
                total_returns += count
                accounts_processed += 1

                if accounts_processed % self.ACCOUNT_BATCH_SIZE == 0:
                    self.db.commit()
                    logger.info(f"Returns: processed {accounts_processed}/{len(accounts)} accounts")

            except Exception as e:
                self.progress.add_error(str(e), {"account_id": account.id})
                self.db.rollback()

        self.db.commit()

        return {
            "accounts_processed": accounts_processed,
            "total_returns": total_returns
        }

    def _compute_returns_for_account_bulk(
        self,
        account_id: int,
        start_date: Optional[date],
        end_date: date
    ) -> int:
        """
        Compute TWR returns using equity sleeve methodology.

        r_t = V_t^{no-trade} / V_{t-1} - 1
        where V_t^{no-trade} = sum(shares_{t-1} * price_t)
        """
        # Get positions and prices efficiently using raw SQL for performance
        # Exclude options from calculations (no reliable prices)
        query = """
            WITH position_prices AS (
                SELECT
                    p.date,
                    p.security_id,
                    p.shares,
                    pr.close as price
                FROM positions_eod p
                JOIN securities s ON p.security_id = s.id
                LEFT JOIN prices_eod pr ON p.security_id = pr.security_id AND p.date = pr.date
                WHERE p.account_id = :account_id
                  AND p.date <= :end_date
                  AND (s.is_option = false OR s.is_option IS NULL)
        """
        params = {"account_id": account_id, "end_date": end_date}

        if start_date:
            # Get one day before start_date for proper return calculation
            query += " AND p.date >= :lookback_date"
            params["lookback_date"] = start_date - timedelta(days=30)  # Buffer for weekends

        query += """
            )
            SELECT date, security_id, shares, price
            FROM position_prices
            ORDER BY date, security_id
        """

        result = self.db.execute(text(query), params)
        rows = result.fetchall()

        if not rows:
            return 0

        # Convert to DataFrame for vectorized computation
        df = pd.DataFrame(rows, columns=['date', 'security_id', 'shares', 'price'])
        # Don't fill NaN prices with 0 here - we'll handle them after pivoting

        # Pivot to wide format
        pos_wide = df.pivot_table(
            index='date', columns='security_id', values='shares', aggfunc='sum'
        ).fillna(0)

        price_wide = df.pivot_table(
            index='date', columns='security_id', values='price', aggfunc='first'
        )

        # Forward fill prices first, then backward fill any remaining at start
        # This ensures we use last known price instead of 0 for missing data
        price_wide = price_wide.ffill().bfill().fillna(0)

        # Align indices
        common_dates = sorted(set(pos_wide.index) & set(price_wide.index))
        if len(common_dates) < 2:
            return 0

        pos_wide = pos_wide.reindex(common_dates)
        price_wide = price_wide.reindex(common_dates)

        # Compute portfolio values
        portfolio_values = (pos_wide * price_wide).sum(axis=1)

        # Get fees
        fees = self._get_fees_for_account(account_id, start_date, end_date)

        # Compute returns
        returns_data = []
        index_value = 1.0

        for i in range(1, len(common_dates)):
            date_t = common_dates[i]
            date_t_minus_1 = common_dates[i - 1]

            # Filter by start_date if provided
            if start_date and date_t < start_date:
                continue

            V_t_minus_1 = portfolio_values.iloc[i - 1]
            if V_t_minus_1 <= 0:
                continue

            # No-trade value: yesterday's holdings at today's prices
            holdings_t_minus_1 = pos_wide.iloc[i - 1]
            prices_t = price_wide.iloc[i]
            V_t_no_trade = (holdings_t_minus_1 * prices_t).sum()

            # Gross return
            r_gross = (V_t_no_trade / V_t_minus_1) - 1

            # Fee drag
            fee_t = fees.get(date_t, 0.0)
            fee_drag = fee_t / V_t_minus_1 if V_t_minus_1 > 0 else 0

            # Net return
            r_net = r_gross - fee_drag

            # Update index
            index_value = index_value * (1 + r_net)

            returns_data.append({
                "view_type": ViewType.ACCOUNT,
                "view_id": account_id,
                "date": date_t,
                "twr_return": float(r_net),
                "twr_index": float(index_value)
            })

        if not returns_data:
            return 0

        # Bulk upsert returns
        stmt = insert(ReturnsEOD).values(returns_data)
        stmt = stmt.on_conflict_do_update(
            index_elements=['view_type', 'view_id', 'date'],
            set_={
                'twr_return': stmt.excluded.twr_return,
                'twr_index': stmt.excluded.twr_index
            }
        )
        self.db.execute(stmt)

        return len(returns_data)

    def _get_trading_calendar(self, start_date: date, end_date: date) -> List[date]:
        """Get trading dates from price data"""
        if self._trading_dates:
            return [d for d in self._trading_dates if start_date <= d <= end_date]

        dates = self.db.query(PricesEOD.date).filter(
            and_(
                PricesEOD.date >= start_date,
                PricesEOD.date <= end_date
            )
        ).distinct().order_by(PricesEOD.date).all()

        self._trading_dates = [d[0] for d in dates]

        if not self._trading_dates:
            # Fall back to all dates
            all_dates = pd.date_range(start=start_date, end=end_date, freq='D')
            self._trading_dates = [d.date() for d in all_dates]

        return self._trading_dates

    def _get_transaction_delta(self, txn_type: TransactionType, units: float) -> float:
        """Get share delta for transaction type"""
        if txn_type in [TransactionType.BUY, TransactionType.TRANSFER_IN, TransactionType.DIVIDEND_REINVEST]:
            return abs(units)
        elif txn_type in [TransactionType.SELL, TransactionType.TRANSFER_OUT]:
            return -abs(units)
        return 0.0

    def _get_fees_for_account(
        self,
        account_id: int,
        start_date: Optional[date],
        end_date: date
    ) -> Dict[date, float]:
        """Get daily fee totals for an account"""
        query = self.db.query(
            Transaction.trade_date,
            func.sum(Transaction.transaction_fee).label('total_fee')
        ).filter(
            and_(
                Transaction.account_id == account_id,
                Transaction.trade_date <= end_date
            )
        )

        if start_date:
            query = query.filter(Transaction.trade_date >= start_date)

        fees = query.group_by(Transaction.trade_date).all()
        return {f[0]: float(f[1]) if f[1] else 0.0 for f in fees}


class PostImportAnalyticsJob:
    """
    Runs analytics computation after a bulk import.

    Features:
    - Determines affected accounts from import
    - Runs optimized batch analytics
    - Provides progress tracking
    - Can be run incrementally (only new data)
    """

    def __init__(self, db: Session):
        self.db = db
        self.analytics_service = BatchAnalyticsService(db)

    def run_for_import(
        self,
        import_job_id: Optional[int] = None,
        incremental: bool = True
    ) -> Dict[str, Any]:
        """
        Run analytics for accounts affected by an import.

        Args:
            import_job_id: Bulk import job ID (None = all accounts)
            incremental: Only compute from earliest new transaction date

        Returns:
            Analytics computation results
        """
        logger.info(f"Running post-import analytics (job_id={import_job_id}, incremental={incremental})")

        # Determine affected accounts and date range
        if import_job_id:
            # Get accounts affected by this import
            # This would require tracking which accounts were affected by the import
            # For now, we get all accounts with transactions
            account_ids = self._get_accounts_with_transactions()
            start_date = self._get_earliest_transaction_date(import_job_id) if incremental else None
        else:
            account_ids = None
            start_date = None

        # Run batch analytics
        return self.analytics_service.run_full_analytics(
            account_ids=account_ids,
            start_date=start_date
        )

    def _get_accounts_with_transactions(self) -> List[int]:
        """Get all account IDs that have transactions or inception data"""
        txn_accounts = set(r[0] for r in self.db.query(Transaction.account_id).distinct().all())
        inception_accounts = set(r[0] for r in self.db.query(AccountInception.account_id).distinct().all())
        return list(txn_accounts.union(inception_accounts))

    def _get_earliest_transaction_date(self, import_job_id: int) -> Optional[date]:
        """Get earliest transaction date from an import"""
        # This would require the import job to track the date range
        # For now, return None to process all dates
        return None


async def run_post_import_analytics(
    import_job_id: Optional[int] = None,
    incremental: bool = True
) -> Dict[str, Any]:
    """
    Background task to run analytics after bulk import.

    Call this after bulk import completes.
    """
    db = SessionLocal()
    try:
        job = PostImportAnalyticsJob(db)
        result = job.run_for_import(import_job_id, incremental)
        logger.info(f"Post-import analytics completed: {result.get('status')}")
        return result
    except Exception as e:
        logger.error(f"Post-import analytics failed: {e}", exc_info=True)
        raise
    finally:
        db.close()
