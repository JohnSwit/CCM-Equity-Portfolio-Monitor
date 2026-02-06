"""
Historical Portfolio Inception Parser

Parses CSV files containing starting portfolio positions at an inception date.
This allows calculating returns from a historical starting point.

Expected CSV Format:
- Column A: Account Number
- Column B: Account Display Name
- Column C: Class (asset class)
- Column D: Asset Name
- Column E: Symbol
- Column F: Units (shares)
- Column G: Price
- Column H: Market Value
- Column I: Inception Date
"""

import pandas as pd
import io
from typing import Dict, Any, List, Tuple, Optional
from datetime import datetime, date
from sqlalchemy.orm import Session
from sqlalchemy import and_

from app.models import (
    Account, Security, AssetClass, ImportLog,
    AccountInception, InceptionPosition, PositionsEOD, PricesEOD
)


class InceptionParser:
    """Parser for historical portfolio inception CSV files"""

    REQUIRED_HEADERS = [
        'Account Number', 'Account Display Name', 'Class', 'Asset Name',
        'Symbol', 'Units', 'Price', 'Market Value', 'Inception Date'
    ]

    # Alternative header names for flexibility
    HEADER_ALIASES = {
        'Account Number': ['Account Number', 'AccountNumber', 'Account #', 'Acct Number'],
        'Account Display Name': ['Account Display Name', 'Display Name', 'Account Name', 'Name'],
        'Class': ['Class', 'Asset Class', 'Type'],
        'Asset Name': ['Asset Name', 'Security Name', 'Name', 'Description'],
        'Symbol': ['Symbol', 'Ticker', 'Security'],
        'Units': ['Units', 'Shares', 'Quantity', 'Qty'],
        'Price': ['Price', 'Unit Price', 'Share Price'],
        'Market Value': ['Market Value', 'Value', 'Total Value', 'MV'],
        'Inception Date': ['Inception Date', 'Date', 'As Of Date', 'Start Date'],
    }

    def __init__(self, db: Session):
        self.db = db

    def parse_csv(self, file_content: bytes, preview: bool = False, max_preview_rows: int = 20) -> Dict[str, Any]:
        """Parse CSV and return preview or full parse results"""
        try:
            content_str = file_content.decode('utf-8')

            # Try different delimiters
            if '\t' in content_str:
                df = pd.read_csv(io.StringIO(content_str), sep='\t')
            else:
                df = pd.read_csv(io.StringIO(content_str))

            # Normalize column names
            df = self._normalize_headers(df)

            # Validate required headers
            missing_headers = set(self.REQUIRED_HEADERS) - set(df.columns)
            if missing_headers:
                return {
                    'error': f'Missing required headers: {missing_headers}',
                    'has_errors': True,
                    'found_headers': list(df.columns)
                }

            # Clean and prepare data
            df = self._clean_dataframe(df)

            if preview:
                return self._generate_preview(df, max_preview_rows)
            else:
                return {'dataframe': df, 'has_errors': False}

        except Exception as e:
            return {
                'error': f'Failed to parse CSV: {str(e)}',
                'has_errors': True
            }

    def _normalize_headers(self, df: pd.DataFrame) -> pd.DataFrame:
        """Normalize header names to standard format"""
        rename_map = {}
        for standard_name, aliases in self.HEADER_ALIASES.items():
            for alias in aliases:
                if alias in df.columns and alias != standard_name:
                    rename_map[alias] = standard_name
                    break
        if rename_map:
            df = df.rename(columns=rename_map)
        return df

    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and normalize dataframe"""
        # Trim whitespace
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].str.strip()

        # Normalize Symbol to uppercase
        df['Symbol'] = df['Symbol'].str.upper()

        # Parse inception date
        df['Inception Date'] = pd.to_datetime(df['Inception Date'], errors='coerce')

        # Parse numeric fields
        for col in ['Units', 'Price', 'Market Value']:
            # Remove currency symbols and commas
            if df[col].dtype == 'object':
                df[col] = df[col].str.replace(r'[\$,]', '', regex=True)
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)

        # Filter out rows with zero or negative units (closed positions)
        df = df[df['Units'] > 0].copy()

        return df

    def _generate_preview(self, df: pd.DataFrame, max_rows: int) -> Dict[str, Any]:
        """Generate preview with validation and summary"""
        preview_rows = []
        accounts_summary = {}
        errors_found = False

        # Get unique inception dates
        inception_dates = df['Inception Date'].dropna().unique()
        if len(inception_dates) > 1:
            errors_found = True

        for idx, row in df.head(max_rows).iterrows():
            row_errors = []

            # Validate required fields
            if pd.isna(row['Inception Date']):
                row_errors.append('Invalid Inception Date')
            if not row['Symbol']:
                row_errors.append('Missing Symbol')
            if not row['Account Number']:
                row_errors.append('Missing Account Number')
            if row['Units'] <= 0:
                row_errors.append('Units must be positive')

            if row_errors:
                errors_found = True

            # Track accounts and positions
            acct_num = str(row['Account Number'])
            if acct_num not in accounts_summary:
                accounts_summary[acct_num] = {
                    'display_name': row['Account Display Name'],
                    'position_count': 0,
                    'total_value': 0.0
                }
            accounts_summary[acct_num]['position_count'] += 1
            accounts_summary[acct_num]['total_value'] += row['Market Value']

            preview_rows.append({
                'row_num': int(idx) + 2,
                'data': {
                    'Account Number': str(row['Account Number']),
                    'Account Display Name': str(row['Account Display Name']),
                    'Symbol': str(row['Symbol']),
                    'Units': float(row['Units']),
                    'Price': float(row['Price']),
                    'Market Value': float(row['Market Value']),
                    'Inception Date': str(row['Inception Date'].date()) if pd.notna(row['Inception Date']) else '',
                    'Class': str(row['Class']) if pd.notna(row['Class']) else '',
                    'Asset Name': str(row['Asset Name']) if pd.notna(row['Asset Name']) else ''
                },
                'errors': row_errors
            })

        # Determine inception date
        inception_date = None
        if len(inception_dates) == 1:
            inception_date = pd.Timestamp(inception_dates[0]).date().isoformat()

        return {
            'total_rows': len(df),
            'preview_rows': preview_rows,
            'inception_date': inception_date,
            'accounts_summary': [
                {
                    'account_number': k,
                    'display_name': v['display_name'],
                    'position_count': v['position_count'],
                    'total_value': v['total_value']
                }
                for k, v in accounts_summary.items()
            ],
            'has_errors': errors_found,
            'multiple_dates_warning': len(inception_dates) > 1
        }

    def commit_inception(self, df: pd.DataFrame, user_id: int = None) -> Dict[str, Any]:
        """
        Commit inception positions to database.
        Creates accounts, securities, inception records, and initial positions.
        """
        try:
            # Create import log
            import_log = ImportLog(
                filename='inception_upload',
                source='inception',
                status='processing',
                records_total=len(df),
                records_processed=0,
                records_skipped=0,
                records_errors=0,
                user_id=user_id
            )
            self.db.add(import_log)
            self.db.flush()

            # Get unique inception date (should be single date)
            inception_dates = df['Inception Date'].dropna().unique()
            if len(inception_dates) != 1:
                raise ValueError("All rows must have the same inception date")

            inception_date = pd.Timestamp(inception_dates[0]).date()

            results = {
                'accounts_created': 0,
                'accounts_updated': 0,
                'securities_created': 0,
                'positions_created': 0,
                'total_value': 0.0,
                'errors': []
            }

            # Group by account
            for acct_num, acct_df in df.groupby('Account Number'):
                try:
                    # Get or create account
                    account = self.db.query(Account).filter(
                        Account.account_number == str(acct_num)
                    ).first()

                    display_name = acct_df['Account Display Name'].iloc[0]

                    if not account:
                        account = Account(
                            account_number=str(acct_num),
                            display_name=str(display_name)
                        )
                        self.db.add(account)
                        self.db.flush()
                        results['accounts_created'] += 1
                    else:
                        results['accounts_updated'] += 1

                    # Check if inception already exists - delete old one
                    existing_inception = self.db.query(AccountInception).filter(
                        AccountInception.account_id == account.id
                    ).first()

                    if existing_inception:
                        self.db.delete(existing_inception)
                        self.db.flush()

                    # Create new inception record
                    account_total = acct_df['Market Value'].sum()
                    inception = AccountInception(
                        account_id=account.id,
                        inception_date=inception_date,
                        total_value=account_total,
                        import_log_id=import_log.id
                    )
                    self.db.add(inception)
                    self.db.flush()

                    # Create inception positions
                    for _, row in acct_df.iterrows():
                        # Get or create security
                        security = self._get_or_create_security(row)

                        # Create inception position
                        position = InceptionPosition(
                            inception_id=inception.id,
                            security_id=security.id,
                            shares=float(row['Units']),
                            price=float(row['Price']),
                            market_value=float(row['Market Value'])
                        )
                        self.db.add(position)
                        results['positions_created'] += 1

                    results['total_value'] += account_total

                except Exception as e:
                    results['errors'].append(f"Account {acct_num}: {str(e)}")

            # Update import log
            import_log.status = 'completed' if not results['errors'] else 'completed_with_errors'
            import_log.records_processed = results['positions_created']
            import_log.records_errors = len(results['errors'])
            import_log.completed_at = datetime.utcnow()

            self.db.commit()

            results['inception_date'] = inception_date.isoformat()
            results['import_log_id'] = import_log.id

            return results

        except Exception as e:
            self.db.rollback()
            return {
                'error': str(e),
                'has_errors': True
            }

    def _get_or_create_security(self, row: pd.Series) -> Security:
        """Get existing security or create new one"""
        symbol = str(row['Symbol']).upper()
        asset_class = self._classify_asset_class(row.get('Class', ''), symbol)

        security = self.db.query(Security).filter(
            Security.symbol == symbol,
            Security.asset_class == asset_class
        ).first()

        if not security:
            security = Security(
                symbol=symbol,
                asset_name=str(row['Asset Name']) if pd.notna(row.get('Asset Name')) else symbol,
                asset_class=asset_class,
                is_option=(asset_class == AssetClass.OPTION)
            )
            self.db.add(security)
            self.db.flush()

        return security

    def _classify_asset_class(self, class_str: str, symbol: str) -> AssetClass:
        """Classify asset class from string"""
        class_lower = str(class_str).lower() if class_str else ''

        if 'option' in class_lower or 'opt' in class_lower:
            return AssetClass.OPTION
        elif 'etf' in class_lower:
            return AssetClass.ETF
        elif 'cash' in class_lower or 'money market' in class_lower:
            return AssetClass.CASH
        elif 'equity' in class_lower or 'stock' in class_lower or 'common' in class_lower:
            return AssetClass.EQUITY
        else:
            # Default to equity for most securities
            return AssetClass.EQUITY

    def create_inception_positions_eod(self, account_id: int) -> Dict[str, Any]:
        """
        Create PositionsEOD records from inception positions.
        This seeds the position calculation with inception data.
        """
        inception = self.db.query(AccountInception).filter(
            AccountInception.account_id == account_id
        ).first()

        if not inception:
            return {'error': 'No inception found for account', 'created': 0}

        created = 0
        for pos in inception.positions:
            # Check if position already exists
            existing = self.db.query(PositionsEOD).filter(
                and_(
                    PositionsEOD.account_id == account_id,
                    PositionsEOD.security_id == pos.security_id,
                    PositionsEOD.date == inception.inception_date
                )
            ).first()

            if not existing:
                position_eod = PositionsEOD(
                    account_id=account_id,
                    security_id=pos.security_id,
                    date=inception.inception_date,
                    shares=pos.shares
                )
                self.db.add(position_eod)
                created += 1

        self.db.commit()
        return {'created': created, 'inception_date': inception.inception_date.isoformat()}


def get_account_inception_date(db: Session, account_id: int) -> Optional[date]:
    """Helper function to get inception date for an account"""
    inception = db.query(AccountInception).filter(
        AccountInception.account_id == account_id
    ).first()
    return inception.inception_date if inception else None


def get_accounts_with_inception(db: Session) -> List[Dict[str, Any]]:
    """Get all accounts that have inception data"""
    inceptions = db.query(AccountInception).all()
    return [
        {
            'account_id': inc.account_id,
            'account_number': inc.account.account_number,
            'display_name': inc.account.display_name,
            'inception_date': inc.inception_date.isoformat(),
            'total_value': inc.total_value,
            'position_count': len(inc.positions)
        }
        for inc in inceptions
    ]
