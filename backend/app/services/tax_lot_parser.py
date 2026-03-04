"""
Tax Lot CSV Parser Service

Parses tax lot CSV files and imports them into the database.
CSV Format:
- Column A: Account Number
- Column B: Account Display Name
- Column C: Class (Asset Class)
- Column D: Symbol
- Column E: Asset Name
- Column F: Open Date (Purchase Date)
- Column G: Unit Cost (Cost Basis Per Share)
- Column H: Units (Shares)
- Column I: Cost Basis (Total)
- Column J: Market Value
- Column K: Short-Term Gain/Loss
- Column L: Long-Term Gain/Loss
- Column M: Total Gain Loss
"""
import hashlib
import logging
from datetime import datetime, date
from typing import Dict, List, Optional, Any, Tuple
from io import StringIO

import pandas as pd
from sqlalchemy.orm import Session

from app.models import Account, Security, AssetClass, TaxLotImportLog
from app.models.models import TaxLot

logger = logging.getLogger(__name__)

# Expected column mappings (can be flexible with column names)
COLUMN_MAPPINGS = {
    'account_number': ['account number', 'account_number', 'acct', 'account'],
    'account_name': ['account display name', 'account_name', 'display name', 'name'],
    'asset_class': ['class', 'asset class', 'asset_class', 'type'],
    'symbol': ['symbol', 'ticker', 'security'],
    'asset_name': ['asset name', 'asset_name', 'security name', 'description'],
    'open_date': ['open date', 'open_date', 'purchase date', 'purchase_date', 'date'],
    'unit_cost': ['unit cost', 'unit_cost', 'cost per share', 'cost_per_share'],
    'units': ['units', 'shares', 'quantity'],
    'cost_basis': ['cost basis', 'cost_basis', 'total cost', 'total_cost'],
    'market_value': ['market value', 'market_value', 'value', 'current value'],
    'short_term_gain_loss': ['short-term gain/loss', 'short_term_gain_loss', 'st gain/loss', 'short term'],
    'long_term_gain_loss': ['long-term gain/loss', 'long_term_gain_loss', 'lt gain/loss', 'long term'],
    'total_gain_loss': ['total gain loss', 'total_gain_loss', 'gain/loss', 'total gain/loss', 'unrealized gain']
}

# Asset class mapping
ASSET_CLASS_MAP = {
    'equity': AssetClass.EQUITY,
    'stock': AssetClass.EQUITY,
    'common stock': AssetClass.EQUITY,
    'etf': AssetClass.ETF,
    'exchange traded fund': AssetClass.ETF,
    'option': AssetClass.OPTION,
    'cash': AssetClass.CASH,
    'money market': AssetClass.CASH,
    'other': AssetClass.OTHER,
}


def calculate_file_hash(content: bytes) -> str:
    """Calculate SHA256 hash of file content"""
    return hashlib.sha256(content).hexdigest()


class TaxLotParser:
    """Parser for tax lot CSV files"""

    def __init__(self, db: Session):
        self.db = db
        self.errors: List[Dict] = []
        self.warnings: List[Dict] = []

    def _decode_file_content(self, file_content: bytes) -> str:
        """Decode file content with multiple encoding attempts"""
        encodings = ['utf-8', 'utf-8-sig', 'cp1252', 'iso-8859-1']
        for encoding in encodings:
            try:
                decoded = file_content.decode(encoding)
                if encoding != 'utf-8':
                    logger.info(f"Tax lot file decoded using {encoding} encoding")
                return decoded
            except (UnicodeDecodeError, LookupError):
                continue
        return file_content.decode('iso-8859-1', errors='replace')

    def _find_column(self, df: pd.DataFrame, field_name: str) -> Optional[str]:
        """Find the actual column name in the DataFrame for a given field"""
        possible_names = COLUMN_MAPPINGS.get(field_name, [field_name])
        df_columns_lower = {col.lower().strip(): col for col in df.columns}

        for name in possible_names:
            if name.lower() in df_columns_lower:
                return df_columns_lower[name.lower()]
        return None

    def _parse_date(self, value: Any) -> Optional[date]:
        """Parse date from various formats"""
        if pd.isna(value):
            return None

        if isinstance(value, (datetime, date)):
            return value if isinstance(value, date) else value.date()

        str_val = str(value).strip()
        if not str_val:
            return None

        # Try common date formats
        formats = ['%m/%d/%Y', '%Y-%m-%d', '%m-%d-%Y', '%d/%m/%Y', '%Y/%m/%d']
        for fmt in formats:
            try:
                return datetime.strptime(str_val, fmt).date()
            except ValueError:
                continue

        return None

    def _parse_float(self, value: Any) -> Optional[float]:
        """Parse float from various formats"""
        if pd.isna(value):
            return None

        if isinstance(value, (int, float)):
            return float(value)

        str_val = str(value).strip()
        if not str_val:
            return None

        # Remove currency symbols and commas
        str_val = str_val.replace('$', '').replace(',', '').replace('(', '-').replace(')', '')

        try:
            return float(str_val)
        except ValueError:
            return None

    def _get_asset_class(self, value: Any) -> AssetClass:
        """Map asset class string to AssetClass enum"""
        if pd.isna(value):
            return AssetClass.EQUITY  # Default

        str_val = str(value).lower().strip()
        return ASSET_CLASS_MAP.get(str_val, AssetClass.OTHER)

    def parse_csv(self, file_content: bytes, preview: bool = True) -> Dict[str, Any]:
        """
        Parse the tax lot CSV file.

        Args:
            file_content: Raw CSV file bytes
            preview: If True, only validate and return preview data

        Returns:
            Dictionary with parsing results
        """
        self.errors = []
        self.warnings = []

        try:
            content = self._decode_file_content(file_content)
            df = pd.read_csv(StringIO(content))
        except Exception as e:
            return {'error': f'Failed to parse CSV: {str(e)}'}

        if df.empty:
            return {'error': 'CSV file is empty'}

        # Find required columns
        col_account_number = self._find_column(df, 'account_number')
        col_symbol = self._find_column(df, 'symbol')
        col_open_date = self._find_column(df, 'open_date')
        col_units = self._find_column(df, 'units')
        col_unit_cost = self._find_column(df, 'unit_cost')

        missing_cols = []
        if not col_account_number:
            missing_cols.append('Account Number')
        if not col_symbol:
            missing_cols.append('Symbol')
        if not col_open_date:
            missing_cols.append('Open Date')
        if not col_units:
            missing_cols.append('Units')
        if not col_unit_cost:
            missing_cols.append('Unit Cost')

        if missing_cols:
            return {'error': f'Missing required columns: {", ".join(missing_cols)}. Found columns: {list(df.columns)}'}

        # Find optional columns
        col_account_name = self._find_column(df, 'account_name')
        col_asset_class = self._find_column(df, 'asset_class')
        col_asset_name = self._find_column(df, 'asset_name')
        col_cost_basis = self._find_column(df, 'cost_basis')
        col_market_value = self._find_column(df, 'market_value')
        col_st_gain_loss = self._find_column(df, 'short_term_gain_loss')
        col_lt_gain_loss = self._find_column(df, 'long_term_gain_loss')
        col_total_gain_loss = self._find_column(df, 'total_gain_loss')

        # Process rows
        parsed_rows = []
        for idx, row in df.iterrows():
            row_num = idx + 2  # +2 for 0-index and header row

            try:
                account_number = str(row[col_account_number]).strip() if pd.notna(row[col_account_number]) else None
                symbol = str(row[col_symbol]).strip().upper() if pd.notna(row[col_symbol]) else None
                open_date = self._parse_date(row[col_open_date])
                units = self._parse_float(row[col_units])
                unit_cost = self._parse_float(row[col_unit_cost])

                # Validate required fields
                if not account_number:
                    self.errors.append({'row': row_num, 'error': 'Missing account number'})
                    continue
                if not symbol:
                    self.errors.append({'row': row_num, 'error': 'Missing symbol'})
                    continue
                if not open_date:
                    self.errors.append({'row': row_num, 'error': 'Invalid or missing open date'})
                    continue
                if units is None:
                    self.errors.append({'row': row_num, 'error': 'Missing units'})
                    continue
                if units == 0:
                    self.warnings.append({'row': row_num, 'warning': 'Skipped closed lot (0 units)'})
                    continue
                if unit_cost is None:
                    self.errors.append({'row': row_num, 'error': 'Invalid or missing unit cost'})
                    continue

                # Normalize negative values (some systems use negatives for short positions)
                if units < 0:
                    self.warnings.append({'row': row_num, 'warning': f'Negative units ({units}) converted to positive'})
                    units = abs(units)
                if unit_cost < 0:
                    self.warnings.append({'row': row_num, 'warning': f'Negative unit cost ({unit_cost}) converted to positive'})
                    unit_cost = abs(unit_cost)

                # Parse optional fields
                account_name = str(row[col_account_name]).strip() if col_account_name and pd.notna(row[col_account_name]) else None
                asset_class = self._get_asset_class(row[col_asset_class]) if col_asset_class else AssetClass.EQUITY
                asset_name = str(row[col_asset_name]).strip() if col_asset_name and pd.notna(row[col_asset_name]) else symbol
                cost_basis = self._parse_float(row[col_cost_basis]) if col_cost_basis else (units * unit_cost)
                market_value = self._parse_float(row[col_market_value]) if col_market_value else None
                st_gain_loss = self._parse_float(row[col_st_gain_loss]) if col_st_gain_loss else None
                lt_gain_loss = self._parse_float(row[col_lt_gain_loss]) if col_lt_gain_loss else None
                total_gain_loss = self._parse_float(row[col_total_gain_loss]) if col_total_gain_loss else None

                parsed_rows.append({
                    'row_num': row_num,
                    'account_number': account_number,
                    'account_name': account_name or account_number,
                    'asset_class': asset_class,
                    'symbol': symbol,
                    'asset_name': asset_name,
                    'open_date': open_date,
                    'unit_cost': unit_cost,
                    'units': units,
                    'cost_basis': cost_basis,
                    'market_value': market_value,
                    'short_term_gain_loss': st_gain_loss,
                    'long_term_gain_loss': lt_gain_loss,
                    'total_gain_loss': total_gain_loss
                })

            except Exception as e:
                self.errors.append({'row': row_num, 'error': f'Parse error: {str(e)}'})

        result = {
            'total_rows': len(df),
            'valid_rows': len(parsed_rows),
            'error_rows': len(self.errors),
            'errors': self.errors[:50],  # Limit errors shown
            'warnings': self.warnings[:50],
            'preview': parsed_rows[:20] if preview else None,
            'dataframe': None if preview else parsed_rows
        }

        return result

    def import_tax_lots(
        self,
        parsed_rows: List[Dict],
        file_name: str,
        file_hash: str
    ) -> Dict[str, Any]:
        """
        Import parsed tax lots into the database.

        Args:
            parsed_rows: List of parsed row dictionaries
            file_name: Original filename
            file_hash: SHA256 hash of file content

        Returns:
            Import result summary
        """
        # Create import log
        import_log = TaxLotImportLog(
            file_name=file_name,
            file_hash=file_hash,
            status='processing',
            rows_processed=len(parsed_rows)
        )
        self.db.add(import_log)
        self.db.flush()

        imported = 0
        skipped = 0
        errors = []

        # Cache for accounts and securities
        account_cache: Dict[str, Account] = {}
        security_cache: Dict[str, Security] = {}

        # Track lots already seen in THIS import to detect true duplicates
        # vs separate lots that happen to share (account, security, date, units, cost)
        seen_in_import: Dict[Tuple, int] = {}

        for row in parsed_rows:
            try:
                # Get or create account
                account = self._get_or_create_account(
                    row['account_number'],
                    row['account_name'],
                    account_cache
                )

                # Get or create security
                security = self._get_or_create_security(
                    row['symbol'],
                    row['asset_name'],
                    row['asset_class'],
                    security_cache
                )

                # Build dedup key from all distinguishing fields
                dedup_key = (
                    account.id, security.id, row['open_date'],
                    row['units'], row['unit_cost']
                )

                # Count how many times we've seen this exact combination in this import
                seen_count = seen_in_import.get(dedup_key, 0)

                # Check for duplicates from PREVIOUS imports only
                # Count existing lots with the same key
                existing_count = self.db.query(TaxLot).filter(
                    TaxLot.account_id == account.id,
                    TaxLot.security_id == security.id,
                    TaxLot.purchase_date == row['open_date'],
                    TaxLot.original_shares == row['units'],
                    TaxLot.cost_basis_per_share == row['unit_cost']
                ).count()

                if seen_count < existing_count:
                    # This is a duplicate of an already-imported lot - update it
                    existing_lots = self.db.query(TaxLot).filter(
                        TaxLot.account_id == account.id,
                        TaxLot.security_id == security.id,
                        TaxLot.purchase_date == row['open_date'],
                        TaxLot.original_shares == row['units'],
                        TaxLot.cost_basis_per_share == row['unit_cost']
                    ).order_by(TaxLot.id).all()

                    existing = existing_lots[seen_count]
                    existing.market_value = row['market_value']
                    existing.short_term_gain_loss = row['short_term_gain_loss']
                    existing.long_term_gain_loss = row['long_term_gain_loss']
                    existing.total_gain_loss = row['total_gain_loss']
                    existing.import_log_id = import_log.id
                    existing.updated_at = datetime.utcnow()
                    seen_in_import[dedup_key] = seen_count + 1
                    skipped += 1
                    continue

                seen_in_import[dedup_key] = seen_count + 1

                # Create new tax lot
                tax_lot = TaxLot(
                    account_id=account.id,
                    security_id=security.id,
                    purchase_date=row['open_date'],
                    import_log_id=import_log.id,
                    original_shares=row['units'],
                    cost_basis_per_share=row['unit_cost'],
                    total_cost_basis=row['cost_basis'],
                    remaining_shares=row['units'],
                    remaining_cost_basis=row['cost_basis'],
                    market_value=row['market_value'],
                    short_term_gain_loss=row['short_term_gain_loss'],
                    long_term_gain_loss=row['long_term_gain_loss'],
                    total_gain_loss=row['total_gain_loss'],
                    is_closed=False
                )
                self.db.add(tax_lot)
                imported += 1

            except Exception as e:
                errors.append({
                    'row': row.get('row_num'),
                    'error': str(e)
                })

        # Update import log
        import_log.rows_imported = imported
        import_log.rows_skipped = skipped
        import_log.rows_error = len(errors)
        import_log.status = 'completed' if not errors else 'completed_with_errors'
        import_log.errors = errors[:100] if errors else None

        self.db.commit()

        return {
            'import_log_id': import_log.id,
            'file_name': file_name,
            'total_rows': len(parsed_rows),
            'imported': imported,
            'skipped': skipped,
            'errors': len(errors),
            'error_details': errors[:20]
        }

    def _get_or_create_account(
        self,
        account_number: str,
        display_name: str,
        cache: Dict[str, Account]
    ) -> Account:
        """Get existing account or create new one"""
        if account_number in cache:
            return cache[account_number]

        account = self.db.query(Account).filter(
            Account.account_number == account_number
        ).first()

        if not account:
            account = Account(
                account_number=account_number,
                display_name=display_name or account_number
            )
            self.db.add(account)
            self.db.flush()

        cache[account_number] = account
        return account

    def _get_or_create_security(
        self,
        symbol: str,
        asset_name: str,
        asset_class: AssetClass,
        cache: Dict[str, Security]
    ) -> Security:
        """Get existing security or create new one"""
        if symbol in cache:
            return cache[symbol]

        security = self.db.query(Security).filter(
            Security.symbol == symbol
        ).first()

        if not security:
            security = Security(
                symbol=symbol,
                asset_name=asset_name or symbol,
                asset_class=asset_class
            )
            self.db.add(security)
            self.db.flush()

        cache[symbol] = security
        return security
