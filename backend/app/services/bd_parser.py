import pandas as pd
import hashlib
import io
from typing import List, Dict, Any, Tuple, Optional
from datetime import datetime
from sqlalchemy.orm import Session
from app.models import (
    Account, Security, Transaction, TransactionTypeMap,
    ImportLog, TransactionType, AssetClass
)


class BDParser:
    """Black Diamond CSV Parser with idempotency and preview support"""

    REQUIRED_HEADERS = [
        'Account Number', 'Account Display Name', 'Class', 'Asset Name',
        'Symbol', 'Trade Date', 'Settle Date', 'Transaction Type',
        'Price', 'Units', 'Market Value', 'Transaction Fee'
    ]

    def __init__(self, db: Session):
        self.db = db

    def parse_csv(self, file_content: bytes, preview: bool = False, max_preview_rows: int = 10) -> Dict[str, Any]:
        """Parse CSV and return preview or full parse results"""
        try:
            # Try both tab and comma delimiters
            content_str = file_content.decode('utf-8')
            if '\t' in content_str:
                df = pd.read_csv(io.StringIO(content_str), sep='\t')
            else:
                df = pd.read_csv(io.StringIO(content_str))

            # Validate headers
            missing_headers = set(self.REQUIRED_HEADERS) - set(df.columns)
            if missing_headers:
                return {
                    'error': f'Missing required headers: {missing_headers}',
                    'has_errors': True
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

    def _clean_dataframe(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and normalize dataframe"""
        # Trim whitespace
        for col in df.columns:
            if df[col].dtype == 'object':
                df[col] = df[col].str.strip()

        # Normalize Symbol to uppercase
        df['Symbol'] = df['Symbol'].str.upper()

        # Parse dates
        df['Trade Date'] = pd.to_datetime(df['Trade Date'], errors='coerce')
        df['Settle Date'] = pd.to_datetime(df['Settle Date'], errors='coerce')

        # Parse numeric fields
        for col in ['Price', 'Units', 'Market Value', 'Transaction Fee']:
            df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0.0)

        return df

    def _generate_preview(self, df: pd.DataFrame, max_rows: int) -> Dict[str, Any]:
        """Generate preview with detected mappings and errors"""
        preview_rows = []
        detected_mappings = {}

        for idx, row in df.head(max_rows).iterrows():
            row_data = row.to_dict()
            errors = []

            # Check for errors
            if pd.isna(row['Trade Date']):
                errors.append('Invalid Trade Date')
            if not row['Symbol']:
                errors.append('Missing Symbol')
            if not row['Account Number']:
                errors.append('Missing Account Number')

            # Detect transaction type mapping
            raw_txn_type = row['Transaction Type']
            if raw_txn_type and raw_txn_type not in detected_mappings:
                normalized = self._infer_transaction_type(raw_txn_type)
                detected_mappings[raw_txn_type] = normalized.value

            preview_rows.append({
                'row_num': int(idx) + 2,  # +2 for header and 0-indexing
                'data': {k: str(v) if pd.notna(v) else '' for k, v in row_data.items()},
                'errors': errors
            })

        return {
            'total_rows': len(df),
            'preview_rows': preview_rows,
            'detected_mappings': detected_mappings,
            'has_errors': any(row['errors'] for row in preview_rows)
        }

    def _infer_transaction_type(self, raw_type: str) -> TransactionType:
        """Infer normalized transaction type from raw string"""
        raw_lower = raw_type.lower()

        # Check database mapping first
        mapping = self.db.query(TransactionTypeMap).filter(
            TransactionTypeMap.source == 'blackdiamond',
            TransactionTypeMap.raw_type == raw_type
        ).first()

        if mapping:
            return mapping.normalized_type

        # Heuristic fallback
        if 'buy' in raw_lower or 'purchase' in raw_lower:
            return TransactionType.BUY
        elif 'sell' in raw_lower or 'sale' in raw_lower:
            return TransactionType.SELL
        elif 'dividend' in raw_lower or 'div' in raw_lower:
            return TransactionType.DIVIDEND
        elif 'transfer in' in raw_lower or 'deposit' in raw_lower:
            return TransactionType.TRANSFER_IN
        elif 'transfer out' in raw_lower or 'withdrawal' in raw_lower:
            return TransactionType.TRANSFER_OUT
        elif 'fee' in raw_lower or 'commission' in raw_lower:
            return TransactionType.FEE
        else:
            return TransactionType.OTHER

    def _classify_asset_class(self, class_str: str, symbol: str) -> Tuple[AssetClass, bool]:
        """Classify asset class and determine if it's an option"""
        class_lower = class_str.lower() if class_str else ''
        is_option = False

        if 'option' in class_lower or 'opt' in class_lower:
            asset_class = AssetClass.OPTION
            is_option = True
        elif 'etf' in class_lower:
            asset_class = AssetClass.ETF
        elif 'cash' in class_lower or 'money market' in class_lower:
            asset_class = AssetClass.CASH
        elif 'equity' in class_lower or 'stock' in class_lower:
            asset_class = AssetClass.EQUITY
        else:
            asset_class = AssetClass.OTHER

        return asset_class, is_option

    def _generate_txn_key(self, row: pd.Series) -> str:
        """Generate idempotency key for transaction"""
        key_parts = [
            str(row['Account Number']),
            str(row['Symbol']),
            str(row['Trade Date']),
            str(row['Transaction Type']),
            str(row['Units']),
            str(row['Price'])
        ]
        key_string = '|'.join(key_parts)
        return hashlib.sha256(key_string.encode()).hexdigest()

    def import_transactions(self, df: pd.DataFrame, file_name: str, file_hash: str) -> Dict[str, Any]:
        """Import transactions with idempotency"""
        # Create import log
        import_log = ImportLog(
            source='blackdiamond',
            file_name=file_name,
            file_hash=file_hash,
            status='processing',
            rows_processed=0,
            rows_imported=0,
            rows_error=0,
            errors=[]
        )
        self.db.add(import_log)
        self.db.commit()
        self.db.refresh(import_log)

        rows_imported = 0
        rows_error = 0
        errors = []

        try:
            for idx, row in df.iterrows():
                try:
                    # Generate idempotency key
                    txn_key = self._generate_txn_key(row)

                    # Check if already exists
                    existing = self.db.query(Transaction).filter(
                        Transaction.source_txn_key == txn_key
                    ).first()

                    if existing:
                        continue  # Skip duplicate

                    # Get or create account
                    account = self._get_or_create_account(row)

                    # Get or create security
                    security = self._get_or_create_security(row)

                    # Infer transaction type
                    txn_type = self._infer_transaction_type(row['Transaction Type'])

                    # Create transaction
                    transaction = Transaction(
                        account_id=account.id,
                        security_id=security.id if security else None,
                        trade_date=row['Trade Date'].date() if pd.notna(row['Trade Date']) else None,
                        settle_date=row['Settle Date'].date() if pd.notna(row['Settle Date']) else None,
                        transaction_type=txn_type,
                        raw_transaction_type=row['Transaction Type'],
                        price=float(row['Price']) if pd.notna(row['Price']) else None,
                        units=float(row['Units']) if pd.notna(row['Units']) else None,
                        market_value=float(row['Market Value']) if pd.notna(row['Market Value']) else None,
                        transaction_fee=float(row['Transaction Fee']) if pd.notna(row['Transaction Fee']) else 0.0,
                        source_txn_key=txn_key,
                        import_log_id=import_log.id
                    )
                    self.db.add(transaction)
                    rows_imported += 1

                except Exception as e:
                    rows_error += 1
                    errors.append({
                        'row': int(idx) + 2,
                        'error': str(e)
                    })

            # Update import log
            import_log.rows_processed = len(df)
            import_log.rows_imported = rows_imported
            import_log.rows_error = rows_error
            import_log.errors = errors[:100]  # Limit error list
            import_log.status = 'completed' if rows_error == 0 else 'completed_with_errors'

            self.db.commit()

            return {
                'import_log_id': import_log.id,
                'status': import_log.status,
                'rows_processed': import_log.rows_processed,
                'rows_imported': rows_imported,
                'rows_error': rows_error,
                'errors': errors[:100]
            }

        except Exception as e:
            import_log.status = 'failed'
            import_log.errors = [{'error': str(e)}]
            self.db.commit()
            raise

    def _get_or_create_account(self, row: pd.Series) -> Account:
        """Get or create account"""
        account_number = str(row['Account Number'])
        account = self.db.query(Account).filter(
            Account.account_number == account_number
        ).first()

        if not account:
            account = Account(
                account_number=account_number,
                display_name=row['Account Display Name']
            )
            self.db.add(account)
            self.db.flush()

        return account

    def _get_or_create_security(self, row: pd.Series) -> Optional[Security]:
        """Get or create security"""
        symbol = str(row['Symbol']).upper()
        if not symbol or symbol == 'NAN':
            return None

        asset_class, is_option = self._classify_asset_class(row['Class'], symbol)

        security = self.db.query(Security).filter(
            Security.symbol == symbol,
            Security.asset_class == asset_class
        ).first()

        if not security:
            security = Security(
                symbol=symbol,
                asset_name=row['Asset Name'],
                asset_class=asset_class,
                is_option=is_option
            )
            self.db.add(security)
            self.db.flush()

        return security


def calculate_file_hash(file_content: bytes) -> str:
    """Calculate SHA256 hash of file content"""
    return hashlib.sha256(file_content).hexdigest()
