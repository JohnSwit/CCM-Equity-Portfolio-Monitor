from fastapi import APIRouter, Depends, UploadFile, File, Query, HTTPException
from sqlalchemy.orm import Session
from typing import List
from app.core.database import get_db
from app.api.auth import get_current_user
from app.models import User, ImportLog, Transaction, AccountInception, Account, Security
from app.models.schemas import ImportPreviewResponse, ImportCommitResponse
from app.models.sector_models import SectorClassification
from app.services.bd_parser import BDParser, calculate_file_hash
from app.services.inception_parser import InceptionParser, get_accounts_with_inception

router = APIRouter(prefix="/imports", tags=["imports"])


@router.post("/blackdiamond/transactions")
async def import_bd_transactions(
    file: UploadFile = File(...),
    mode: str = Query("preview", regex="^(preview|commit)$"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Import Black Diamond transactions CSV.
    - mode=preview: Parse and show preview with errors
    - mode=commit: Actually import the transactions
    """
    # Read file content
    file_content = await file.read()
    file_hash = calculate_file_hash(file_content)

    # Check if already imported
    if mode == "commit":
        existing = db.query(ImportLog).filter(
            ImportLog.file_hash == file_hash,
            ImportLog.status.in_(['completed', 'completed_with_errors'])
        ).first()

        if existing:
            raise HTTPException(
                status_code=400,
                detail=f"File already imported (import_log_id: {existing.id})"
            )

    # Parse CSV
    parser = BDParser(db)

    if mode == "preview":
        result = parser.parse_csv(file_content, preview=True)

        if result.get('error'):
            raise HTTPException(status_code=400, detail=result['error'])

        return ImportPreviewResponse(**result)

    else:  # commit
        result = parser.parse_csv(file_content, preview=False)

        if result.get('error'):
            raise HTTPException(status_code=400, detail=result['error'])

        df = result['dataframe']

        # Import transactions
        import_result = parser.import_transactions(df, file.filename, file_hash)

        return ImportCommitResponse(**import_result)


@router.get("/")
def get_import_history(
    limit: int = Query(50, ge=1, le=200),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get import history"""
    imports = db.query(ImportLog).order_by(
        ImportLog.created_at.desc()
    ).limit(limit).all()

    return [
        {
            'id': imp.id,
            'source': imp.source,
            'file_name': imp.file_name,
            'status': imp.status,
            'rows_processed': imp.rows_processed,
            'rows_imported': imp.rows_imported,
            'rows_error': imp.rows_error,
            'created_at': imp.created_at
        }
        for imp in imports
    ]


@router.delete("/{import_id}")
async def delete_import(
    import_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Delete an import and all its transactions.
    Clears analytics for affected accounts but does NOT recompute.
    Use the analytics endpoints to rebuild if needed.
    WARNING: This will delete all transactions from this import.
    """
    # Find the import
    import_log = db.query(ImportLog).filter(ImportLog.id == import_id).first()
    if not import_log:
        raise HTTPException(status_code=404, detail="Import not found")

    # Get affected account IDs before deletion
    affected_account_ids = set([
        t.account_id for t in db.query(Transaction.account_id).filter(
            Transaction.import_log_id == import_id
        ).distinct().all()
    ])

    # Count transactions to delete
    txn_count = db.query(Transaction).filter(
        Transaction.import_log_id == import_id
    ).count()

    file_name = import_log.file_name

    # Delete all transactions from this import
    db.query(Transaction).filter(
        Transaction.import_log_id == import_id
    ).delete()

    # Delete the import log
    db.delete(import_log)
    db.commit()

    # Clear analytics for all affected accounts (each call commits automatically)
    from app.workers.jobs import clear_analytics_for_account
    for account_id in affected_account_ids:
        clear_analytics_for_account(db, account_id)

    return {
        'deleted': True,
        'import_id': import_id,
        'transactions_deleted': txn_count,
        'accounts_affected': len(affected_account_ids),
        'message': f'Deleted import {file_name} and {txn_count} transactions. Analytics cleared for {len(affected_account_ids)} accounts.'
    }


# ==================== HISTORICAL INCEPTION ENDPOINTS ====================

@router.post("/inception")
async def import_inception_positions(
    file: UploadFile = File(...),
    mode: str = Query("preview", regex="^(preview|commit)$"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Import historical portfolio inception positions CSV.
    This establishes a starting point for portfolio tracking from a past date.

    Expected CSV columns:
    - Account Number
    - Account Display Name
    - Class (asset class)
    - Asset Name
    - Symbol
    - Units (shares)
    - Price
    - Market Value
    - Inception Date (e.g., 12/31/2020)

    - mode=preview: Parse and show preview with validation
    - mode=commit: Import the inception positions
    """
    file_content = await file.read()
    parser = InceptionParser(db)

    if mode == "preview":
        result = parser.parse_csv(file_content, preview=True)

        if result.get('error'):
            raise HTTPException(status_code=400, detail=result['error'])

        return {
            'mode': 'preview',
            'total_rows': result.get('total_rows', 0),
            'inception_date': result.get('inception_date'),
            'accounts_summary': result.get('accounts_summary', []),
            'preview_rows': result.get('preview_rows', []),
            'has_errors': result.get('has_errors', False),
            'multiple_dates_warning': result.get('multiple_dates_warning', False)
        }

    else:  # commit
        result = parser.parse_csv(file_content, preview=False)

        if result.get('error'):
            raise HTTPException(status_code=400, detail=result['error'])

        df = result['dataframe']

        # Commit inception positions
        commit_result = parser.commit_inception(df, user_id=current_user.id)

        if commit_result.get('error'):
            raise HTTPException(status_code=400, detail=commit_result['error'])

        # Create PositionsEOD records for all affected accounts
        from app.models import AccountInception
        inceptions = db.query(AccountInception).filter(
            AccountInception.import_log_id == commit_result.get('import_log_id')
        ).all()

        for inception in inceptions:
            parser.create_inception_positions_eod(inception.account_id)

        return {
            'mode': 'commit',
            'success': True,
            'inception_date': commit_result.get('inception_date'),
            'accounts_created': commit_result.get('accounts_created', 0),
            'accounts_updated': commit_result.get('accounts_updated', 0),
            'securities_created': commit_result.get('securities_created', 0),
            'positions_created': commit_result.get('positions_created', 0),
            'total_value': commit_result.get('total_value', 0),
            'import_log_id': commit_result.get('import_log_id'),
            'errors': commit_result.get('errors', []),
            'message': f"Successfully imported {commit_result.get('positions_created', 0)} inception positions across {commit_result.get('accounts_created', 0) + commit_result.get('accounts_updated', 0)} accounts"
        }


@router.get("/inception")
def get_inception_data(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Get all accounts with inception data.
    Returns summary of inception positions for each account.
    """
    return {
        'accounts': get_accounts_with_inception(db)
    }


@router.get("/inception/{account_id}")
def get_account_inception(
    account_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get inception details for a specific account"""
    inception = db.query(AccountInception).filter(
        AccountInception.account_id == account_id
    ).first()

    if not inception:
        raise HTTPException(status_code=404, detail="No inception data found for this account")

    return {
        'account_id': account_id,
        'account_number': inception.account.account_number,
        'display_name': inception.account.display_name,
        'inception_date': inception.inception_date.isoformat(),
        'total_value': inception.total_value,
        'positions': [
            {
                'security_id': pos.security_id,
                'symbol': pos.security.symbol,
                'asset_name': pos.security.asset_name,
                'shares': pos.shares,
                'price': pos.price,
                'market_value': pos.market_value
            }
            for pos in inception.positions
        ]
    }


@router.delete("/inception/{account_id}")
async def delete_account_inception(
    account_id: int,
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Delete inception data for a specific account.
    This will reset the account to only use transaction-based position tracking.
    """
    inception = db.query(AccountInception).filter(
        AccountInception.account_id == account_id
    ).first()

    if not inception:
        raise HTTPException(status_code=404, detail="No inception data found for this account")

    account_number = inception.account.account_number
    inception_date = inception.inception_date
    position_count = len(inception.positions)

    # Delete the inception (cascade deletes positions)
    db.delete(inception)
    db.commit()

    # Clear analytics for the account
    from app.workers.jobs import clear_analytics_for_account
    clear_analytics_for_account(db, account_id)

    return {
        'deleted': True,
        'account_id': account_id,
        'account_number': account_number,
        'inception_date': inception_date.isoformat(),
        'positions_deleted': position_count,
        'message': f'Deleted inception data for account {account_number}. Analytics cleared.'
    }


# ==================== CLASSIFICATION IMPORT ENDPOINTS ====================

@router.post("/classifications")
async def import_classifications(
    file: UploadFile = File(...),
    mode: str = Query("preview", regex="^(preview|commit)$"),
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """
    Import ticker classifications from CSV.

    Expected CSV columns (case-insensitive, flexible naming):
    - Symbol / Ticker (required)
    - Sector (required)
    - Industry (optional)
    - Country (optional)
    - GICS Sector (optional - if different from simplified sector)
    - GICS Industry (optional)
    - Market Cap (optional - Large/Mid/Small)

    Uploaded classifications are stored with source='upload' and take
    priority over API-fetched classifications during refresh.

    - mode=preview: Parse CSV and show match summary
    - mode=commit: Save classifications to database
    """
    import csv
    import io
    from datetime import date, datetime

    file_content = await file.read()

    try:
        text = file_content.decode('utf-8-sig')  # Handle BOM
    except UnicodeDecodeError:
        text = file_content.decode('latin-1')

    reader = csv.DictReader(io.StringIO(text))
    if not reader.fieldnames:
        raise HTTPException(status_code=400, detail="CSV file is empty or has no headers")

    # Normalize headers to lowercase for flexible matching
    raw_headers = [h.strip() for h in reader.fieldnames]
    header_map = {h.lower().replace(' ', '_'): h for h in raw_headers}

    # Find required columns
    symbol_col = None
    for key in ['symbol', 'ticker', 'symbols', 'tickers']:
        if key in header_map:
            symbol_col = header_map[key]
            break
    if not symbol_col:
        raise HTTPException(
            status_code=400,
            detail=f"CSV must have a 'Symbol' or 'Ticker' column. Found: {raw_headers}"
        )

    sector_col = None
    for key in ['sector', 'sectors']:
        if key in header_map:
            sector_col = header_map[key]
            break
    if not sector_col:
        raise HTTPException(
            status_code=400,
            detail=f"CSV must have a 'Sector' column. Found: {raw_headers}"
        )

    # Find optional columns
    def find_col(*candidates):
        for c in candidates:
            if c in header_map:
                return header_map[c]
        return None

    industry_col = find_col('industry', 'gics_industry', 'industries')
    country_col = find_col('country', 'countries', 'domicile')
    gics_sector_col = find_col('gics_sector')
    gics_industry_col = find_col('gics_industry') if not industry_col else None
    market_cap_col = find_col('market_cap', 'market_cap_category', 'cap', 'size')

    # Parse rows
    rows = []
    errors = []
    for i, row in enumerate(reader, start=2):  # start=2 because row 1 is header
        symbol = (row.get(symbol_col) or '').strip().upper()
        sector = (row.get(sector_col) or '').strip()

        if not symbol:
            errors.append(f"Row {i}: Missing symbol")
            continue
        if not sector:
            errors.append(f"Row {i}: Missing sector for {symbol}")
            continue

        parsed = {
            'symbol': symbol,
            'sector': sector,
            'industry': (row.get(industry_col) or '').strip() if industry_col else None,
            'country': (row.get(country_col) or '').strip() if country_col else None,
            'gics_sector': (row.get(gics_sector_col) or '').strip() if gics_sector_col else None,
            'gics_industry': (row.get(gics_industry_col) or '').strip() if gics_industry_col else None,
            'market_cap': (row.get(market_cap_col) or '').strip() if market_cap_col else None,
        }
        rows.append(parsed)

    if not rows and not errors:
        raise HTTPException(status_code=400, detail="CSV contains no data rows")

    # Look up which symbols exist in the securities table
    all_symbols = list({r['symbol'] for r in rows})
    existing_securities = db.query(Security).filter(
        Security.symbol.in_(all_symbols)
    ).all()
    security_map = {s.symbol.upper(): s for s in existing_securities}

    matched = [r for r in rows if r['symbol'] in security_map]
    unmatched = [r for r in rows if r['symbol'] not in security_map]

    # Check which matched securities already have classifications
    matched_ids = [security_map[r['symbol']].id for r in matched]
    existing_classifications = {
        c.security_id: c.source
        for c in db.query(SectorClassification).filter(
            SectorClassification.security_id.in_(matched_ids)
        ).all()
    } if matched_ids else {}

    will_create = sum(
        1 for r in matched
        if security_map[r['symbol']].id not in existing_classifications
    )
    will_update = sum(
        1 for r in matched
        if security_map[r['symbol']].id in existing_classifications
    )

    if mode == "preview":
        # Build preview showing what will happen
        preview_rows = []
        for r in rows[:50]:  # Show first 50
            sec = security_map.get(r['symbol'])
            status = "new" if sec and sec.id not in existing_classifications else \
                     "update" if sec else "unmatched"
            existing_source = existing_classifications.get(sec.id) if sec else None
            preview_rows.append({
                'symbol': r['symbol'],
                'sector': r['sector'],
                'industry': r['industry'],
                'country': r['country'],
                'status': status,
                'existing_source': existing_source,
            })

        return {
            'mode': 'preview',
            'total_rows': len(rows),
            'matched': len(matched),
            'unmatched': len(unmatched),
            'will_create': will_create,
            'will_update': will_update,
            'unmatched_symbols': [r['symbol'] for r in unmatched[:20]],
            'preview_rows': preview_rows,
            'errors': errors[:20],
            'columns_detected': {
                'symbol': symbol_col,
                'sector': sector_col,
                'industry': industry_col,
                'country': country_col,
                'gics_sector': gics_sector_col,
                'market_cap': market_cap_col,
            },
        }

    else:  # commit
        created = 0
        updated = 0

        for r in matched:
            sec = security_map[r['symbol']]
            existing = db.query(SectorClassification).filter(
                SectorClassification.security_id == sec.id
            ).first()

            classification_data = {
                'sector': r['sector'],
                'gics_sector': r['gics_sector'] or r['sector'],
                'gics_industry': r['gics_industry'] or r['industry'],
                'market_cap_category': r['market_cap'],
            }

            if existing:
                existing.sector = classification_data['sector']
                existing.gics_sector = classification_data['gics_sector']
                existing.gics_industry = classification_data['gics_industry']
                if classification_data['market_cap_category']:
                    existing.market_cap_category = classification_data['market_cap_category']
                if r.get('country'):
                    existing.country = r['country']
                existing.source = 'upload'
                existing.as_of_date = date.today()
                existing.updated_at = datetime.utcnow()
                updated += 1
            else:
                new_class = SectorClassification(
                    security_id=sec.id,
                    sector=classification_data['sector'],
                    gics_sector=classification_data['gics_sector'],
                    gics_industry=classification_data['gics_industry'],
                    market_cap_category=classification_data['market_cap_category'],
                    country=r.get('country'),
                    source='upload',
                    as_of_date=date.today(),
                )
                db.add(new_class)
                created += 1

        db.commit()

        return {
            'mode': 'commit',
            'success': True,
            'total_rows': len(rows),
            'matched': len(matched),
            'unmatched': len(unmatched),
            'created': created,
            'updated': updated,
            'unmatched_symbols': [r['symbol'] for r in unmatched[:20]],
            'errors': errors[:20],
            'message': f"Imported {created + updated} classifications ({created} new, {updated} updated). {len(unmatched)} symbols not found in portfolio."
        }


@router.get("/classifications")
def get_classification_summary(
    db: Session = Depends(get_db),
    current_user: User = Depends(get_current_user)
):
    """Get summary of current classifications by source.

    Only shows unclassified securities that are currently held in the portfolio
    (have positions), not all securities in the database.
    """
    from sqlalchemy import func
    from app.models import PositionsEOD

    total = db.query(func.count(Security.id)).scalar() or 0
    classified = db.query(func.count(SectorClassification.id)).scalar() or 0

    # Count by source
    source_counts = dict(
        db.query(SectorClassification.source, func.count(SectorClassification.id))
        .group_by(SectorClassification.source)
        .all()
    )

    # Get securities currently held in the portfolio (latest position date, shares > 0)
    latest_date = db.query(func.max(PositionsEOD.date)).scalar()
    if latest_date:
        held_security_ids = db.query(PositionsEOD.security_id).filter(
            PositionsEOD.date == latest_date,
            PositionsEOD.shares > 0,
        ).distinct().subquery()
    else:
        held_security_ids = db.query(Security.id).filter(False).subquery()

    # Only show unclassified securities that are currently held
    classified_ids = db.query(SectorClassification.security_id).subquery()
    unclassified = db.query(Security.symbol).filter(
        Security.id.in_(held_security_ids),
        ~Security.id.in_(classified_ids),
        Security.is_option == False,
    ).order_by(Security.symbol).all()

    unclassified_count = len(unclassified)

    # Count held securities for coverage
    held_total = db.query(func.count(Security.id)).filter(
        Security.id.in_(held_security_ids),
        Security.is_option == False,
    ).scalar() or 0

    held_classified = db.query(func.count(SectorClassification.id)).filter(
        SectorClassification.security_id.in_(held_security_ids),
    ).scalar() or 0

    return {
        'total_securities': total,
        'classified': classified,
        'held_securities': held_total,
        'held_classified': held_classified,
        'unclassified': unclassified_count,
        'coverage_percent': round(held_classified / held_total * 100, 1) if held_total > 0 else 0,
        'by_source': source_counts,
        'unclassified_symbols': [s[0] for s in unclassified[:50]],
    }
