"""
Tests for the inception positions pipeline.

Verifies that:
1. Inception prices are seeded into PricesEOD
2. Portfolio values are correctly computed from inception positions
3. Returns chain starts correctly from inception date
"""
import pytest
import pandas as pd
from datetime import date
from sqlalchemy import create_engine, event
from sqlalchemy.orm import sessionmaker
from app.core.database import Base
from app.models import (
    Account, Security, AssetClass, AccountInception, InceptionPosition,
    PositionsEOD, PricesEOD, PortfolioValueEOD, ReturnsEOD, ViewType,
    ImportLog
)
from app.services.inception_parser import InceptionParser


@pytest.fixture
def test_db():
    """Create a test database with SQLite"""
    engine = create_engine("sqlite:///:memory:")

    # SQLite doesn't enforce foreign keys by default
    @event.listens_for(engine, "connect")
    def set_sqlite_pragma(dbapi_conn, connection_record):
        cursor = dbapi_conn.cursor()
        cursor.execute("PRAGMA foreign_keys=OFF")
        cursor.close()

    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()


@pytest.fixture
def setup_inception_data(test_db):
    """Set up test data simulating an inception import"""
    # Create account
    account = Account(account_number="TEST001", display_name="Test Account")
    test_db.add(account)
    test_db.flush()

    # Create securities
    sec_aapl = Security(symbol="AAPL", asset_name="Apple Inc", asset_class=AssetClass.EQUITY)
    sec_msft = Security(symbol="MSFT", asset_name="Microsoft Corp", asset_class=AssetClass.EQUITY)
    test_db.add_all([sec_aapl, sec_msft])
    test_db.flush()

    # Create import log
    import_log = ImportLog(
        file_name='test_inception.csv',
        source='inception',
        status='completed',
        rows_processed=2,
        rows_imported=2,
        rows_error=0
    )
    test_db.add(import_log)
    test_db.flush()

    # Create inception record
    inception_date = date(2020, 12, 31)
    inception = AccountInception(
        account_id=account.id,
        inception_date=inception_date,
        total_value=28550.00,
        import_log_id=import_log.id
    )
    test_db.add(inception)
    test_db.flush()

    # Create inception positions with known prices
    pos_aapl = InceptionPosition(
        inception_id=inception.id,
        security_id=sec_aapl.id,
        shares=100.0,
        price=130.00,
        market_value=13000.00
    )
    pos_msft = InceptionPosition(
        inception_id=inception.id,
        security_id=sec_msft.id,
        shares=50.0,
        price=311.00,
        market_value=15550.00
    )
    test_db.add_all([pos_aapl, pos_msft])
    test_db.commit()

    return {
        'account': account,
        'securities': [sec_aapl, sec_msft],
        'inception': inception,
        'inception_date': inception_date
    }


def test_create_inception_positions_eod_seeds_prices(test_db, setup_inception_data):
    """
    Test that create_inception_positions_eod seeds both PositionsEOD AND PricesEOD.
    This is the critical fix: without PricesEOD on inception date, portfolio value = $0.
    """
    data = setup_inception_data
    account = data['account']
    inception_date = data['inception_date']
    sec_aapl, sec_msft = data['securities']

    parser = InceptionParser(test_db)
    result = parser.create_inception_positions_eod(account.id)

    # Should create positions
    assert result['positions_created'] == 2

    # Should create prices
    assert result['prices_created'] == 2

    # Verify PositionsEOD records
    positions = test_db.query(PositionsEOD).filter(
        PositionsEOD.account_id == account.id,
        PositionsEOD.date == inception_date
    ).all()
    assert len(positions) == 2

    aapl_pos = next(p for p in positions if p.security_id == sec_aapl.id)
    assert aapl_pos.shares == 100.0

    msft_pos = next(p for p in positions if p.security_id == sec_msft.id)
    assert msft_pos.shares == 50.0

    # Verify PricesEOD records - THIS IS THE KEY FIX
    prices = test_db.query(PricesEOD).filter(
        PricesEOD.date == inception_date
    ).all()
    assert len(prices) == 2

    aapl_price = next(p for p in prices if p.security_id == sec_aapl.id)
    assert aapl_price.close == 130.00
    assert aapl_price.source == 'inception'

    msft_price = next(p for p in prices if p.security_id == sec_msft.id)
    assert msft_price.close == 311.00
    assert msft_price.source == 'inception'


def test_inception_prices_not_overwritten(test_db, setup_inception_data):
    """
    Test that if PricesEOD already has prices on inception date,
    inception price seeding doesn't overwrite them.
    """
    data = setup_inception_data
    account = data['account']
    inception_date = data['inception_date']
    sec_aapl = data['securities'][0]

    # Pre-insert a market price for AAPL on inception date
    existing_price = PricesEOD(
        security_id=sec_aapl.id,
        date=inception_date,
        close=132.50,  # Different from inception price of 130.00
        source='tiingo'
    )
    test_db.add(existing_price)
    test_db.commit()

    parser = InceptionParser(test_db)
    result = parser.create_inception_positions_eod(account.id)

    # Should only create 1 price (MSFT, not AAPL since it already exists)
    assert result['prices_created'] == 1

    # Verify AAPL price was NOT overwritten
    aapl_price = test_db.query(PricesEOD).filter(
        PricesEOD.security_id == sec_aapl.id,
        PricesEOD.date == inception_date
    ).first()
    assert aapl_price.close == 132.50  # Original market price preserved
    assert aapl_price.source == 'tiingo'


def test_portfolio_value_nonzero_on_inception_date(test_db, setup_inception_data):
    """
    Test that portfolio value on inception date is non-zero when inception prices
    are seeded into PricesEOD. This verifies the fix for the core bug where
    portfolio value was $0 because PricesEOD had no entries for inception date.
    """
    data = setup_inception_data
    account = data['account']
    inception_date = data['inception_date']

    # Seed positions and prices
    parser = InceptionParser(test_db)
    parser.create_inception_positions_eod(account.id)

    # Now compute portfolio values using ReturnsEngine
    from app.services.returns import ReturnsEngine
    engine = ReturnsEngine(test_db)
    engine.compute_portfolio_values_for_account(account.id, start_date=inception_date)

    # Portfolio value should be non-zero
    value = test_db.query(PortfolioValueEOD).filter(
        PortfolioValueEOD.view_type == ViewType.ACCOUNT,
        PortfolioValueEOD.view_id == account.id,
        PortfolioValueEOD.date == inception_date
    ).first()

    assert value is not None, "PortfolioValueEOD should exist on inception date"
    # Expected: 100 * 130 + 50 * 311 = 13000 + 15550 = 28550
    assert value.total_value == pytest.approx(28550.00, rel=0.01), \
        f"Portfolio value should be $28,550 but was ${value.total_value}"
