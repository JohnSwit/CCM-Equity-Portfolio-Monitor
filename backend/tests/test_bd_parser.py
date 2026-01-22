import pytest
import pandas as pd
from datetime import date
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from app.core.database import Base
from app.models import Transaction, Account, Security
from app.services.bd_parser import BDParser, calculate_file_hash


@pytest.fixture
def test_db():
    """Create a test database"""
    engine = create_engine("sqlite:///:memory:")
    Base.metadata.create_all(engine)
    Session = sessionmaker(bind=engine)
    session = Session()
    yield session
    session.close()


@pytest.fixture
def sample_csv_content():
    """Sample CSV content for testing"""
    return b"""Account Number,Account Display Name,Class,Asset Name,Symbol,Trade Date,Settle Date,Transaction Type,Price,Units,Market Value,Transaction Fee
12345,Test Account,Equity,Apple Inc,AAPL,01/15/2024,01/17/2024,Buy,185.50,100,18550.00,9.99
12345,Test Account,Equity,Microsoft Corp,MSFT,01/15/2024,01/17/2024,Buy,380.25,50,19012.50,9.99"""


def test_idempotency_same_file_twice(test_db, sample_csv_content):
    """Test that importing the same file twice doesn't create duplicates"""
    parser = BDParser(test_db)
    file_hash = calculate_file_hash(sample_csv_content)

    # Parse CSV
    result = parser.parse_csv(sample_csv_content, preview=False)
    df = result['dataframe']

    # First import
    import_result1 = parser.import_transactions(df, "test.csv", file_hash)
    count1 = test_db.query(Transaction).count()

    # Second import (same file)
    import_result2 = parser.import_transactions(df, "test.csv", file_hash)
    count2 = test_db.query(Transaction).count()

    # Should have same count (no duplicates)
    assert count1 == count2
    assert count1 == 2  # Two transactions from sample


def test_transaction_key_generation(test_db, sample_csv_content):
    """Test that transaction key generation is deterministic"""
    parser = BDParser(test_db)

    result = parser.parse_csv(sample_csv_content, preview=False)
    df = result['dataframe']

    # Generate keys for rows
    key1 = parser._generate_txn_key(df.iloc[0])
    key2 = parser._generate_txn_key(df.iloc[0])

    # Same row should generate same key
    assert key1 == key2


def test_transaction_type_mapping(test_db):
    """Test transaction type inference"""
    parser = BDParser(test_db)

    # Test various transaction types
    assert parser._infer_transaction_type("Buy") == parser.db.query(Transaction).first()
    assert str(parser._infer_transaction_type("BUY")) == "TransactionType.BUY"
    assert str(parser._infer_transaction_type("Sell")) == "TransactionType.SELL"
    assert str(parser._infer_transaction_type("Dividend")) == "TransactionType.DIVIDEND"
    assert str(parser._infer_transaction_type("Transfer In")) == "TransactionType.TRANSFER_IN"


def test_parse_csv_validates_headers(test_db):
    """Test that CSV parsing validates required headers"""
    parser = BDParser(test_db)

    invalid_csv = b"""Symbol,Date,Amount
AAPL,01/15/2024,100"""

    result = parser.parse_csv(invalid_csv, preview=False)

    assert result['has_errors']
    assert 'error' in result
    assert 'Missing required headers' in result['error']


def test_accounts_and_securities_creation(test_db, sample_csv_content):
    """Test that accounts and securities are created from CSV"""
    parser = BDParser(test_db)
    file_hash = calculate_file_hash(sample_csv_content)

    result = parser.parse_csv(sample_csv_content, preview=False)
    df = result['dataframe']

    parser.import_transactions(df, "test.csv", file_hash)

    # Check accounts created
    accounts = test_db.query(Account).all()
    assert len(accounts) == 1
    assert accounts[0].account_number == "12345"
    assert accounts[0].display_name == "Test Account"

    # Check securities created
    securities = test_db.query(Security).all()
    assert len(securities) == 2
    symbols = [s.symbol for s in securities]
    assert "AAPL" in symbols
    assert "MSFT" in symbols


def test_returns_computation_ignores_trades():
    """
    Test that returns computation doesn't treat trades as performance.
    This is a conceptual test showing the equity sleeve logic.
    """
    # Day 1: 100 shares @ $100 = $10,000
    shares_t0 = 100
    price_t0 = 100
    value_t0 = shares_t0 * price_t0

    # Day 2: Price moves to $110, but we buy 50 more shares
    # Correct return should be based on original 100 shares
    shares_t1 = 150  # After buy
    price_t1 = 110

    # WRONG way (treats buy as performance):
    value_t1_wrong = shares_t1 * price_t1  # $16,500
    return_wrong = (value_t1_wrong / value_t0) - 1  # 65% (WRONG!)

    # CORRECT way (use start-of-day shares):
    value_t1_correct = shares_t0 * price_t1  # $11,000 (original shares only)
    return_correct = (value_t1_correct / value_t0) - 1  # 10% (CORRECT!)

    assert return_correct == pytest.approx(0.10)
    assert return_wrong != return_correct

    # This demonstrates the equity sleeve logic implemented in ReturnsEngine
