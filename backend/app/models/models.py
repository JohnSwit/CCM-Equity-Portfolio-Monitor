from sqlalchemy import (
    Column, Integer, String, Float, Date, DateTime, Boolean,
    Text, JSON, ForeignKey, Index, UniqueConstraint, Enum as SQLEnum
)
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from datetime import datetime
import enum
from app.core.database import Base


class User(Base):
    __tablename__ = "users"

    id = Column(Integer, primary_key=True, index=True)
    email = Column(String, unique=True, index=True, nullable=False)
    hashed_password = Column(String, nullable=False)
    full_name = Column(String)
    is_active = Column(Boolean, default=True)
    is_admin = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)


class Account(Base):
    __tablename__ = "accounts"

    id = Column(Integer, primary_key=True, index=True)
    account_number = Column(String, unique=True, nullable=False, index=True)
    display_name = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class AssetClass(str, enum.Enum):
    EQUITY = "EQUITY"
    ETF = "ETF"
    OPTION = "OPTION"
    CASH = "CASH"
    OTHER = "OTHER"


class Security(Base):
    __tablename__ = "securities"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String, nullable=False, index=True)
    asset_name = Column(String)
    asset_class = Column(SQLEnum(AssetClass), default=AssetClass.EQUITY)
    is_option = Column(Boolean, default=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint('symbol', 'asset_class', name='uq_security_symbol_class'),
    )


class TransactionType(str, enum.Enum):
    BUY = "BUY"
    SELL = "SELL"
    DIVIDEND = "DIVIDEND"
    TRANSFER_IN = "TRANSFER_IN"
    TRANSFER_OUT = "TRANSFER_OUT"
    FEE = "FEE"
    OTHER = "OTHER"


class Transaction(Base):
    __tablename__ = "transactions"

    id = Column(Integer, primary_key=True, index=True)
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=False, index=True)
    security_id = Column(Integer, ForeignKey("securities.id"), nullable=True, index=True)
    trade_date = Column(Date, nullable=False, index=True)
    settle_date = Column(Date)
    transaction_type = Column(SQLEnum(TransactionType), nullable=False)
    raw_transaction_type = Column(String)
    price = Column(Float)
    units = Column(Float)
    market_value = Column(Float)
    transaction_fee = Column(Float, default=0.0)
    source_txn_key = Column(String, unique=True, nullable=False, index=True)
    import_log_id = Column(Integer, ForeignKey("import_logs.id"), index=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    account = relationship("Account")
    security = relationship("Security")


class TransactionTypeMap(Base):
    __tablename__ = "transaction_type_map"

    id = Column(Integer, primary_key=True, index=True)
    source = Column(String, default="blackdiamond", nullable=False)
    raw_type = Column(String, nullable=False)
    normalized_type = Column(SQLEnum(TransactionType), nullable=False)
    affects_units = Column(Boolean, default=True)
    affects_value = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint('source', 'raw_type', name='uq_txn_map_source_raw'),
    )


class PricesEOD(Base):
    __tablename__ = "prices_eod"

    id = Column(Integer, primary_key=True, index=True)
    security_id = Column(Integer, ForeignKey("securities.id"), nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)
    close = Column(Float, nullable=False)
    source = Column(String, default="stooq")
    created_at = Column(DateTime, default=datetime.utcnow)

    security = relationship("Security")

    __table_args__ = (
        UniqueConstraint('security_id', 'date', name='uq_price_security_date'),
        Index('idx_prices_date', 'date'),
    )


class PositionsEOD(Base):
    __tablename__ = "positions_eod"

    id = Column(Integer, primary_key=True, index=True)
    account_id = Column(Integer, ForeignKey("accounts.id"), nullable=False, index=True)
    security_id = Column(Integer, ForeignKey("securities.id"), nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)
    shares = Column(Float, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    account = relationship("Account")
    security = relationship("Security")

    __table_args__ = (
        UniqueConstraint('account_id', 'security_id', 'date', name='uq_position_account_security_date'),
    )


class ViewType(str, enum.Enum):
    ACCOUNT = "account"
    GROUP = "group"
    FIRM = "firm"


class PortfolioValueEOD(Base):
    __tablename__ = "portfolio_value_eod"

    id = Column(Integer, primary_key=True, index=True)
    view_type = Column(SQLEnum(ViewType), nullable=False, index=True)
    view_id = Column(Integer, nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)
    total_value = Column(Float, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint('view_type', 'view_id', 'date', name='uq_value_view_date'),
    )


class ReturnsEOD(Base):
    __tablename__ = "returns_eod"

    id = Column(Integer, primary_key=True, index=True)
    view_type = Column(SQLEnum(ViewType), nullable=False, index=True)
    view_id = Column(Integer, nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)
    twr_return = Column(Float)
    twr_index = Column(Float)
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint('view_type', 'view_id', 'date', name='uq_return_view_date'),
    )


class GroupType(str, enum.Enum):
    FAMILY = "family"
    ESTATE = "estate"
    CUSTOM = "custom"
    FIRM = "firm"


class Group(Base):
    __tablename__ = "groups"

    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, nullable=False)
    type = Column(SQLEnum(GroupType), nullable=False)
    parent_group_id = Column(Integer, ForeignKey("groups.id"), nullable=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class GroupMember(Base):
    __tablename__ = "group_members"

    id = Column(Integer, primary_key=True, index=True)
    group_id = Column(Integer, ForeignKey("groups.id"), nullable=False, index=True)
    member_type = Column(String, default="account")
    member_id = Column(Integer, nullable=False, index=True)
    created_at = Column(DateTime, default=datetime.utcnow)

    group = relationship("Group")

    __table_args__ = (
        UniqueConstraint('group_id', 'member_type', 'member_id', name='uq_group_member'),
    )


class BenchmarkDefinition(Base):
    __tablename__ = "benchmark_definitions"

    id = Column(Integer, primary_key=True, index=True)
    code = Column(String, unique=True, nullable=False, index=True)
    name = Column(String, nullable=False)
    provider_symbol = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)


class BenchmarkLevel(Base):
    __tablename__ = "benchmark_levels"

    id = Column(Integer, primary_key=True, index=True)
    code = Column(String, nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)
    level = Column(Float, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint('code', 'date', name='uq_benchmark_level_code_date'),
    )


class BenchmarkReturn(Base):
    __tablename__ = "benchmark_returns"

    id = Column(Integer, primary_key=True, index=True)
    code = Column(String, nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)
    return_value = Column(Float, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint('code', 'date', name='uq_benchmark_return_code_date'),
    )


class BenchmarkMetric(Base):
    __tablename__ = "benchmark_metrics"

    id = Column(Integer, primary_key=True, index=True)
    view_type = Column(SQLEnum(ViewType), nullable=False, index=True)
    view_id = Column(Integer, nullable=False, index=True)
    benchmark_code = Column(String, nullable=False, index=True)
    as_of_date = Column(Date, nullable=False, index=True)
    beta_252 = Column(Float)
    alpha_252 = Column(Float)
    te_252 = Column(Float)
    corr_252 = Column(Float)
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint('view_type', 'view_id', 'benchmark_code', 'as_of_date',
                        name='uq_benchmark_metric_view_bench_date'),
    )


class Basket(Base):
    __tablename__ = "baskets"

    id = Column(Integer, primary_key=True, index=True)
    code = Column(String, unique=True, nullable=False, index=True)
    name = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class BasketConstituent(Base):
    __tablename__ = "basket_constituents"

    id = Column(Integer, primary_key=True, index=True)
    basket_id = Column(Integer, ForeignKey("baskets.id"), nullable=False, index=True)
    symbol = Column(String, nullable=False)
    weight = Column(Float, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    basket = relationship("Basket")

    __table_args__ = (
        UniqueConstraint('basket_id', 'symbol', name='uq_basket_constituent'),
    )


class FactorSet(Base):
    __tablename__ = "factor_sets"

    id = Column(Integer, primary_key=True, index=True)
    code = Column(String, unique=True, nullable=False, index=True)
    name = Column(String, nullable=False)
    factor_names = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)


class FactorReturn(Base):
    __tablename__ = "factor_returns"

    id = Column(Integer, primary_key=True, index=True)
    factor_set_code = Column(String, nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)
    factors_json = Column(JSON, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint('factor_set_code', 'date', name='uq_factor_return_set_date'),
    )


class FactorRegression(Base):
    __tablename__ = "factor_regressions"

    id = Column(Integer, primary_key=True, index=True)
    view_type = Column(SQLEnum(ViewType), nullable=False, index=True)
    view_id = Column(Integer, nullable=False, index=True)
    factor_set_code = Column(String, nullable=False, index=True)
    as_of_date = Column(Date, nullable=False, index=True)
    window = Column(Integer, nullable=False)
    betas_json = Column(JSON)
    alpha = Column(Float)
    r_squared = Column(Float)
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint('view_type', 'view_id', 'factor_set_code', 'as_of_date', 'window',
                        name='uq_factor_regression_view_set_date_window'),
    )


class RiskEOD(Base):
    __tablename__ = "risk_eod"

    id = Column(Integer, primary_key=True, index=True)
    view_type = Column(SQLEnum(ViewType), nullable=False, index=True)
    view_id = Column(Integer, nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)
    vol_21d = Column(Float)
    vol_63d = Column(Float)
    max_drawdown_1y = Column(Float)
    var_95_1d_hist = Column(Float)
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint('view_type', 'view_id', 'date', name='uq_risk_view_date'),
    )


class ImportLog(Base):
    __tablename__ = "import_logs"

    id = Column(Integer, primary_key=True, index=True)
    source = Column(String, nullable=False)
    file_name = Column(String)
    file_hash = Column(String, index=True)
    status = Column(String)
    rows_processed = Column(Integer, default=0)
    rows_imported = Column(Integer, default=0)
    rows_error = Column(Integer, default=0)
    errors = Column(JSON)
    created_at = Column(DateTime, default=datetime.utcnow)


class ManualPrice(Base):
    __tablename__ = "manual_prices"

    id = Column(Integer, primary_key=True, index=True)
    security_id = Column(Integer, ForeignKey("securities.id"), nullable=False, index=True)
    date = Column(Date, nullable=False, index=True)
    close = Column(Float, nullable=False)
    created_by = Column(Integer, ForeignKey("users.id"))
    created_at = Column(DateTime, default=datetime.utcnow)

    security = relationship("Security")

    __table_args__ = (
        UniqueConstraint('security_id', 'date', name='uq_manual_price_security_date'),
    )


class FactorDataSource(str, enum.Enum):
    STOOQ = "stooq"
    FRED = "fred"
    YFINANCE = "yfinance"
    ALPHAVANTAGE = "alphavantage"


class FactorProxySeries(Base):
    """Cached market data series for factor proxies (ETFs, rates, etc.)"""
    __tablename__ = "factor_proxy_series"

    id = Column(Integer, primary_key=True, index=True)
    symbol = Column(String, nullable=False, index=True)
    source = Column(SQLEnum(FactorDataSource), nullable=False)
    date = Column(Date, nullable=False, index=True)
    close = Column(Float)  # Price level for ETFs
    value = Column(Float)  # Value for rates/macro series
    daily_return = Column(Float)  # Pre-computed daily return
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint('symbol', 'source', 'date', name='uq_factor_proxy_symbol_source_date'),
        Index('idx_factor_proxy_symbol_date', 'symbol', 'date'),
    )


class FactorModelDefinition(Base):
    """Defines factor models with their proxy ETFs/series"""
    __tablename__ = "factor_model_definitions"

    id = Column(Integer, primary_key=True, index=True)
    code = Column(String, unique=True, nullable=False, index=True)
    name = Column(String, nullable=False)
    description = Column(Text)
    factors_config = Column(JSON, nullable=False)  # Dict of factor_name -> {symbol, source, spread_vs}
    is_active = Column(Boolean, default=True)
    created_at = Column(DateTime, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)


class FactorAttributionResult(Base):
    """Stores factor attribution analysis results"""
    __tablename__ = "factor_attribution_results"

    id = Column(Integer, primary_key=True, index=True)
    view_type = Column(SQLEnum(ViewType), nullable=False, index=True)
    view_id = Column(Integer, nullable=False, index=True)
    factor_model_code = Column(String, nullable=False, index=True)
    start_date = Column(Date, nullable=False)
    end_date = Column(Date, nullable=False)
    # Regression results
    betas_json = Column(JSON)  # factor_name -> beta
    alpha_daily = Column(Float)
    alpha_annualized = Column(Float)
    r_squared = Column(Float)
    adj_r_squared = Column(Float)
    # Diagnostics
    residual_std = Column(Float)
    durbin_watson = Column(Float)
    t_stats_json = Column(JSON)  # factor_name -> t-stat
    p_values_json = Column(JSON)  # factor_name -> p-value
    # Attribution
    total_return = Column(Float)
    factor_contribution_json = Column(JSON)  # factor_name -> contribution %
    alpha_contribution = Column(Float)
    residual_contribution = Column(Float)
    created_at = Column(DateTime, default=datetime.utcnow)

    __table_args__ = (
        UniqueConstraint('view_type', 'view_id', 'factor_model_code', 'start_date', 'end_date',
                        name='uq_factor_attribution_view_model_period'),
    )
