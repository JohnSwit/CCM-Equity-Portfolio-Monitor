from sqlalchemy import Column, Integer, String, Float, ForeignKey, Index, DateTime, Date, Text
from sqlalchemy.orm import relationship
from datetime import datetime
from app.core.database import Base


class SectorClassification(Base):
    """
    Sector and industry classifications for securities.
    Maps securities to GICS sectors or custom taxonomy.
    """
    __tablename__ = "sector_classifications"

    id = Column(Integer, primary_key=True, index=True)
    security_id = Column(Integer, ForeignKey("securities.id"), nullable=False, unique=True)

    # GICS Classification
    gics_sector = Column(String)  # e.g., "Information Technology"
    gics_industry_group = Column(String)  # e.g., "Software & Services"
    gics_industry = Column(String)  # e.g., "Software"
    gics_sub_industry = Column(String)  # e.g., "Application Software"

    # Simplified sector (for ease of use)
    sector = Column(String, index=True)  # e.g., "Technology", "Healthcare"

    # Market cap classification
    market_cap_category = Column(String)  # "Large", "Mid", "Small"

    # Custom tags
    custom_sector = Column(String)  # User-defined sector

    # Data sourcing metadata
    source = Column(String)  # e.g., "polygon", "iex", "manual"
    as_of_date = Column(Date, default=datetime.utcnow)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    security = relationship("Security", backref="sector_classification")

    __table_args__ = (
        Index('idx_sector', 'sector'),
        Index('idx_gics_sector', 'gics_sector'),
    )


class BenchmarkConstituent(Base):
    """
    Benchmark constituents with their weights.
    Stores S&P 500, Russell 2000, etc. constituent data.
    """
    __tablename__ = "benchmark_constituents"

    id = Column(Integer, primary_key=True, index=True)
    benchmark_code = Column(String, nullable=False, index=True)  # e.g., "SPY", "QQQ", "INDU"
    symbol = Column(String, nullable=False)
    weight = Column(Float, nullable=False)  # Weight in benchmark (0-1)
    sector = Column(String)

    # Data sourcing metadata
    as_of_date = Column(Date, nullable=False, index=True)
    source_url = Column(Text)  # URL of the source data
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index('idx_benchmark_symbol', 'benchmark_code', 'symbol'),
        Index('idx_benchmark_date', 'benchmark_code', 'as_of_date'),
    )


class FactorReturns(Base):
    """
    Factor returns time series (Fama-French 5 factors + Momentum).
    Source: Kenneth French Data Library.
    """
    __tablename__ = "fama_french_factor_returns"

    id = Column(Integer, primary_key=True, index=True)
    date = Column(Date, nullable=False, index=True)
    factor_name = Column(String, nullable=False, index=True)  # e.g., "Mkt-RF", "SMB", "HML", "RMW", "CMA", "Mom"
    value = Column(Float, nullable=False)  # Decimal form (e.g., 0.01 for 1%)

    # Data sourcing metadata
    source = Column(String, default="kenneth_french")
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

    __table_args__ = (
        Index('idx_factor_date', 'date', 'factor_name'),
    )
