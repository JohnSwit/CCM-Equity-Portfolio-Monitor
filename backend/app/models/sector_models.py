from sqlalchemy import Column, Integer, String, Float, ForeignKey, Index
from sqlalchemy.orm import relationship
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
    benchmark_code = Column(String, nullable=False, index=True)  # e.g., "SPY", "SP500"
    symbol = Column(String, nullable=False)
    weight = Column(Float, nullable=False)  # Weight in benchmark (0-1)
    sector = Column(String)

    __table_args__ = (
        Index('idx_benchmark_symbol', 'benchmark_code', 'symbol'),
    )
