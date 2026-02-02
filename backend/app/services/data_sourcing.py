"""
Data sourcing services for classifications, benchmark constituents, and factor returns.
Updated: 2026-01-29 - Migrated to Tiingo API
"""
import os
import httpx
import pandas as pd
import io
import zipfile
from datetime import datetime, date, timedelta
from typing import Optional, Dict, List, Any
from sqlalchemy.orm import Session
from sqlalchemy import func
from tiingo import TiingoClient

from app.models.sector_models import SectorClassification, BenchmarkConstituent, FactorReturns
from app.models import Security
from app.utils.ticker_utils import TickerNormalizer, SectorMapper
from app.core.config import settings
import logging

logger = logging.getLogger(__name__)


class ClassificationService:
    """
    Service for fetching and storing security classifications from Tiingo (primary)
    with fallback to Polygon.io and IEX Cloud.
    """

    # Class-level static mapping for common tickers (fallback when APIs fail)
    STATIC_MAPPING = {
        "AAPL": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Technology Hardware"},
        "MSFT": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "GOOGL": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Interactive Media"},
        "GOOG": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Interactive Media"},
        "AMZN": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Internet Retail"},
        "TSLA": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Automobiles"},
        "META": {"sector": "Technology", "gics_sector": "Communication Services", "gics_industry": "Interactive Media"},
        "NVDA": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Semiconductors"},
        "JPM": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Banks"},
        "JNJ": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Pharmaceuticals"},
        "V": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Financial Services"},
        "MA": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Financial Services"},
        "UNH": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Providers"},
        "HD": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Home Improvement Retail"},
        "PG": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Household Products"},
        "DIS": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Entertainment"},
        "NFLX": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Entertainment"},
        "ADBE": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "CRM": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "COST": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Consumer Staples Distribution"},
        "PEP": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Beverages"},
        "AVGO": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Semiconductors"},
        "CSCO": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Communications Equipment"},
        "ABT": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Equipment"},
        "TMO": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Life Sciences Tools"},
        "NKE": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Footwear"},
        "LLY": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Pharmaceuticals"},
        "WFC": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Banks"},
        "INTU": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "CVS": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Providers"},
        "AMGN": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Biotechnology"},
        "NOW": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "SBUX": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Restaurants"},
        "DHR": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Equipment"},
        "HON": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Industrial Conglomerates"},
        "LMT": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Aerospace & Defense"},
        "NEE": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Electric Utilities"},
        "UPS": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Air Freight & Logistics"},
        "BLK": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Asset Management"},
        "MDT": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Equipment"},
        "AEP": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Electric Utilities"},
        "DE": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Machinery"},
        "LIN": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Chemicals"},
        "FDX": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Air Freight & Logistics"},
        "SLB": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas Equipment"},
        # Additional US Stocks (from failed classifications)
        "ABNB": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Hotels & Resorts"},
        "AMAT": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Semiconductor Equipment"},
        "ANET": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Communications Equipment"},
        "APD": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Chemicals"},
        "APP": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "ADSK": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "BERY": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Containers & Packaging"},
        "BILL": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "BRK-B": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Diversified Financials"},
        "BRK.B": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Diversified Financials"},
        "BRKB": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Diversified Financials"},
        "BX": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Asset Management"},
        "BURL": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Apparel Retail"},
        "CLX": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Household Products"},
        "CME": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Capital Markets"},
        "CPNG": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Internet Retail"},
        "CPRT": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Specialty Retail"},
        "CRSP": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Biotechnology"},
        "CSX": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Railroads"},
        "DDOG": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "DELL": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Technology Hardware"},
        "DLR": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Data Center REITs"},
        "DRI": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Restaurants"},
        "FAST": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Industrial Distribution"},
        "FCX": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Metals & Mining"},
        "FTRE": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Real Estate Services"},
        "GRAB": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Internet Services"},
        "GRAL": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Biotechnology"},
        "HII": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Aerospace & Defense"},
        "ILMN": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Life Sciences Tools"},
        "KLAC": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Semiconductor Equipment"},
        "KVUE": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Personal Products"},
        "LH": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Services"},
        "MAR": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Hotels & Resorts"},
        "MLM": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Construction Materials"},
        "MRNA": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Biotechnology"},
        "MTCH": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Interactive Media"},
        "NUE": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Steel"},
        "NXT": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "OTIS": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Building Products"},
        "PANW": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "PCAR": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Machinery"},
        "PLD": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Industrial REITs"},
        "POST": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Packaged Foods"},
        "PWR": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Construction & Engineering"},
        "PZZA": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Restaurants"},
        "SHOP": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "SKIN": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Services"},
        "SMPL": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Packaged Foods"},
        "SNOW": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "SNPS": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "SOLS": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "STZ": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Beverages"},
        "SYK": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Equipment"},
        "TDY": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Aerospace & Defense"},
        "UNP": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Railroads"},
        "USB": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Banks"},
        "VTR": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Health Care REITs"},
        "WDAY": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "WM": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Waste Management"},
        "WPC": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Diversified REITs"},
        "ZTS": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Pharmaceuticals"},
        # International ADRs
        "ASML": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Semiconductor Equipment"},
        "NVO": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Pharmaceuticals"},
        "EADSY": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Aerospace & Defense"},
        "LVMUY": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Textiles & Apparel"},
        "LRLCY": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Personal Products"},
        "AMVOY": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Beverages"},
        "CTTAY": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Tobacco"},
        "SBGSY": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Beverages"},
        "VEOEY": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Water Utilities"},
        "VWAGY": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Automobiles"},
        # Additional ETFs
        "ACWI": {"sector": "ETF", "gics_sector": "Broad Market ETF", "gics_industry": "All Country World Index"},
        "EFA": {"sector": "ETF", "gics_sector": "Broad Market ETF", "gics_industry": "International Developed"},
        "EEM": {"sector": "ETF", "gics_sector": "Broad Market ETF", "gics_industry": "Emerging Markets"},
        "AGG": {"sector": "ETF", "gics_sector": "Fixed Income ETF", "gics_industry": "Aggregate Bond"},
        "BND": {"sector": "ETF", "gics_sector": "Fixed Income ETF", "gics_industry": "Total Bond"},
        "LQD": {"sector": "ETF", "gics_sector": "Fixed Income ETF", "gics_industry": "Corporate Bond"},
        "HYG": {"sector": "ETF", "gics_sector": "Fixed Income ETF", "gics_industry": "High Yield Bond"},
        "TLT": {"sector": "ETF", "gics_sector": "Fixed Income ETF", "gics_industry": "Long-Term Treasury"},
        "GLD": {"sector": "ETF", "gics_sector": "Commodity ETF", "gics_industry": "Gold"},
        "SLV": {"sector": "ETF", "gics_sector": "Commodity ETF", "gics_industry": "Silver"},
        # ETFs - Broad Market
        "SPY": {"sector": "ETF", "gics_sector": "Broad Market ETF", "gics_industry": "S&P 500 Index"},
        "QQQ": {"sector": "ETF", "gics_sector": "Broad Market ETF", "gics_industry": "Nasdaq 100 Index"},
        "IWM": {"sector": "ETF", "gics_sector": "Broad Market ETF", "gics_industry": "Russell 2000 Index"},
        "DIA": {"sector": "ETF", "gics_sector": "Broad Market ETF", "gics_industry": "Dow Jones Index"},
        "VTI": {"sector": "ETF", "gics_sector": "Broad Market ETF", "gics_industry": "Total Stock Market"},
        "VOO": {"sector": "ETF", "gics_sector": "Broad Market ETF", "gics_industry": "S&P 500 Index"},
        # ETFs - Style
        "IVE": {"sector": "ETF", "gics_sector": "Style ETF", "gics_industry": "S&P 500 Value"},
        "IVW": {"sector": "ETF", "gics_sector": "Style ETF", "gics_industry": "S&P 500 Growth"},
        "IWD": {"sector": "ETF", "gics_sector": "Style ETF", "gics_industry": "Russell 1000 Value"},
        "IWF": {"sector": "ETF", "gics_sector": "Style ETF", "gics_industry": "Russell 1000 Growth"},
        "IWN": {"sector": "ETF", "gics_sector": "Style ETF", "gics_industry": "Russell 2000 Value"},
        "IWO": {"sector": "ETF", "gics_sector": "Style ETF", "gics_industry": "Russell 2000 Growth"},
        "VTV": {"sector": "ETF", "gics_sector": "Style ETF", "gics_industry": "Value"},
        "VUG": {"sector": "ETF", "gics_sector": "Style ETF", "gics_industry": "Growth"},
        # ETFs - Factor
        "QUAL": {"sector": "ETF", "gics_sector": "Factor ETF", "gics_industry": "Quality Factor"},
        "SPLV": {"sector": "ETF", "gics_sector": "Factor ETF", "gics_industry": "Low Volatility"},
        "MTUM": {"sector": "ETF", "gics_sector": "Factor ETF", "gics_industry": "Momentum Factor"},
        "VLUE": {"sector": "ETF", "gics_sector": "Factor ETF", "gics_industry": "Value Factor"},
        "SIZE": {"sector": "ETF", "gics_sector": "Factor ETF", "gics_industry": "Size Factor"},
        # ETFs - Sector
        "XLF": {"sector": "ETF", "gics_sector": "Sector ETF", "gics_industry": "Financials"},
        "XLK": {"sector": "ETF", "gics_sector": "Sector ETF", "gics_industry": "Technology"},
        "XLV": {"sector": "ETF", "gics_sector": "Sector ETF", "gics_industry": "Health Care"},
        "XLE": {"sector": "ETF", "gics_sector": "Sector ETF", "gics_industry": "Energy"},
        "XLI": {"sector": "ETF", "gics_sector": "Sector ETF", "gics_industry": "Industrials"},
        "XLP": {"sector": "ETF", "gics_sector": "Sector ETF", "gics_industry": "Consumer Staples"},
        "XLY": {"sector": "ETF", "gics_sector": "Sector ETF", "gics_industry": "Consumer Discretionary"},
        "XLU": {"sector": "ETF", "gics_sector": "Sector ETF", "gics_industry": "Utilities"},
        "XLB": {"sector": "ETF", "gics_sector": "Sector ETF", "gics_industry": "Materials"},
        "XLRE": {"sector": "ETF", "gics_sector": "Sector ETF", "gics_industry": "Real Estate"},
        "XLC": {"sector": "ETF", "gics_sector": "Sector ETF", "gics_industry": "Communication Services"},
    }

    def __init__(self, db: Session):
        self.db = db
        self._tiingo_client = None
        # Legacy fallback API keys (optional)
        self.polygon_api_key = os.getenv("POLYGON_API_KEY")
        self.iex_api_key = os.getenv("IEX_API_KEY")

    @property
    def tiingo_client(self) -> Optional[TiingoClient]:
        """Lazy initialization of Tiingo client"""
        if self._tiingo_client is None and settings.TIINGO_API_KEY:
            config = {
                'api_key': settings.TIINGO_API_KEY,
                'session': True
            }
            self._tiingo_client = TiingoClient(config)
        return self._tiingo_client

    async def refresh_classification(self, security_id: int) -> Optional[Dict[str, Any]]:
        """
        Refresh classification for a single security.

        Args:
            security_id: Security ID to refresh

        Returns:
            Classification data dict or None if failed
        """
        security = self.db.query(Security).filter(Security.id == security_id).first()
        if not security:
            logger.error(f"Security {security_id} not found")
            return None

        ticker = security.symbol
        logger.info(f"Refreshing classification for {ticker}")

        # Try Tiingo first (primary source)
        if self.tiingo_client:
            classification = self._fetch_from_tiingo(ticker)
            if classification:
                return self._save_classification(security_id, classification, "tiingo")

        # Fallback to Polygon.io
        if self.polygon_api_key:
            classification = await self._fetch_from_polygon(ticker)
            if classification:
                return self._save_classification(security_id, classification, "polygon")

        # Fallback to IEX Cloud
        if self.iex_api_key:
            classification = await self._fetch_from_iex(ticker)
            if classification:
                return self._save_classification(security_id, classification, "iex")

        # Fallback to static mapping (basic SIC code mapping)
        classification = self._fetch_from_static(ticker)
        if classification:
            return self._save_classification(security_id, classification, "static")

        logger.warning(f"Could not fetch classification for {ticker}")
        return None

    async def refresh_all_classifications(self, limit: Optional[int] = None) -> Dict[str, Any]:
        """
        Refresh classifications for all securities (or a limited batch).

        Args:
            limit: Maximum number of securities to refresh (None for all)

        Returns:
            Summary dict with success/failure counts
        """
        query = self.db.query(Security)
        if limit:
            query = query.limit(limit)

        securities = query.all()
        results = {"total": len(securities), "success": 0, "failed": 0, "errors": []}

        for security in securities:
            try:
                classification = await self.refresh_classification(security.id)
                if classification:
                    results["success"] += 1
                else:
                    results["failed"] += 1
            except Exception as e:
                logger.error(f"Error refreshing {security.symbol}: {str(e)}")
                results["failed"] += 1
                results["errors"].append({"ticker": security.symbol, "error": str(e)})

        logger.info(f"Classification refresh complete: {results['success']}/{results['total']} successful")
        return results

    def _fetch_from_tiingo(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Fetch classification from Tiingo fundamentals API"""
        if not self.tiingo_client:
            return None

        try:
            # Normalize ticker for Tiingo
            normalized_ticker = ticker.replace('.', '-').upper()
            logger.info(f"Fetching Tiingo fundamentals for {normalized_ticker}")

            # Try fundamentals definitions first (has sector/industry)
            try:
                fundamentals = self.tiingo_client.get_fundamentals_definitions(normalized_ticker)
                if fundamentals and len(fundamentals) > 0:
                    fund_data = fundamentals[0] if isinstance(fundamentals, list) else fundamentals
                    sector = fund_data.get('sector', '')
                    industry = fund_data.get('industry', '')

                    if sector or industry:
                        result = {
                            "gics_sector": sector,
                            "sector": SectorMapper.normalize_sector(sector) if sector else None,
                            "gics_industry": industry,
                            "market_cap_category": None,
                        }
                        logger.info(f"Tiingo fundamentals for {ticker}: sector={sector}, industry={industry}")
                        return result
            except Exception as fund_err:
                logger.debug(f"Tiingo fundamentals not available for {ticker}: {fund_err}")

            # Fallback to basic metadata
            metadata = self.tiingo_client.get_ticker_metadata(normalized_ticker)
            if not metadata:
                logger.warning(f"No Tiingo metadata returned for {ticker}")
                return None

            # Check if static mapping has this ticker
            static_data = self.STATIC_MAPPING.get(normalized_ticker)
            if static_data:
                logger.info(f"Using static mapping for {ticker}")
                return static_data

            return None

        except Exception as e:
            logger.warning(f"Tiingo fetch failed for {ticker}: {str(e)}")
            return None

    async def _fetch_from_polygon(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Fetch classification from Polygon.io (fallback)"""
        try:
            url = f"https://api.polygon.io/v3/reference/tickers/{ticker}"
            params = {"apiKey": self.polygon_api_key}

            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                data = response.json()

                if data.get("status") == "OK" and data.get("results"):
                    result = data["results"]
                    return {
                        "gics_sector": result.get("sic_description") or result.get("industry"),
                        "sector": SectorMapper.normalize_sector(result.get("sic_description", "")),
                        "gics_industry": result.get("industry"),
                        "market_cap_category": self._categorize_market_cap(result.get("market_cap")),
                    }
        except Exception as e:
            logger.warning(f"Polygon.io fetch failed for {ticker}: {str(e)}")

        return None

    async def _fetch_from_iex(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Fetch classification from IEX Cloud"""
        try:
            url = f"https://cloud.iexapis.com/stable/stock/{ticker}/company"
            params = {"token": self.iex_api_key}

            async with httpx.AsyncClient(timeout=30.0) as client:
                response = await client.get(url, params=params)
                response.raise_for_status()
                data = response.json()

                sector = data.get("sector", "")
                industry = data.get("industry", "")

                return {
                    "gics_sector": sector,
                    "sector": SectorMapper.normalize_sector(sector),
                    "gics_industry": industry,
                    "market_cap_category": None,  # IEX doesn't provide market cap in company endpoint
                }
        except Exception as e:
            logger.warning(f"IEX Cloud fetch failed for {ticker}: {str(e)}")

        return None

    def _fetch_from_static(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Fallback to static sector mapping for common tickers"""
        normalized = TickerNormalizer.normalize(ticker)
        return self.STATIC_MAPPING.get(normalized)

    def _categorize_market_cap(self, market_cap: Optional[float]) -> Optional[str]:
        """Categorize market cap into Large/Mid/Small"""
        if not market_cap:
            return None

        if market_cap >= 10_000_000_000:  # $10B+
            return "Large"
        elif market_cap >= 2_000_000_000:  # $2B-$10B
            return "Mid"
        else:
            return "Small"

    def _save_classification(
        self, security_id: int, classification: Dict[str, Any], source: str
    ) -> Dict[str, Any]:
        """Save or update classification in database"""
        existing = (
            self.db.query(SectorClassification)
            .filter(SectorClassification.security_id == security_id)
            .first()
        )

        if existing:
            # Update existing
            existing.gics_sector = classification.get("gics_sector")
            existing.gics_industry = classification.get("gics_industry")
            existing.sector = classification.get("sector")
            existing.market_cap_category = classification.get("market_cap_category")
            existing.source = source
            existing.as_of_date = date.today()
            existing.updated_at = datetime.utcnow()
        else:
            # Create new
            new_classification = SectorClassification(
                security_id=security_id,
                gics_sector=classification.get("gics_sector"),
                gics_industry=classification.get("gics_industry"),
                sector=classification.get("sector"),
                market_cap_category=classification.get("market_cap_category"),
                source=source,
                as_of_date=date.today(),
            )
            self.db.add(new_classification)

        self.db.commit()
        logger.info(f"Saved classification for security {security_id} from {source}")
        return classification


class BenchmarkService:
    """
    Service for fetching and storing S&P 500 constituent data.
    """

    # S&P 500 ETF holdings URL
    SPY_HOLDINGS_URL = "https://www.ssga.com/us/en/individual/etfs/library-content/products/fund-data/etfs/us/holdings-daily-us-en-spy.xlsx"

    def __init__(self, db: Session):
        self.db = db

    async def refresh_benchmark(self, benchmark_code: str) -> Dict[str, Any]:
        """
        Refresh S&P 500 benchmark constituents.

        Args:
            benchmark_code: Benchmark code (SP500 or SPY)

        Returns:
            Summary dict with success/failure info
        """
        # Accept both SP500 and SPY as valid codes
        if benchmark_code in ["SP500", "SPY"]:
            return await self._refresh_sp500()
        else:
            return {"success": False, "error": f"Only SP500 benchmark is supported. Got: {benchmark_code}"}

    async def _refresh_sp500(self) -> Dict[str, Any]:
        """Refresh S&P 500 holdings from State Street SPY ETF"""
        try:
            logger.info(f"Fetching S&P 500 holdings from SPY ETF: {self.SPY_HOLDINGS_URL}")
            async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
                response = await client.get(self.SPY_HOLDINGS_URL)
                logger.info(f"SPY response status: {response.status_code}, content-type: {response.headers.get('content-type')}")
                response.raise_for_status()

                # Parse Excel file
                df = pd.read_excel(io.BytesIO(response.content), skiprows=4)
                logger.info(f"SPY DataFrame columns: {df.columns.tolist()}")
                logger.info(f"SPY DataFrame rows: {len(df)}")

                # Expecting columns: Ticker, Name, Weight, Sector, etc.
                holdings = []
                for _, row in df.iterrows():
                    ticker = row.get("Ticker") or row.get("Symbol")
                    weight = row.get("Weight") or row.get("% Weight")
                    # Get sector directly from SPY Excel file
                    sector = row.get("Sector")

                    if pd.notna(ticker) and pd.notna(weight):
                        holding_data = {
                            "ticker": TickerNormalizer.normalize(str(ticker)),
                            "weight": float(weight) if isinstance(weight, (int, float)) else float(weight.strip('%')) / 100.0,
                        }
                        # Include sector if available from the SPY file
                        if pd.notna(sector) and str(sector).strip():
                            holding_data["sector"] = str(sector).strip()
                        holdings.append(holding_data)

                logger.info(f"Parsed {len(holdings)} S&P 500 holdings from SPY")

                if len(holdings) == 0:
                    return {"success": False, "error": "No holdings parsed from Excel file"}

                return self._save_benchmark_holdings("SP500", holdings, self.SPY_HOLDINGS_URL)

        except Exception as e:
            logger.error(f"Failed to refresh S&P 500: {str(e)}", exc_info=True)
            return {"success": False, "error": str(e)}

    def _save_benchmark_holdings(
        self, benchmark_code: str, holdings: List[Dict[str, Any]], source_url: str
    ) -> Dict[str, Any]:
        """Save benchmark holdings to database with sector enrichment.

        Sector sources (in order of priority):
        1. Sector from the source file (e.g., SPY Excel 'Sector' column)
        2. Sector from SectorClassification table in database
        3. Sector from static mapping (STATIC_MAPPING)
        """
        try:
            as_of = date.today()

            # Delete old holdings for this benchmark
            self.db.query(BenchmarkConstituent).filter(
                BenchmarkConstituent.benchmark_code == benchmark_code,
                BenchmarkConstituent.as_of_date < as_of
            ).delete()

            # Delete today's holdings if they exist (to refresh)
            self.db.query(BenchmarkConstituent).filter(
                BenchmarkConstituent.benchmark_code == benchmark_code,
                BenchmarkConstituent.as_of_date == as_of
            ).delete()

            # Build lookup of symbol -> sector from SectorClassification (fallback)
            sector_lookup = {}
            classifications = self.db.query(
                Security.symbol,
                SectorClassification.sector,
                SectorClassification.gics_sector
            ).join(
                SectorClassification, Security.id == SectorClassification.security_id
            ).all()

            for c in classifications:
                # Use sector if available, otherwise use gics_sector
                sector_lookup[c.symbol] = c.sector or c.gics_sector

            logger.info(f"Loaded {len(sector_lookup)} sector classifications for fallback enrichment")

            # Insert new holdings with sector data
            classified_count = 0
            source_file_count = 0
            db_count = 0
            static_count = 0

            for holding in holdings:
                ticker = holding["ticker"]

                # Priority 1: Use sector from source file (e.g., SPY Excel)
                sector = holding.get("sector")
                if sector:
                    source_file_count += 1

                # Priority 2: Look up from DB
                if not sector:
                    sector = sector_lookup.get(ticker)
                    if sector:
                        db_count += 1

                # Priority 3: Fall back to STATIC_MAPPING
                if not sector and ticker in ClassificationService.STATIC_MAPPING:
                    sector = ClassificationService.STATIC_MAPPING[ticker].get("sector")
                    if sector:
                        static_count += 1

                if sector:
                    classified_count += 1

                constituent = BenchmarkConstituent(
                    benchmark_code=benchmark_code,
                    symbol=ticker,
                    weight=holding["weight"],
                    sector=sector,
                    as_of_date=as_of,
                    source_url=source_url,
                )
                self.db.add(constituent)

            self.db.commit()

            logger.info(f"Saved {len(holdings)} holdings for {benchmark_code} ({classified_count} with sectors)")
            logger.info(f"  Sector sources: {source_file_count} from file, {db_count} from DB, {static_count} from static mapping")

            return {
                "success": True,
                "benchmark": benchmark_code,
                "count": len(holdings),
                "classified_count": classified_count,
                "sector_sources": {
                    "source_file": source_file_count,
                    "database": db_count,
                    "static_mapping": static_count
                },
                "as_of_date": as_of.isoformat(),
            }

        except Exception as e:
            self.db.rollback()
            logger.error(f"Failed to save benchmark holdings: {str(e)}")
            return {"success": False, "error": str(e)}


class FactorReturnsService:
    """
    Service for fetching and storing factor returns from Kenneth French Data Library.
    """

    # Fama-French 5 Factors + Momentum (daily)
    FACTOR_DATA_URL = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Research_Data_5_Factors_2x3_daily_CSV.zip"
    MOMENTUM_DATA_URL = "https://mba.tuck.dartmouth.edu/pages/faculty/ken.french/ftp/F-F_Momentum_Factor_daily_CSV.zip"

    def __init__(self, db: Session):
        self.db = db

    async def refresh_factor_returns(self, start_date: Optional[date] = None) -> Dict[str, Any]:
        """
        Refresh factor returns from Kenneth French Data Library.

        Args:
            start_date: Start date for refresh (None = last 5 years)

        Returns:
            Summary dict
        """
        if not start_date:
            start_date = date.today() - timedelta(days=365 * 5)

        logger.info(f"Refreshing factor returns from {start_date}")

        results = {"success": 0, "failed": 0, "errors": []}

        # Fetch 5 factors
        try:
            factors_5 = await self._fetch_5_factors()
            if factors_5 is not None and not factors_5.empty:
                self._save_factor_returns(factors_5, start_date)
                results["success"] += len(factors_5)
        except Exception as e:
            logger.error(f"Failed to fetch 5 factors: {str(e)}")
            results["failed"] += 1
            results["errors"].append({"source": "5_factors", "error": str(e)})

        # Fetch momentum
        try:
            momentum = await self._fetch_momentum()
            if momentum is not None and not momentum.empty:
                self._save_factor_returns(momentum, start_date)
                results["success"] += len(momentum)
        except Exception as e:
            logger.error(f"Failed to fetch momentum: {str(e)}")
            results["failed"] += 1
            results["errors"].append({"source": "momentum", "error": str(e)})

        return results

    async def _fetch_5_factors(self) -> Optional[pd.DataFrame]:
        """Fetch Fama-French 5 factors"""
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.get(self.FACTOR_DATA_URL)
                response.raise_for_status()

                # Extract CSV from ZIP
                with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                    csv_name = z.namelist()[0]
                    with z.open(csv_name) as f:
                        # Read CSV (skip header rows)
                        df = pd.read_csv(f, skiprows=3)

                        # Find where data ends (blank line or non-numeric)
                        df = df[pd.to_numeric(df.iloc[:, 0], errors='coerce').notna()]

                        # Rename columns
                        df.columns = ['Date', 'Mkt-RF', 'SMB', 'HML', 'RMW', 'CMA', 'RF']

                        # Convert date
                        df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d')

                        # Convert percentages to decimals
                        for col in ['Mkt-RF', 'SMB', 'HML', 'RMW', 'CMA', 'RF']:
                            df[col] = pd.to_numeric(df[col], errors='coerce') / 100.0

                        return df

        except Exception as e:
            logger.error(f"Error fetching 5 factors: {str(e)}")
            return None

    async def _fetch_momentum(self) -> Optional[pd.DataFrame]:
        """Fetch momentum factor"""
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                response = await client.get(self.MOMENTUM_DATA_URL)
                response.raise_for_status()

                with zipfile.ZipFile(io.BytesIO(response.content)) as z:
                    csv_name = z.namelist()[0]
                    with z.open(csv_name) as f:
                        df = pd.read_csv(f, skiprows=13)  # Momentum file has more header rows

                        df = df[pd.to_numeric(df.iloc[:, 0], errors='coerce').notna()]
                        df.columns = ['Date', 'Mom']

                        df['Date'] = pd.to_datetime(df['Date'], format='%Y%m%d')
                        df['Mom'] = pd.to_numeric(df['Mom'], errors='coerce') / 100.0

                        return df

        except Exception as e:
            logger.error(f"Error fetching momentum: {str(e)}")
            return None

    def _save_factor_returns(self, df: pd.DataFrame, start_date: date) -> None:
        """Save factor returns to database"""
        # Filter by date
        df = df[df['Date'] >= pd.Timestamp(start_date)]

        # Melt dataframe to long format, excluding RF (risk-free rate) column
        id_vars = ['Date']
        value_vars = [col for col in df.columns if col not in ['Date', 'RF']]

        logger.info(f"Saving factors: {value_vars}")

        df_long = df.melt(id_vars=id_vars, value_vars=value_vars, var_name='factor_name', value_name='value')

        # Delete existing data for these dates and factors
        dates_to_delete = df_long['Date'].unique()
        factor_names = df_long['factor_name'].unique()

        deleted_count = self.db.query(FactorReturns).filter(
            FactorReturns.date.in_([d.date() for d in dates_to_delete])
        ).delete(synchronize_session=False)

        logger.info(f"Deleted {deleted_count} existing factor return records")

        # Insert new data
        saved_count = 0
        for _, row in df_long.iterrows():
            if pd.notna(row['value']):
                factor_return = FactorReturns(
                    date=row['Date'].date(),
                    factor_name=row['factor_name'],
                    value=float(row['value']),
                    source="kenneth_french",
                )
                self.db.add(factor_return)
                saved_count += 1

        self.db.commit()

        # Log summary by factor
        factor_counts = df_long.groupby('factor_name').size().to_dict()
        logger.info(f"Saved {saved_count} total factor return records")
        for factor, count in factor_counts.items():
            logger.info(f"  - {factor}: {count} records")
