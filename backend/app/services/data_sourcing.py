"""
Data sourcing services for classifications, benchmark constituents, and factor returns.
Updated: 2026-01-29 - Migrated to Tiingo API
"""
import os
import re
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
        # ============ COMPREHENSIVE S&P 500 COVERAGE ============
        # Technology Sector
        "ORCL": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "ACN": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "IT Services"},
        "IBM": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "IT Services"},
        "AMD": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Semiconductors"},
        "TXN": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Semiconductors"},
        "QCOM": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Semiconductors"},
        "MU": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Semiconductors"},
        "LRCX": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Semiconductor Equipment"},
        "INTC": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Semiconductors"},
        "ADI": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Semiconductors"},
        "MCHP": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Semiconductors"},
        "NXPI": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Semiconductors"},
        "ON": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Semiconductors"},
        "MPWR": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Semiconductors"},
        "CDNS": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "FTNT": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "CRWD": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "PLTR": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "TEAM": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "HUBS": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "ZS": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "ANSS": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "CTSH": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "IT Services"},
        "EPAM": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "IT Services"},
        "IT": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "IT Services"},
        "GDDY": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "GEN": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "HPE": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Technology Hardware"},
        "HPQ": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Technology Hardware"},
        "JNPR": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Communications Equipment"},
        "KEYS": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Electronic Equipment"},
        "NTAP": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Technology Hardware"},
        "PTC": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "ROP": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "STX": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Technology Hardware"},
        "TDY": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Electronic Equipment"},
        "TER": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Semiconductor Equipment"},
        "TRMB": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Electronic Equipment"},
        "TYL": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "VRSN": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "WDC": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Technology Hardware"},
        "ZBRA": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Electronic Equipment"},
        # Healthcare Sector
        "PFE": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Pharmaceuticals"},
        "MRK": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Pharmaceuticals"},
        "ABBV": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Pharmaceuticals"},
        "BMY": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Pharmaceuticals"},
        "GILD": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Biotechnology"},
        "VRTX": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Biotechnology"},
        "REGN": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Biotechnology"},
        "BIIB": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Biotechnology"},
        "ISRG": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Equipment"},
        "BSX": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Equipment"},
        "EW": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Equipment"},
        "DXCM": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Equipment"},
        "IDXX": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Equipment"},
        "A": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Life Sciences Tools"},
        "IQV": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Life Sciences Tools"},
        "MTD": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Life Sciences Tools"},
        "WAT": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Life Sciences Tools"},
        "CI": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Providers"},
        "ELV": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Providers"},
        "HUM": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Providers"},
        "CNC": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Providers"},
        "HCA": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Providers"},
        "MCK": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Distributors"},
        "CAH": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Distributors"},
        "COR": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Distributors"},
        "ALGN": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Equipment"},
        "BAX": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Equipment"},
        "BDX": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Equipment"},
        "COO": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Equipment"},
        "HOLX": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Equipment"},
        "PODD": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Equipment"},
        "RMD": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Equipment"},
        "TFX": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Equipment"},
        "TECH": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Life Sciences Tools"},
        "WST": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Life Sciences Tools"},
        "INCY": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Biotechnology"},
        "VTRS": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Pharmaceuticals"},
        # Financials Sector
        "BAC": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Banks"},
        "GS": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Capital Markets"},
        "MS": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Capital Markets"},
        "C": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Banks"},
        "SCHW": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Capital Markets"},
        "AXP": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Consumer Finance"},
        "SPGI": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Capital Markets"},
        "ICE": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Capital Markets"},
        "PNC": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Banks"},
        "TFC": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Banks"},
        "AIG": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Insurance"},
        "MET": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Insurance"},
        "PRU": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Insurance"},
        "AFL": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Insurance"},
        "ALL": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Insurance"},
        "TRV": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Insurance"},
        "CB": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Insurance"},
        "AON": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Insurance"},
        "MMC": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Insurance"},
        "COF": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Consumer Finance"},
        "DFS": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Consumer Finance"},
        "SYF": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Consumer Finance"},
        "PYPL": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Financial Services"},
        "FIS": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Financial Services"},
        "FISV": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Financial Services"},
        "GPN": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Financial Services"},
        "AJG": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Insurance"},
        "ACGL": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Insurance"},
        "BRO": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Insurance"},
        "CINF": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Insurance"},
        "EG": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Insurance"},
        "GL": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Insurance"},
        "HIG": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Insurance"},
        "L": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Insurance"},
        "LNC": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Insurance"},
        "RJF": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Capital Markets"},
        "FITB": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Banks"},
        "HBAN": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Banks"},
        "KEY": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Banks"},
        "MTB": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Banks"},
        "NTRS": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Banks"},
        "RF": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Banks"},
        "STT": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Banks"},
        "CFG": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Banks"},
        "BK": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Banks"},
        "TROW": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Asset Management"},
        "BEN": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Asset Management"},
        "IVZ": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Asset Management"},
        "NDAQ": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Capital Markets"},
        "CBOE": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Capital Markets"},
        "MSCI": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Capital Markets"},
        "MCO": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Capital Markets"},
        # Consumer Discretionary Sector
        "MCD": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Restaurants"},
        "LOW": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Home Improvement Retail"},
        "TJX": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Apparel Retail"},
        "BKNG": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Hotels & Resorts"},
        "CMG": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Restaurants"},
        "ORLY": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Specialty Retail"},
        "AZO": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Specialty Retail"},
        "GM": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Automobiles"},
        "F": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Automobiles"},
        "ROST": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Apparel Retail"},
        "YUM": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Restaurants"},
        "HLT": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Hotels & Resorts"},
        "LVS": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Casinos & Gaming"},
        "WYNN": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Casinos & Gaming"},
        "MGM": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Casinos & Gaming"},
        "CZR": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Casinos & Gaming"},
        "RCL": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Hotels & Resorts"},
        "CCL": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Hotels & Resorts"},
        "NCLH": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Hotels & Resorts"},
        "EXPE": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Hotels & Resorts"},
        "DHI": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Homebuilding"},
        "LEN": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Homebuilding"},
        "PHM": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Homebuilding"},
        "NVR": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Homebuilding"},
        "TOL": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Homebuilding"},
        "BBY": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Specialty Retail"},
        "DPZ": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Restaurants"},
        "EBAY": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Internet Retail"},
        "ETSY": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Internet Retail"},
        "GRMN": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Consumer Electronics"},
        "GPC": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Specialty Retail"},
        "HAS": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Leisure Products"},
        "KMX": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Specialty Retail"},
        "LKQ": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Distributors"},
        "LULU": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Apparel Retail"},
        "POOL": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Distributors"},
        "TPR": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Apparel"},
        "ULTA": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Specialty Retail"},
        "VFC": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Apparel"},
        "APTV": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Auto Parts"},
        "BWA": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Auto Parts"},
        # Consumer Staples Sector
        "WMT": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Consumer Staples Distribution"},
        "KO": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Beverages"},
        "PM": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Tobacco"},
        "MO": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Tobacco"},
        "MDLZ": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Packaged Foods"},
        "TGT": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Consumer Staples Distribution"},
        "KR": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Food Retail"},
        "SYY": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Food Distributors"},
        "GIS": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Packaged Foods"},
        "K": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Packaged Foods"},
        "HSY": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Packaged Foods"},
        "KDP": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Beverages"},
        "MNST": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Beverages"},
        "ADM": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Agricultural Products"},
        "EL": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Personal Products"},
        "CL": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Household Products"},
        "KMB": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Household Products"},
        "CHD": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Household Products"},
        "WBA": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Drug Retail"},
        "DG": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Consumer Staples Distribution"},
        "DLTR": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Consumer Staples Distribution"},
        "BG": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Agricultural Products"},
        "CAG": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Packaged Foods"},
        "CPB": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Packaged Foods"},
        "HRL": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Packaged Foods"},
        "MKC": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Packaged Foods"},
        "SJM": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Packaged Foods"},
        "TSN": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Packaged Foods"},
        "LW": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Packaged Foods"},
        # Industrials Sector
        "CAT": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Machinery"},
        "GE": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Aerospace & Defense"},
        "RTX": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Aerospace & Defense"},
        "BA": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Aerospace & Defense"},
        "MMM": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Industrial Conglomerates"},
        "ETN": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Electrical Equipment"},
        "EMR": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Electrical Equipment"},
        "ITW": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Machinery"},
        "PH": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Machinery"},
        "NSC": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Railroads"},
        "WM": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Waste Management"},
        "RSG": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Waste Management"},
        "WCN": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Waste Management"},
        "CTAS": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Commercial Services"},
        "CARR": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Building Products"},
        "TT": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Building Products"},
        "AME": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Electrical Equipment"},
        "ROK": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Electrical Equipment"},
        "DOV": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Machinery"},
        "VRSK": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Research & Consulting"},
        "GWW": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Industrial Distribution"},
        "EXPD": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Air Freight & Logistics"},
        "CHRW": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Air Freight & Logistics"},
        "JBHT": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Trucking"},
        "ODFL": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Trucking"},
        "DAL": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Airlines"},
        "UAL": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Airlines"},
        "LUV": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Airlines"},
        "AAL": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Airlines"},
        "ALK": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Airlines"},
        "GD": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Aerospace & Defense"},
        "NOC": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Aerospace & Defense"},
        "LHX": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Aerospace & Defense"},
        "TXT": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Aerospace & Defense"},
        "AXON": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Aerospace & Defense"},
        "AOS": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Building Products"},
        "EME": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Construction & Engineering"},
        "FTV": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Industrial Conglomerates"},
        "GGG": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Machinery"},
        "GNRC": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Electrical Equipment"},
        "HWM": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Aerospace & Defense"},
        "IEX": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Machinery"},
        "IR": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Machinery"},
        "J": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Research & Consulting"},
        "LDOS": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Research & Consulting"},
        "MAS": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Building Products"},
        "NDSN": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Machinery"},
        "PAYX": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Professional Services"},
        "PAYC": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Professional Services"},
        "ROL": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Commercial Services"},
        "SNA": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Machinery"},
        "SWK": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Machinery"},
        "WAB": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Machinery"},
        "XYL": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Machinery"},
        # Energy Sector
        "XOM": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas"},
        "CVX": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas"},
        "COP": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas"},
        "EOG": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas"},
        "PXD": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas"},
        "OXY": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas"},
        "MPC": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas Refining"},
        "VLO": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas Refining"},
        "PSX": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas Refining"},
        "HAL": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas Equipment"},
        "BKR": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas Equipment"},
        "KMI": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas Storage"},
        "WMB": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas Storage"},
        "OKE": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas Storage"},
        "TRGP": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas Storage"},
        "HES": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas"},
        "DVN": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas"},
        "FANG": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas"},
        "APA": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas"},
        "MRO": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas"},
        # Materials Sector
        "SHW": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Chemicals"},
        "ECL": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Chemicals"},
        "DD": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Chemicals"},
        "DOW": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Chemicals"},
        "PPG": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Chemicals"},
        "VMC": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Construction Materials"},
        "NEM": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Metals & Mining"},
        "CTVA": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Chemicals"},
        "ALB": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Chemicals"},
        "IFF": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Chemicals"},
        "CE": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Chemicals"},
        "EMN": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Chemicals"},
        "CF": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Chemicals"},
        "MOS": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Chemicals"},
        "FMC": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Chemicals"},
        "PKG": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Containers & Packaging"},
        "IP": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Paper & Forest Products"},
        "AVY": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Containers & Packaging"},
        "BALL": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Containers & Packaging"},
        "AMCR": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Containers & Packaging"},
        "SEE": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Containers & Packaging"},
        "STLD": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Steel"},
        "RS": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Steel"},
        # Communication Services Sector
        "T": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Telecommunications"},
        "VZ": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Telecommunications"},
        "TMUS": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Telecommunications"},
        "CMCSA": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Media"},
        "CHTR": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Media"},
        "WBD": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Entertainment"},
        "PARA": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Entertainment"},
        "FOX": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Media"},
        "FOXA": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Media"},
        "EA": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Entertainment"},
        "TTWO": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Entertainment"},
        "ATVI": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Entertainment"},
        "LYV": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Entertainment"},
        "OMC": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Advertising"},
        "IPG": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Advertising"},
        "NWSA": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Media"},
        "NWS": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Media"},
        # Real Estate Sector
        "AMT": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Tower REITs"},
        "PLD": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Industrial REITs"},
        "CCI": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Tower REITs"},
        "EQIX": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Data Center REITs"},
        "PSA": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Storage REITs"},
        "SPG": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Retail REITs"},
        "WELL": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Health Care REITs"},
        "O": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Retail REITs"},
        "AVB": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Residential REITs"},
        "EQR": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Residential REITs"},
        "VICI": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Gaming REITs"},
        "SBAC": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Tower REITs"},
        "ARE": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Office REITs"},
        "BXP": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Office REITs"},
        "EXR": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Storage REITs"},
        "MAA": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Residential REITs"},
        "UDR": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Residential REITs"},
        "ESS": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Residential REITs"},
        "CPT": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Residential REITs"},
        "INVH": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Residential REITs"},
        "HST": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Hotel REITs"},
        "IRM": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Storage REITs"},
        "KIM": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Retail REITs"},
        "REG": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Retail REITs"},
        "FRT": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Retail REITs"},
        "SLG": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Office REITs"},
        # Utilities Sector
        "DUK": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Electric Utilities"},
        "SO": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Electric Utilities"},
        "D": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Electric Utilities"},
        "SRE": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Multi-Utilities"},
        "EXC": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Electric Utilities"},
        "XEL": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Electric Utilities"},
        "ED": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Electric Utilities"},
        "WEC": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Electric Utilities"},
        "PCG": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Electric Utilities"},
        "AWK": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Water Utilities"},
        "ES": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Electric Utilities"},
        "ETR": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Electric Utilities"},
        "PPL": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Electric Utilities"},
        "FE": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Electric Utilities"},
        "DTE": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Electric Utilities"},
        "EIX": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Electric Utilities"},
        "CMS": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Electric Utilities"},
        "CNP": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Electric Utilities"},
        "AES": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Electric Utilities"},
        "ATO": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Gas Utilities"},
        "LNT": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Electric Utilities"},
        "NI": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Multi-Utilities"},
        "NRG": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Electric Utilities"},
        "PNW": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Electric Utilities"},
        "EVRG": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Electric Utilities"},
        # ============ EXTENDED COVERAGE: MID-CAP & SMALL-CAP ============
        # Technology - Mid/Small Cap
        "FICO": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "MANH": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "PCTY": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "SSNC": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "GWRE": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "BSY": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "DOCU": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "NTNX": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "VEEV": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "MDB": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "NET": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "OKTA": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "CFLT": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "PATH": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "TWLO": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "U": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "ZM": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "DBX": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "APPN": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "CDAY": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "WK": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "ESTC": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "DT": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "TOST": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "DUOL": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "RIOT": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "MARA": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "COIN": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Capital Markets"},
        "WOLF": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Semiconductors"},
        "MRVL": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Semiconductors"},
        "SWKS": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Semiconductors"},
        "QRVO": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Semiconductors"},
        "SMCI": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Technology Hardware"},
        "ENTG": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Semiconductor Equipment"},
        "MKSI": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Semiconductor Equipment"},
        "COHR": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Electronic Equipment"},
        "GLW": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Electronic Equipment"},
        "TEL": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Electronic Equipment"},
        "APH": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Electronic Equipment"},
        "CDW": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "IT Services"},
        "FI": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "IT Services"},
        "BR": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "IT Services"},
        "WEX": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "IT Services"},
        "DXC": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "IT Services"},
        "AFRM": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "SQ": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "HOOD": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Capital Markets"},
        "SOFI": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Consumer Finance"},
        "ASAN": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "ROKU": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Entertainment"},
        "SNAP": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Interactive Media"},
        "PINS": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Interactive Media"},
        "TTD": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "SPOT": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Entertainment"},
        "RBLX": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Entertainment"},
        "DASH": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Internet Services"},
        "LYFT": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Internet Services"},
        "UBER": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Internet Services"},
        "ABNB": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Hotels & Resorts"},
        "DKNG": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Casinos & Gaming"},
        "PENN": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Casinos & Gaming"},
        "CHWY": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Internet Retail"},
        "W": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Internet Retail"},
        "RIVN": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Automobiles"},
        "LCID": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Automobiles"},
        # Healthcare - Mid/Small Cap
        "NTRA": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Life Sciences Tools"},
        "VEEV": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Technology"},
        "RVMD": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Biotechnology"},
        "PCVX": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Biotechnology"},
        "ARGX": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Biotechnology"},
        "SRPT": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Biotechnology"},
        "EXAS": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Life Sciences Tools"},
        "NBIX": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Pharmaceuticals"},
        "JAZZ": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Pharmaceuticals"},
        "BMRN": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Biotechnology"},
        "ALNY": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Biotechnology"},
        "HALO": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Biotechnology"},
        "SGEN": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Biotechnology"},
        "IONS": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Biotechnology"},
        "RARE": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Biotechnology"},
        "GMED": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Equipment"},
        "NVST": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Equipment"},
        "HAE": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Equipment"},
        "PEN": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Equipment"},
        "XRAY": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Equipment"},
        "STE": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Equipment"},
        "ENSG": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Providers"},
        "ACHC": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Providers"},
        "THC": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Providers"},
        "DVA": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Providers"},
        "DOCS": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Technology"},
        "GEHC": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Equipment"},
        "SOLV": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Equipment"},
        # Financials - Mid/Small Cap
        "LPLA": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Capital Markets"},
        "RGA": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Insurance"},
        "WRB": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Insurance"},
        "MKTX": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Capital Markets"},
        "IBKR": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Capital Markets"},
        "ALLY": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Consumer Finance"},
        "EWBC": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Banks"},
        "FNF": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Insurance"},
        "ORI": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Insurance"},
        "SEIC": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Asset Management"},
        "VOYA": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Insurance"},
        "CG": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Asset Management"},
        "KKR": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Asset Management"},
        "APO": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Asset Management"},
        "ARES": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Asset Management"},
        "OWL": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Asset Management"},
        "MSTR": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "FHN": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Banks"},
        "SNV": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Banks"},
        "ZION": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Banks"},
        "CMA": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Banks"},
        "FRC": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Banks"},
        "SBNY": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Banks"},
        "SIVB": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Banks"},
        "WAL": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Banks"},
        "BOKF": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Banks"},
        "PNFP": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Banks"},
        "WTFC": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Banks"},
        "SSB": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Banks"},
        "ONB": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Banks"},
        "COLB": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Banks"},
        "HLNE": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Asset Management"},
        "TRUP": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Insurance"},
        "KNSL": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Insurance"},
        "RNR": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Insurance"},
        "EG": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Insurance"},
        # Industrials - Mid/Small Cap
        "AXON": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Aerospace & Defense"},
        "BLDR": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Building Products"},
        "ACM": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Construction & Engineering"},
        "GNRC": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Electrical Equipment"},
        "RBC": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Machinery"},
        "FIX": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Construction & Engineering"},
        "MTZ": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Construction & Engineering"},
        "TTC": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Machinery"},
        "SWK": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Industrial Machinery"},
        "WSO": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Industrial Distribution"},
        "CR": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Machinery"},
        "TREX": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Building Products"},
        "SITE": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Industrial Distribution"},
        "WMS": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Building Products"},
        "AAON": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Building Products"},
        "FBIN": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Building Products"},
        "AIT": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Industrial Distribution"},
        "ALLE": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Building Products"},
        "AYI": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Electrical Equipment"},
        "RRX": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Electrical Equipment"},
        "HUBB": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Electrical Equipment"},
        "PLUG": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Electrical Equipment"},
        "XPO": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Trucking"},
        "SAIA": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Trucking"},
        "R": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Trucking"},
        "KEX": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Marine Transportation"},
        "GXO": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Air Freight & Logistics"},
        "LSTR": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Trucking"},
        "SNDR": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Trucking"},
        "KNX": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Trucking"},
        "WERN": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Trucking"},
        "HTLD": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Trucking"},
        "HEI": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Aerospace & Defense"},
        "TDG": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Aerospace & Defense"},
        "BWXT": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Aerospace & Defense"},
        "KTOS": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Aerospace & Defense"},
        "SPR": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Aerospace & Defense"},
        "TXT": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Aerospace & Defense"},
        "WWD": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Aerospace & Defense"},
        "EAF": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Machinery"},
        "PAYC": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "CSGP": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Research & Consulting"},
        "VRSK": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Research & Consulting"},
        "FLR": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Construction & Engineering"},
        "BLD": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Building Products"},
        "RHI": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Professional Services"},
        "HSIC": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Distributors"},
        "EXPO": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Professional Services"},
        "HRI": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Trading Companies"},
        "WCC": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Industrial Distribution"},
        "GGG": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Machinery"},
        # Consumer Discretionary - Mid/Small Cap
        "DECK": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Footwear"},
        "LEVI": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Apparel"},
        "PVH": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Apparel"},
        "RL": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Apparel"},
        "GIII": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Apparel"},
        "HBI": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Apparel"},
        "CROX": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Footwear"},
        "SKX": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Footwear"},
        "WSM": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Home Furnishings"},
        "RH": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Home Furnishings"},
        "FIVE": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Specialty Retail"},
        "OLLI": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Specialty Retail"},
        "AEO": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Apparel Retail"},
        "ANF": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Apparel Retail"},
        "GAP": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Apparel Retail"},
        "GPS": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Apparel Retail"},
        "FL": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Apparel Retail"},
        "CAVA": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Restaurants"},
        "SHAK": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Restaurants"},
        "WING": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Restaurants"},
        "TXRH": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Restaurants"},
        "EAT": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Restaurants"},
        "CAKE": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Restaurants"},
        "DINE": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Restaurants"},
        "QSR": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Restaurants"},
        "WEN": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Restaurants"},
        "YUM": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Restaurants"},
        "MHK": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Home Improvement Retail"},
        "FND": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Home Improvement Retail"},
        "AAP": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Automotive Retail"},
        "GPC": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Automotive Parts"},
        "TSCO": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Specialty Retail"},
        "WOOF": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Specialty Retail"},
        "LEA": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Auto Parts"},
        "APTV": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Auto Parts"},
        "VC": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Auto Parts"},
        "DAN": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Auto Parts"},
        "LAD": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Automotive Retail"},
        "AN": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Automotive Retail"},
        "PAG": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Automotive Retail"},
        "SAH": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Automotive Retail"},
        "SIG": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Specialty Retail"},
        "BIRK": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Footwear"},
        "ONON": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Footwear"},
        # Consumer Staples - Mid/Small Cap
        "COKE": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Beverages"},
        "SAM": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Beverages"},
        "MNST": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Beverages"},
        "CELH": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Beverages"},
        "BF.A": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Beverages"},
        "BF.B": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Beverages"},
        "TAP": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Beverages"},
        "SFM": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Food Retail"},
        "SPTN": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Food Retail"},
        "ACI": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Food Retail"},
        "WBA": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Drug Retail"},
        "USFD": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Food Distribution"},
        "PFGC": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Food Distribution"},
        "INGR": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Packaged Foods"},
        "THS": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Packaged Foods"},
        "BGS": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Packaged Foods"},
        "FLO": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Packaged Foods"},
        "FRPT": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Packaged Foods"},
        "EPC": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Household Products"},
        "SPB": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Household Products"},
        "CL": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Household Products"},
        "EL": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Personal Products"},
        # Energy - Mid/Small Cap
        "AR": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas E&P"},
        "EQT": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas E&P"},
        "RRC": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas E&P"},
        "SM": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas E&P"},
        "PR": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas E&P"},
        "OVV": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas E&P"},
        "CTRA": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas E&P"},
        "MTDR": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas E&P"},
        "CHRD": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas E&P"},
        "MGY": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas E&P"},
        "NOG": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas E&P"},
        "WFRD": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas Services"},
        "CHX": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas Equipment"},
        "HP": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas Drilling"},
        "RIG": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas Drilling"},
        "DINO": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas Refining"},
        "PBF": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas Refining"},
        "DK": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas Refining"},
        "AM": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas Midstream"},
        "DTM": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas Midstream"},
        "ENLC": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas Midstream"},
        "WES": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas Midstream"},
        "ET": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas Midstream"},
        "EPD": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas Midstream"},
        "MLP": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas Midstream"},
        "PAA": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas Midstream"},
        "MPLX": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas Midstream"},
        # Materials - Mid/Small Cap
        "MP": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Metals & Mining"},
        "AA": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Metals & Mining"},
        "CLF": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Steel"},
        "X": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Steel"},
        "STLD": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Steel"},
        "ATI": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Steel"},
        "CRS": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Steel"},
        "CC": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Chemicals"},
        "HUN": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Chemicals"},
        "OLN": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Chemicals"},
        "AXTA": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Chemicals"},
        "RPM": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Chemicals"},
        "CBT": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Chemicals"},
        "TROX": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Chemicals"},
        "SMG": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Chemicals"},
        "SON": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Containers & Packaging"},
        "SEE": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Containers & Packaging"},
        "GPK": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Containers & Packaging"},
        "OI": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Containers & Packaging"},
        "SWX": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Construction Materials"},
        "EXP": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Construction Materials"},
        "USLM": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Construction Materials"},
        "RYN": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Timber REITs"},
        # Real Estate - Mid/Small Cap
        "CBRE": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Real Estate Services"},
        "JLL": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Real Estate Services"},
        "CWK": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Real Estate Services"},
        "Z": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Real Estate Services"},
        "ZG": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Real Estate Services"},
        "RDFN": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Real Estate Services"},
        "OPEN": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Real Estate Services"},
        "NMRK": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Real Estate Services"},
        "RKT": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Mortgage Finance"},
        "AGNC": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Mortgage REITs"},
        "NLY": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Mortgage REITs"},
        "STWD": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Mortgage REITs"},
        "BXMT": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Mortgage REITs"},
        "MPW": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Health Care REITs"},
        "OHI": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Health Care REITs"},
        "HR": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Health Care REITs"},
        "CUBE": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Self Storage REITs"},
        "LSI": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Self Storage REITs"},
        "NSA": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Self Storage REITs"},
        "NNN": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Retail REITs"},
        "STOR": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Retail REITs"},
        "ADC": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Retail REITs"},
        "EPRT": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Retail REITs"},
        "BRX": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Retail REITs"},
        "ROIC": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Retail REITs"},
        "KRC": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Office REITs"},
        "HIW": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Office REITs"},
        "DEI": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Office REITs"},
        "CUZ": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Office REITs"},
        "FR": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Industrial REITs"},
        "REXR": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Industrial REITs"},
        "TRNO": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Industrial REITs"},
        "GLPI": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Specialized REITs"},
        # Communication - Mid/Small Cap
        "GOOGL": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Interactive Media"},
        "ZI": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Interactive Media"},
        "IAC": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Interactive Media"},
        "YELP": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Interactive Media"},
        "CARG": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Interactive Media"},
        "LUMN": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Telecommunications"},
        "FYBR": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Telecommunications"},
        "USM": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Telecommunications"},
        "SIRI": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Entertainment"},
        "LYV": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Entertainment"},
        "IMAX": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Entertainment"},
        "WMG": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Entertainment"},
        "EDR": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Entertainment"},
        # Utilities - Mid/Small Cap
        "WTRG": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Water Utilities"},
        "SWX": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Gas Utilities"},
        "OGS": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Gas Utilities"},
        "SR": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Gas Utilities"},
        "NWE": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Electric Utilities"},
        "AVA": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Electric Utilities"},
        "OGE": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Electric Utilities"},
        "POR": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Electric Utilities"},
        "IDA": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Electric Utilities"},
        "MGEE": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Electric Utilities"},
        "BKH": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Multi-Utilities"},
        "ALE": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Electric Utilities"},
        "VST": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Independent Power"},
        "CEG": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Independent Power"},
        # ============ ADDITIONAL ETFS ============
        # International ETFs
        "VEA": {"sector": "ETF", "gics_sector": "International ETF", "gics_industry": "Developed Markets"},
        "VWO": {"sector": "ETF", "gics_sector": "International ETF", "gics_industry": "Emerging Markets"},
        "IEFA": {"sector": "ETF", "gics_sector": "International ETF", "gics_industry": "Developed Markets"},
        "IEMG": {"sector": "ETF", "gics_sector": "International ETF", "gics_industry": "Emerging Markets"},
        "EWJ": {"sector": "ETF", "gics_sector": "International ETF", "gics_industry": "Japan"},
        "EWZ": {"sector": "ETF", "gics_sector": "International ETF", "gics_industry": "Brazil"},
        "EWT": {"sector": "ETF", "gics_sector": "International ETF", "gics_industry": "Taiwan"},
        "EWY": {"sector": "ETF", "gics_sector": "International ETF", "gics_industry": "South Korea"},
        "EWG": {"sector": "ETF", "gics_sector": "International ETF", "gics_industry": "Germany"},
        "EWU": {"sector": "ETF", "gics_sector": "International ETF", "gics_industry": "United Kingdom"},
        "KWEB": {"sector": "ETF", "gics_sector": "International ETF", "gics_industry": "China Internet"},
        "FXI": {"sector": "ETF", "gics_sector": "International ETF", "gics_industry": "China Large-Cap"},
        "MCHI": {"sector": "ETF", "gics_sector": "International ETF", "gics_industry": "China"},
        # Fixed Income ETFs
        "IEF": {"sector": "ETF", "gics_sector": "Fixed Income ETF", "gics_industry": "Intermediate Treasury"},
        "SHY": {"sector": "ETF", "gics_sector": "Fixed Income ETF", "gics_industry": "Short-Term Treasury"},
        "TIP": {"sector": "ETF", "gics_sector": "Fixed Income ETF", "gics_industry": "TIPS"},
        "VCSH": {"sector": "ETF", "gics_sector": "Fixed Income ETF", "gics_industry": "Short-Term Corporate"},
        "VCIT": {"sector": "ETF", "gics_sector": "Fixed Income ETF", "gics_industry": "Intermediate Corporate"},
        "JNK": {"sector": "ETF", "gics_sector": "Fixed Income ETF", "gics_industry": "High Yield Bond"},
        "EMB": {"sector": "ETF", "gics_sector": "Fixed Income ETF", "gics_industry": "Emerging Markets Bond"},
        "MUB": {"sector": "ETF", "gics_sector": "Fixed Income ETF", "gics_industry": "Municipal Bond"},
        "BNDX": {"sector": "ETF", "gics_sector": "Fixed Income ETF", "gics_industry": "International Bond"},
        "GOVT": {"sector": "ETF", "gics_sector": "Fixed Income ETF", "gics_industry": "Treasury Bond"},
        "SGOV": {"sector": "ETF", "gics_sector": "Fixed Income ETF", "gics_industry": "Short Treasury"},
        "BIL": {"sector": "ETF", "gics_sector": "Fixed Income ETF", "gics_industry": "T-Bill"},
        "FLOT": {"sector": "ETF", "gics_sector": "Fixed Income ETF", "gics_industry": "Floating Rate"},
        "SPSB": {"sector": "ETF", "gics_sector": "Fixed Income ETF", "gics_industry": "Short-Term Corporate"},
        "SPAB": {"sector": "ETF", "gics_sector": "Fixed Income ETF", "gics_industry": "Aggregate Bond"},
        # Thematic / Specialty ETFs
        "ARKK": {"sector": "ETF", "gics_sector": "Thematic ETF", "gics_industry": "Innovation"},
        "ARKG": {"sector": "ETF", "gics_sector": "Thematic ETF", "gics_industry": "Genomics"},
        "ARKW": {"sector": "ETF", "gics_sector": "Thematic ETF", "gics_industry": "Internet"},
        "ARKF": {"sector": "ETF", "gics_sector": "Thematic ETF", "gics_industry": "Fintech"},
        "SOXX": {"sector": "ETF", "gics_sector": "Sector ETF", "gics_industry": "Semiconductors"},
        "SMH": {"sector": "ETF", "gics_sector": "Sector ETF", "gics_industry": "Semiconductors"},
        "IGV": {"sector": "ETF", "gics_sector": "Sector ETF", "gics_industry": "Software"},
        "IBB": {"sector": "ETF", "gics_sector": "Sector ETF", "gics_industry": "Biotechnology"},
        "XBI": {"sector": "ETF", "gics_sector": "Sector ETF", "gics_industry": "Biotechnology"},
        "XOP": {"sector": "ETF", "gics_sector": "Sector ETF", "gics_industry": "Oil & Gas E&P"},
        "OIH": {"sector": "ETF", "gics_sector": "Sector ETF", "gics_industry": "Oil Services"},
        "AMLP": {"sector": "ETF", "gics_sector": "Sector ETF", "gics_industry": "MLPs"},
        "KBE": {"sector": "ETF", "gics_sector": "Sector ETF", "gics_industry": "Banks"},
        "KRE": {"sector": "ETF", "gics_sector": "Sector ETF", "gics_industry": "Regional Banks"},
        "VNQ": {"sector": "ETF", "gics_sector": "Sector ETF", "gics_industry": "Real Estate"},
        "IYR": {"sector": "ETF", "gics_sector": "Sector ETF", "gics_industry": "Real Estate"},
        "ITB": {"sector": "ETF", "gics_sector": "Sector ETF", "gics_industry": "Homebuilders"},
        "XHB": {"sector": "ETF", "gics_sector": "Sector ETF", "gics_industry": "Homebuilders"},
        "HACK": {"sector": "ETF", "gics_sector": "Sector ETF", "gics_industry": "Cybersecurity"},
        "CIBR": {"sector": "ETF", "gics_sector": "Sector ETF", "gics_industry": "Cybersecurity"},
        # Dividend / Income ETFs
        "VIG": {"sector": "ETF", "gics_sector": "Dividend ETF", "gics_industry": "Dividend Growth"},
        "VYM": {"sector": "ETF", "gics_sector": "Dividend ETF", "gics_industry": "High Dividend"},
        "SCHD": {"sector": "ETF", "gics_sector": "Dividend ETF", "gics_industry": "Dividend"},
        "HDV": {"sector": "ETF", "gics_sector": "Dividend ETF", "gics_industry": "High Dividend"},
        "DGRO": {"sector": "ETF", "gics_sector": "Dividend ETF", "gics_industry": "Dividend Growth"},
        "NOBL": {"sector": "ETF", "gics_sector": "Dividend ETF", "gics_industry": "Dividend Aristocrats"},
        "SDY": {"sector": "ETF", "gics_sector": "Dividend ETF", "gics_industry": "High Yield Dividend"},
        "DGRW": {"sector": "ETF", "gics_sector": "Dividend ETF", "gics_industry": "Dividend Growth"},
        "DVY": {"sector": "ETF", "gics_sector": "Dividend ETF", "gics_industry": "Dividend"},
        # Multi-Asset / Balanced ETFs
        "AOR": {"sector": "ETF", "gics_sector": "Multi-Asset ETF", "gics_industry": "Balanced Growth"},
        "AOA": {"sector": "ETF", "gics_sector": "Multi-Asset ETF", "gics_industry": "Aggressive Growth"},
        "AOM": {"sector": "ETF", "gics_sector": "Multi-Asset ETF", "gics_industry": "Moderate"},
        "AOK": {"sector": "ETF", "gics_sector": "Multi-Asset ETF", "gics_industry": "Conservative"},
        # Commodity ETFs
        "USO": {"sector": "ETF", "gics_sector": "Commodity ETF", "gics_industry": "Crude Oil"},
        "UNG": {"sector": "ETF", "gics_sector": "Commodity ETF", "gics_industry": "Natural Gas"},
        "DBA": {"sector": "ETF", "gics_sector": "Commodity ETF", "gics_industry": "Agriculture"},
        "DBC": {"sector": "ETF", "gics_sector": "Commodity ETF", "gics_industry": "Broad Commodities"},
        "PDBC": {"sector": "ETF", "gics_sector": "Commodity ETF", "gics_industry": "Broad Commodities"},
        "IAU": {"sector": "ETF", "gics_sector": "Commodity ETF", "gics_industry": "Gold"},
        "GLDM": {"sector": "ETF", "gics_sector": "Commodity ETF", "gics_industry": "Gold"},
        # Broad Market / Size ETFs
        "MDY": {"sector": "ETF", "gics_sector": "Broad Market ETF", "gics_industry": "S&P 400 MidCap"},
        "IJH": {"sector": "ETF", "gics_sector": "Broad Market ETF", "gics_industry": "S&P 400 MidCap"},
        "IJR": {"sector": "ETF", "gics_sector": "Broad Market ETF", "gics_industry": "S&P 600 SmallCap"},
        "RSP": {"sector": "ETF", "gics_sector": "Broad Market ETF", "gics_industry": "S&P 500 Equal Weight"},
        "SPLG": {"sector": "ETF", "gics_sector": "Broad Market ETF", "gics_industry": "S&P 500 Index"},
        "SPTM": {"sector": "ETF", "gics_sector": "Broad Market ETF", "gics_industry": "Total Market"},
        "ITOT": {"sector": "ETF", "gics_sector": "Broad Market ETF", "gics_industry": "Total Market"},
        "VXUS": {"sector": "ETF", "gics_sector": "International ETF", "gics_industry": "Total International"},
        "IXUS": {"sector": "ETF", "gics_sector": "International ETF", "gics_industry": "Total International"},
        "USMV": {"sector": "ETF", "gics_sector": "Factor ETF", "gics_industry": "Min Volatility"},
        # Leveraged / Inverse ETFs (common in active portfolios)
        "TQQQ": {"sector": "ETF", "gics_sector": "Leveraged ETF", "gics_industry": "3x Nasdaq 100"},
        "SQQQ": {"sector": "ETF", "gics_sector": "Leveraged ETF", "gics_industry": "-3x Nasdaq 100"},
        "UPRO": {"sector": "ETF", "gics_sector": "Leveraged ETF", "gics_industry": "3x S&P 500"},
        "SPXU": {"sector": "ETF", "gics_sector": "Leveraged ETF", "gics_industry": "-3x S&P 500"},
        "SH": {"sector": "ETF", "gics_sector": "Inverse ETF", "gics_industry": "-1x S&P 500"},
        "PSQ": {"sector": "ETF", "gics_sector": "Inverse ETF", "gics_industry": "-1x Nasdaq 100"},
        "TNA": {"sector": "ETF", "gics_sector": "Leveraged ETF", "gics_industry": "3x Russell 2000"},
        "TZA": {"sector": "ETF", "gics_sector": "Leveraged ETF", "gics_industry": "-3x Russell 2000"},
        # ============ ADDITIONAL INTERNATIONAL ADRS ============
        "TSM": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Semiconductors"},
        "BABA": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Internet Retail"},
        "JD": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Internet Retail"},
        "PDD": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Internet Retail"},
        "BIDU": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Interactive Media"},
        "NTES": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Interactive Media"},
        "TCEHY": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Interactive Media"},
        "SE": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Internet Services"},
        "MELI": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Internet Retail"},
        "NU": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Consumer Finance"},
        "INFY": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "IT Services"},
        "WIT": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "IT Services"},
        "HDB": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Banks"},
        "IBN": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Banks"},
        "UL": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Personal Products"},
        "DEO": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Beverages"},
        "BHP": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Metals & Mining"},
        "RIO": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Metals & Mining"},
        "VALE": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Metals & Mining"},
        "SCCO": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Metals & Mining"},
        "GOLD": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Gold Mining"},
        "NEM": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Gold Mining"},
        "AEM": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Gold Mining"},
        "WPM": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Gold Mining"},
        "FNV": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Gold Mining"},
        "TECK": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Metals & Mining"},
        "TM": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Automobiles"},
        "HMC": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Automobiles"},
        "SONY": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Consumer Electronics"},
        "SNE": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Consumer Electronics"},
        "SAP": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "SHOP": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "SU": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas"},
        "CNQ": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas"},
        "ENB": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas Midstream"},
        "TRP": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas Midstream"},
        "CP": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Railroads"},
        "CNI": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Railroads"},
        "TD": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Banks"},
        "RY": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Banks"},
        "BMO": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Banks"},
        "BNS": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Banks"},
        "CM": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Banks"},
        "MFC": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Insurance"},
        "SLF": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Insurance"},
        "AZN": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Pharmaceuticals"},
        "GSK": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Pharmaceuticals"},
        "SNY": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Pharmaceuticals"},
        "NVS": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Pharmaceuticals"},
        "RHHBY": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Pharmaceuticals"},
        "BAYRY": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Pharmaceuticals"},
        "TAK": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Pharmaceuticals"},
        "SMFG": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Banks"},
        "MUFG": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Banks"},
        "MFG": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Banks"},
        "KB": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Banks"},
        "SHG": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Insurance"},
        "ING": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Banks"},
        "DB": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Banks"},
        "CS": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Banks"},
        "UBS": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Banks"},
        "HSBC": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Banks"},
        "LYG": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Banks"},
        "BCS": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Banks"},
        "BP": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas"},
        "SHEL": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas"},
        "TTE": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas"},
        "EQNR": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas"},
        "E": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas"},
        "EC": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas"},
        "YPF": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas"},
        "SQM": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Chemicals"},
        "LI": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Automobiles"},
        "NIO": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Automobiles"},
        "XPEV": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Automobiles"},
        "GRAB": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Internet Services"},
        "GLBE": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "WIX": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "NICE": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "MNDY": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "ARM": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Semiconductors"},
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

        Tries providers in order: static mapping -> Tiingo -> yfinance -> Polygon -> IEX.
        Static mapping is checked first since it's instant and covers ~500+ tickers.

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

        # Try static mapping first (instant, no API calls needed)
        classification = self._fetch_from_static(ticker)
        if classification:
            return self._save_classification(security_id, classification, "static")

        # Try Tiingo (primary API source)
        if self.tiingo_client:
            classification = self._fetch_from_tiingo(ticker)
            if classification:
                return self._save_classification(security_id, classification, "tiingo")

        # Fallback to yfinance (broad coverage for US and international stocks)
        classification = self._fetch_from_yfinance(ticker)
        if classification:
            return self._save_classification(security_id, classification, "yfinance")

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

        # If the security looks like an option (has digits or special chars), classify as Options
        if security.is_option or self._looks_like_option(ticker):
            return self._save_classification(security_id, {
                "sector": "Options",
                "gics_sector": "Options",
                "gics_industry": "Equity Options",
                "market_cap_category": None,
            }, "inferred")

        logger.warning(f"Could not fetch classification for {ticker}")
        return None

    async def refresh_all_classifications(
        self, limit: Optional[int] = None, unclassified_only: bool = True
    ) -> Dict[str, Any]:
        """
        Refresh classifications for securities.

        By default, only processes securities that don't have a classification yet.
        Set unclassified_only=False to re-classify all securities.

        Args:
            limit: Maximum number of securities to refresh (None for all)
            unclassified_only: If True, only classify securities without existing classification

        Returns:
            Summary dict with success/failure counts
        """
        if unclassified_only:
            # Only get securities that don't have a classification yet
            classified_ids = self.db.query(SectorClassification.security_id).subquery()
            query = self.db.query(Security).filter(~Security.id.in_(classified_ids))
        else:
            query = self.db.query(Security)

        if limit:
            query = query.limit(limit)

        securities = query.all()

        total_securities = self.db.query(func.count(Security.id)).scalar() or 0
        already_classified = total_securities - len(securities) if unclassified_only else 0

        results = {
            "total": len(securities),
            "total_securities": total_securities,
            "already_classified": already_classified,
            "success": 0,
            "failed": 0,
            "errors": []
        }

        logger.info(f"Classifying {len(securities)} securities ({already_classified} already classified)")

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
                if len(results["errors"]) < 20:  # Cap error list size
                    results["errors"].append({"ticker": security.symbol, "error": str(e)})

        logger.info(
            f"Classification refresh complete: {results['success']}/{results['total']} successful, "
            f"{results['failed']} failed"
        )
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
        """Fallback to static sector mapping for common tickers.

        Tries multiple ticker variants (e.g., BRK.B, BRK-B, BRKB) to maximize matches.
        """
        # Try exact match first
        result = self.STATIC_MAPPING.get(ticker.upper())
        if result:
            return result

        # Try normalized form
        normalized = TickerNormalizer.normalize(ticker)
        result = self.STATIC_MAPPING.get(normalized)
        if result:
            return result

        # Try all known variants
        for variant in TickerNormalizer.get_variants(ticker):
            result = self.STATIC_MAPPING.get(variant)
            if result:
                return result

        # Try without dots/dashes (e.g., "BRKB" for "BRK.B")
        stripped = re.sub(r'[.\-/\s]', '', ticker.upper())
        result = self.STATIC_MAPPING.get(stripped)
        if result:
            return result

        return None

    def _fetch_from_yfinance(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Fetch classification from yfinance (broad coverage fallback).

        yfinance gets sector/industry data from Yahoo Finance, which covers
        most US and international securities.
        """
        try:
            import yfinance as yf
        except ImportError:
            return None

        try:
            normalized = ticker.replace('.', '-').upper()
            yf_ticker = yf.Ticker(normalized)
            info = yf_ticker.info

            if not info:
                return None

            sector = info.get('sector', '')
            industry = info.get('industry', '')

            if not sector and not industry:
                return None

            market_cap = info.get('marketCap')

            result = {
                "gics_sector": sector,
                "sector": SectorMapper.normalize_sector(sector) if sector else None,
                "gics_industry": industry,
                "market_cap_category": self._categorize_market_cap(market_cap),
            }
            logger.info(f"yfinance classification for {ticker}: sector={sector}, industry={industry}")
            return result

        except Exception as e:
            logger.debug(f"yfinance classification failed for {ticker}: {e}")
            return None

    @staticmethod
    def _looks_like_option(ticker: str) -> bool:
        """Check if a ticker looks like an options contract.

        Options tickers typically contain digits and are longer than normal tickers,
        e.g., 'AAPL240119C00190000' or 'AAPL 01/19/24 C190'.
        """
        if not ticker:
            return False
        # Options typically have digits embedded in the symbol and are long
        has_digits = any(c.isdigit() for c in ticker)
        is_long = len(ticker) > 8
        return has_digits and is_long

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

                # Find the right columns - State Street uses various naming conventions
                # Ticker/Symbol column
                ticker_col = None
                for col in ['Ticker', 'Symbol', 'ticker', 'symbol', 'TICKER', 'SYMBOL']:
                    if col in df.columns:
                        ticker_col = col
                        break

                # Weight column
                weight_col = None
                for col in ['Weight', 'Weight (%)', '% Weight', 'Percent', 'weight', 'WEIGHT']:
                    if col in df.columns:
                        weight_col = col
                        break

                # Sector column - State Street often uses "Sector" or "GICS Sector"
                sector_col = None
                for col in ['Sector', 'GICS Sector', 'GICS Sub-Industry', 'sector', 'SECTOR',
                           'Industry', 'industry', 'INDUSTRY']:
                    if col in df.columns:
                        sector_col = col
                        break

                logger.info(f"Found columns: ticker={ticker_col}, weight={weight_col}, sector={sector_col}")

                if not ticker_col or not weight_col:
                    # Try to infer columns from position
                    logger.warning("Could not find standard columns, using first columns")
                    cols = df.columns.tolist()
                    ticker_col = cols[0] if len(cols) > 0 else None
                    weight_col = cols[2] if len(cols) > 2 else None  # Usually Name is col 1

                holdings = []
                for _, row in df.iterrows():
                    ticker = row.get(ticker_col) if ticker_col else None
                    weight = row.get(weight_col) if weight_col else None
                    sector = row.get(sector_col) if sector_col else None

                    if pd.notna(ticker) and pd.notna(weight):
                        # Normalize weight - might be decimal or percentage
                        weight_val = float(weight) if isinstance(weight, (int, float)) else 0.0
                        if isinstance(weight, str):
                            weight_val = float(weight.strip('%').strip()) / 100.0 if '%' in weight else float(weight)

                        # Normalize sector name
                        # Note: State Street SPY Excel often uses "-" for missing sectors
                        sector_val = None
                        if pd.notna(sector) and str(sector).strip() and str(sector).strip() != '-':
                            sector_val = self._normalize_gics_sector(str(sector).strip())

                        holding_data = {
                            "ticker": TickerNormalizer.normalize(str(ticker)),
                            "weight": weight_val,
                        }
                        if sector_val:
                            holding_data["sector"] = sector_val
                        holdings.append(holding_data)

                logger.info(f"Parsed {len(holdings)} S&P 500 holdings from SPY")

                # Count how many have sectors from file
                with_sector = sum(1 for h in holdings if h.get("sector"))
                logger.info(f"  {with_sector}/{len(holdings)} have sector from Excel file")

                # Log unique sector values for debugging
                unique_sectors = set(h.get("sector") for h in holdings if h.get("sector"))
                logger.info(f"  Unique sectors found: {sorted(unique_sectors)}")

                # Also log sample of raw sector values from Excel
                raw_sectors = set()
                for _, row in df.head(20).iterrows():
                    if sector_col and pd.notna(row.get(sector_col)):
                        raw_sectors.add(str(row.get(sector_col)))
                logger.info(f"  Sample raw sectors from Excel: {sorted(raw_sectors)}")

                if len(holdings) == 0:
                    return {"success": False, "error": "No holdings parsed from Excel file"}

                return self._save_benchmark_holdings("SP500", holdings, self.SPY_HOLDINGS_URL)

        except Exception as e:
            logger.error(f"Failed to refresh S&P 500: {str(e)}", exc_info=True)
            return {"success": False, "error": str(e)}

    def _normalize_gics_sector(self, sector: str) -> str:
        """Normalize GICS sector names to consistent format"""
        # Map common variations to standard names
        sector_mapping = {
            # Standard GICS sectors
            "Information Technology": "Technology",
            "Info Tech": "Technology",
            "Tech": "Technology",
            "Health Care": "Healthcare",
            "HealthCare": "Healthcare",
            "Consumer Discretionary": "Consumer Discretionary",
            "Consumer Staples": "Consumer Staples",
            "Communication Services": "Communication",
            "Communications": "Communication",
            "Telecom": "Communication",
            "Telecommunication Services": "Communication",
            "Real Estate": "Real Estate",
            "Financials": "Financials",
            "Financial": "Financials",
            "Industrials": "Industrials",
            "Industrial": "Industrials",
            "Materials": "Materials",
            "Energy": "Energy",
            "Utilities": "Utilities",
        }

        # Check for exact match first
        if sector in sector_mapping:
            return sector_mapping[sector]

        # Check for partial match
        sector_lower = sector.lower()
        for key, value in sector_mapping.items():
            if key.lower() in sector_lower or sector_lower in key.lower():
                return value

        # Return as-is if no mapping found
        return sector

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
