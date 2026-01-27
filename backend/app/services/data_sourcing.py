"""
Data sourcing services for classifications, benchmark constituents, and factor returns.
Updated: 2026-01-26
"""
import os
import httpx
import pandas as pd
import yfinance as yf
import asyncio
import io
import zipfile
from datetime import datetime, date, timedelta
from typing import Optional, Dict, List, Any
from sqlalchemy.orm import Session
from sqlalchemy import func

from app.models.sector_models import SectorClassification, BenchmarkConstituent, FactorReturns
from app.models import Security
from app.utils.ticker_utils import TickerNormalizer, SectorMapper
import logging

logger = logging.getLogger(__name__)


class ClassificationService:
    """
    Service for fetching and storing security classifications from multiple sources.
    Priority order: Static mapping (instant, no API) -> yfinance -> Polygon -> IEX
    """

    # Expanded static mapping for S&P 500 and common securities
    STATIC_MAPPING = {
        # Technology
        "AAPL": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Technology Hardware"},
        "MSFT": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "NVDA": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Semiconductors"},
        "AVGO": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Semiconductors"},
        "ORCL": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "CRM": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "CSCO": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Communications Equipment"},
        "ACN": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "IT Consulting"},
        "ADBE": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "IBM": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "IT Consulting"},
        "INTC": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Semiconductors"},
        "AMD": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Semiconductors"},
        "QCOM": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Semiconductors"},
        "TXN": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Semiconductors"},
        "NOW": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "INTU": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "AMAT": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Semiconductor Equipment"},
        "MU": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Semiconductors"},
        "LRCX": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Semiconductor Equipment"},
        "ADI": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Semiconductors"},
        "KLAC": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Semiconductor Equipment"},
        "SNPS": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "CDNS": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "PANW": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "MRVL": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Semiconductors"},
        "FTNT": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
        "MSI": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Communications Equipment"},
        "APH": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Electronic Equipment"},
        "NXPI": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Semiconductors"},
        "ON": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Semiconductors"},
        "GRAB": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Internet Services"},
        # Communication Services
        "GOOGL": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Interactive Media"},
        "GOOG": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Interactive Media"},
        "META": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Interactive Media"},
        "NFLX": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Entertainment"},
        "DIS": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Entertainment"},
        "CMCSA": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Cable & Satellite"},
        "VZ": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Telecom Services"},
        "T": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Telecom Services"},
        "TMUS": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Wireless Telecom"},
        "CHTR": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Cable & Satellite"},
        "EA": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Entertainment"},
        "WBD": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Entertainment"},
        "TTWO": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Entertainment"},
        # Consumer Discretionary
        "AMZN": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Internet Retail"},
        "TSLA": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Automobiles"},
        "HD": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Home Improvement Retail"},
        "MCD": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Restaurants"},
        "NKE": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Footwear"},
        "LOW": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Home Improvement Retail"},
        "SBUX": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Restaurants"},
        "TJX": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Apparel Retail"},
        "BKNG": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Hotels & Resorts"},
        "CMG": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Restaurants"},
        "ORLY": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Automotive Retail"},
        "AZO": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Automotive Retail"},
        "ROST": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Apparel Retail"},
        "MAR": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Hotels & Resorts"},
        "HLT": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Hotels & Resorts"},
        "GM": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Automobiles"},
        "F": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Automobiles"},
        "YUM": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Restaurants"},
        "DHI": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Homebuilding"},
        "LEN": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Homebuilding"},
        "CPNG": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Internet Retail"},
        # Consumer Staples
        "WMT": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Hypermarkets"},
        "PG": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Household Products"},
        "COST": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Hypermarkets"},
        "KO": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Soft Drinks"},
        "PEP": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Soft Drinks"},
        "PM": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Tobacco"},
        "MO": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Tobacco"},
        "MDLZ": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Packaged Foods"},
        "CL": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Household Products"},
        "KMB": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Household Products"},
        "GIS": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Packaged Foods"},
        "K": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Packaged Foods"},
        "SYY": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Food Distributors"},
        "STZ": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Distillers & Vintners"},
        "KR": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Food Retail"},
        "HSY": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Packaged Foods"},
        "KDP": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Soft Drinks"},
        "EL": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Personal Products"},
        "KVUE": {"sector": "Consumer Staples", "gics_sector": "Consumer Staples", "gics_industry": "Personal Products"},
        # Financials
        "JPM": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Diversified Banks"},
        "V": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Transaction & Payment Services"},
        "MA": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Transaction & Payment Services"},
        "BAC": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Diversified Banks"},
        "WFC": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Diversified Banks"},
        "GS": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Investment Banking"},
        "MS": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Investment Banking"},
        "BLK": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Asset Management"},
        "SCHW": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Investment Banking"},
        "AXP": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Consumer Finance"},
        "C": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Diversified Banks"},
        "SPGI": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Financial Exchanges & Data"},
        "CB": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Property & Casualty Insurance"},
        "MMC": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Insurance Brokers"},
        "PGR": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Property & Casualty Insurance"},
        "ICE": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Financial Exchanges & Data"},
        "CME": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Financial Exchanges & Data"},
        "AON": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Insurance Brokers"},
        "USB": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Regional Banks"},
        "PNC": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Regional Banks"},
        "TFC": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Regional Banks"},
        "AIG": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Multi-line Insurance"},
        "MET": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Life & Health Insurance"},
        "AFL": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Life & Health Insurance"},
        "PRU": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Life & Health Insurance"},
        "COF": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Consumer Finance"},
        "BK": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Asset Management & Custody"},
        "PYPL": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Transaction & Payment Services"},
        # Healthcare
        "JNJ": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Pharmaceuticals"},
        "UNH": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Managed Health Care"},
        "LLY": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Pharmaceuticals"},
        "PFE": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Pharmaceuticals"},
        "ABBV": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Pharmaceuticals"},
        "MRK": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Pharmaceuticals"},
        "TMO": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Life Sciences Tools"},
        "ABT": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Equipment"},
        "DHR": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Equipment"},
        "BMY": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Pharmaceuticals"},
        "AMGN": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Biotechnology"},
        "CVS": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Services"},
        "MDT": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Equipment"},
        "CI": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Managed Health Care"},
        "ELV": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Managed Health Care"},
        "ISRG": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Equipment"},
        "GILD": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Biotechnology"},
        "VRTX": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Biotechnology"},
        "SYK": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Equipment"},
        "REGN": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Biotechnology"},
        "BSX": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Equipment"},
        "ZTS": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Pharmaceuticals"},
        "BDX": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Equipment"},
        "HCA": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Facilities"},
        "MRNA": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Biotechnology"},
        "MCK": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Distributors"},
        "EW": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Equipment"},
        "A": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Life Sciences Tools"},
        "IQV": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Life Sciences Tools"},
        "HUM": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Managed Health Care"},
        "BIIB": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Biotechnology"},
        "IDXX": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Health Care Equipment"},
        # Industrials
        "CAT": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Construction Machinery"},
        "UNP": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Railroads"},
        "GE": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Aerospace & Defense"},
        "RTX": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Aerospace & Defense"},
        "HON": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Industrial Conglomerates"},
        "BA": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Aerospace & Defense"},
        "UPS": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Air Freight & Logistics"},
        "LMT": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Aerospace & Defense"},
        "DE": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Agricultural Machinery"},
        "ADP": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Human Resource Services"},
        "ETN": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Electrical Components"},
        "WM": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Environmental Services"},
        "ITW": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Industrial Machinery"},
        "FDX": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Air Freight & Logistics"},
        "NOC": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Aerospace & Defense"},
        "EMR": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Electrical Components"},
        "GD": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Aerospace & Defense"},
        "CSX": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Railroads"},
        "NSC": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Railroads"},
        "MMM": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Industrial Conglomerates"},
        "TT": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Building Products"},
        "JCI": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Building Products"},
        "PH": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Industrial Machinery"},
        "CARR": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Building Products"},
        "PCAR": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Trucking"},
        "RSG": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Environmental Services"},
        "LHX": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Aerospace & Defense"},
        "CMI": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Industrial Machinery"},
        "CTAS": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Diversified Support Services"},
        "FAST": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Building Products & Equipment"},
        "PAYX": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Human Resource Services"},
        "OTIS": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Industrial Machinery"},
        "TDY": {"sector": "Industrials", "gics_sector": "Industrials", "gics_industry": "Aerospace & Defense"},
        # Energy
        "XOM": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Integrated Oil & Gas"},
        "CVX": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Integrated Oil & Gas"},
        "COP": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas Exploration"},
        "SLB": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas Equipment"},
        "EOG": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas Exploration"},
        "MPC": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas Refining"},
        "PXD": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas Exploration"},
        "PSX": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas Refining"},
        "VLO": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas Refining"},
        "OXY": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas Exploration"},
        "WMB": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas Storage"},
        "HAL": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas Equipment"},
        "KMI": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas Storage"},
        "DVN": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas Exploration"},
        "HES": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas Exploration"},
        "BKR": {"sector": "Energy", "gics_sector": "Energy", "gics_industry": "Oil & Gas Equipment"},
        # Materials
        "LIN": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Industrial Gases"},
        "APD": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Industrial Gases"},
        "SHW": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Specialty Chemicals"},
        "FCX": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Copper"},
        "ECL": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Specialty Chemicals"},
        "NEM": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Gold"},
        "DOW": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Commodity Chemicals"},
        "DD": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Specialty Chemicals"},
        "NUE": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Steel"},
        "CTVA": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Fertilizers & Agri Chemicals"},
        "PPG": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Specialty Chemicals"},
        "VMC": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Construction Materials"},
        "MLM": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Construction Materials"},
        "ALB": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Specialty Chemicals"},
        "BALL": {"sector": "Materials", "gics_sector": "Materials", "gics_industry": "Metal & Glass Containers"},
        # Utilities
        "NEE": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Electric Utilities"},
        "DUK": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Electric Utilities"},
        "SO": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Electric Utilities"},
        "D": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Electric Utilities"},
        "SRE": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Multi-Utilities"},
        "AEP": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Electric Utilities"},
        "EXC": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Electric Utilities"},
        "XEL": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Electric Utilities"},
        "PCG": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Electric Utilities"},
        "ED": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Electric Utilities"},
        "WEC": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Electric Utilities"},
        "PEG": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Electric Utilities"},
        "AWK": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Water Utilities"},
        "ES": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Multi-Utilities"},
        "DTE": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Electric Utilities"},
        "EIX": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Electric Utilities"},
        "ETR": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Electric Utilities"},
        "FE": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Electric Utilities"},
        "PPL": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Electric Utilities"},
        "CMS": {"sector": "Utilities", "gics_sector": "Utilities", "gics_industry": "Multi-Utilities"},
        # Real Estate
        "AMT": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Telecom Tower REITs"},
        "PLD": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Industrial REITs"},
        "CCI": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Telecom Tower REITs"},
        "EQIX": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Data Center REITs"},
        "PSA": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Self Storage REITs"},
        "SBAC": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Telecom Tower REITs"},
        "O": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Retail REITs"},
        "WELL": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Health Care REITs"},
        "DLR": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Data Center REITs"},
        "SPG": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Retail REITs"},
        "VICI": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Diversified REITs"},
        "AVB": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Residential REITs"},
        "EQR": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Residential REITs"},
        "ARE": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Office REITs"},
        "EXR": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Self Storage REITs"},
        "CBRE": {"sector": "Real Estate", "gics_sector": "Real Estate", "gics_industry": "Real Estate Services"},
        # ETFs (common factor ETFs)
        "SPY": {"sector": "ETF", "gics_sector": "ETF", "gics_industry": "Index Fund"},
        "QQQ": {"sector": "ETF", "gics_sector": "ETF", "gics_industry": "Index Fund"},
        "IWM": {"sector": "ETF", "gics_sector": "ETF", "gics_industry": "Index Fund"},
        "VTI": {"sector": "ETF", "gics_sector": "ETF", "gics_industry": "Index Fund"},
        "VOO": {"sector": "ETF", "gics_sector": "ETF", "gics_industry": "Index Fund"},
        "IVV": {"sector": "ETF", "gics_sector": "ETF", "gics_industry": "Index Fund"},
        "VEA": {"sector": "ETF", "gics_sector": "ETF", "gics_industry": "International Fund"},
        "VWO": {"sector": "ETF", "gics_sector": "ETF", "gics_industry": "Emerging Markets Fund"},
        "EFA": {"sector": "ETF", "gics_sector": "ETF", "gics_industry": "International Fund"},
        "AGG": {"sector": "ETF", "gics_sector": "ETF", "gics_industry": "Bond Fund"},
        "BND": {"sector": "ETF", "gics_sector": "ETF", "gics_industry": "Bond Fund"},
        "LQD": {"sector": "ETF", "gics_sector": "ETF", "gics_industry": "Bond Fund"},
        "GLD": {"sector": "ETF", "gics_sector": "ETF", "gics_industry": "Commodity Fund"},
        "SLV": {"sector": "ETF", "gics_sector": "ETF", "gics_industry": "Commodity Fund"},
        "XLK": {"sector": "ETF", "gics_sector": "ETF", "gics_industry": "Sector Fund"},
        "XLF": {"sector": "ETF", "gics_sector": "ETF", "gics_industry": "Sector Fund"},
        "XLV": {"sector": "ETF", "gics_sector": "ETF", "gics_industry": "Sector Fund"},
        "XLE": {"sector": "ETF", "gics_sector": "ETF", "gics_industry": "Sector Fund"},
        "XLI": {"sector": "ETF", "gics_sector": "ETF", "gics_industry": "Sector Fund"},
        "XLY": {"sector": "ETF", "gics_sector": "ETF", "gics_industry": "Sector Fund"},
        "XLP": {"sector": "ETF", "gics_sector": "ETF", "gics_industry": "Sector Fund"},
        "XLU": {"sector": "ETF", "gics_sector": "ETF", "gics_industry": "Sector Fund"},
        "XLB": {"sector": "ETF", "gics_sector": "ETF", "gics_industry": "Sector Fund"},
        "XLRE": {"sector": "ETF", "gics_sector": "ETF", "gics_industry": "Sector Fund"},
        "XLC": {"sector": "ETF", "gics_sector": "ETF", "gics_industry": "Sector Fund"},
        "VTV": {"sector": "ETF", "gics_sector": "ETF", "gics_industry": "Style Fund"},
        "VUG": {"sector": "ETF", "gics_sector": "ETF", "gics_industry": "Style Fund"},
        "IWF": {"sector": "ETF", "gics_sector": "ETF", "gics_industry": "Style Fund"},
        "IWD": {"sector": "ETF", "gics_sector": "ETF", "gics_industry": "Style Fund"},
        "MTUM": {"sector": "ETF", "gics_sector": "ETF", "gics_industry": "Factor Fund"},
        "QUAL": {"sector": "ETF", "gics_sector": "ETF", "gics_industry": "Factor Fund"},
        "VLUE": {"sector": "ETF", "gics_sector": "ETF", "gics_industry": "Factor Fund"},
        "SIZE": {"sector": "ETF", "gics_sector": "ETF", "gics_industry": "Factor Fund"},
        "USMV": {"sector": "ETF", "gics_sector": "ETF", "gics_industry": "Factor Fund"},
        "IVE": {"sector": "ETF", "gics_sector": "ETF", "gics_industry": "Style Fund"},
        "IVW": {"sector": "ETF", "gics_sector": "ETF", "gics_industry": "Style Fund"},
        "SPLV": {"sector": "ETF", "gics_sector": "ETF", "gics_industry": "Factor Fund"},
    }

    def __init__(self, db: Session):
        self.db = db
        self.polygon_api_key = os.getenv("POLYGON_API_KEY")
        self.iex_api_key = os.getenv("IEX_API_KEY")
        self.request_delay = 2.0  # Delay between API requests to avoid rate limiting

    async def refresh_classification(self, security_id: int) -> Optional[Dict[str, Any]]:
        """
        Refresh classification for a single security.
        Priority: Static mapping (instant) -> yfinance -> Polygon -> IEX

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
        normalized_ticker = TickerNormalizer.normalize(ticker)
        logger.info(f"Refreshing classification for {ticker}")

        # 1. Try static mapping FIRST (instant, no API calls, no rate limits)
        classification = self._fetch_from_static(normalized_ticker)
        if classification:
            logger.info(f"Found {ticker} in static mapping")
            return self._save_classification(security_id, classification, "static")

        # 2. Fallback to yfinance (free, but rate limited)
        await asyncio.sleep(self.request_delay)  # Rate limiting
        classification = self._fetch_from_yfinance(ticker)
        if classification:
            return self._save_classification(security_id, classification, "yfinance")

        # 3. Fallback to Polygon.io
        if self.polygon_api_key:
            await asyncio.sleep(self.request_delay)  # Rate limiting
            classification = await self._fetch_from_polygon(ticker)
            if classification:
                return self._save_classification(security_id, classification, "polygon")

        # 4. Fallback to IEX Cloud
        if self.iex_api_key:
            await asyncio.sleep(self.request_delay)  # Rate limiting
            classification = await self._fetch_from_iex(ticker)
            if classification:
                return self._save_classification(security_id, classification, "iex")

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
        results = {"total": len(securities), "success": 0, "failed": 0, "errors": [], "sources": {}}

        for i, security in enumerate(securities):
            try:
                # Log progress every 10 securities
                if i > 0 and i % 10 == 0:
                    logger.info(f"Classification progress: {i}/{len(securities)} ({results['success']} success, {results['failed']} failed)")

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

    def _fetch_from_yfinance(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Fetch classification from yfinance (free, no API key required)"""
        try:
            stock = yf.Ticker(ticker)
            info = stock.info

            if not info:
                logger.warning(f"yfinance returned no info for {ticker}")
                return None

            sector = info.get("sector", "")
            industry = info.get("industry", "")
            market_cap = info.get("marketCap")

            if not sector and not industry:
                logger.warning(f"yfinance has no sector/industry for {ticker}")
                return None

            return {
                "gics_sector": sector,
                "sector": SectorMapper.normalize_sector(sector) if sector else None,
                "gics_industry": industry,
                "market_cap_category": self._categorize_market_cap(market_cap),
            }
        except Exception as e:
            logger.warning(f"yfinance fetch failed for {ticker}: {str(e)}")
            return None

    async def _fetch_from_polygon(self, ticker: str) -> Optional[Dict[str, Any]]:
        """Fetch classification from Polygon.io"""
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
            logger.warning(f"Fetching S&P 500 holdings from SPY ETF: {self.SPY_HOLDINGS_URL}")
            async with httpx.AsyncClient(timeout=60.0, follow_redirects=True) as client:
                response = await client.get(self.SPY_HOLDINGS_URL)
                logger.warning(f"SPY response status: {response.status_code}")
                response.raise_for_status()

                # Parse Excel file - try different skip rows
                df = pd.read_excel(io.BytesIO(response.content), skiprows=4)
                logger.warning(f"SPY DataFrame columns: {df.columns.tolist()}")
                logger.warning(f"SPY DataFrame shape: {df.shape}")
                logger.warning(f"SPY first 5 rows:\n{df.head(5).to_string()}")

                # Find the correct column names (case-insensitive)
                columns_lower = {col.lower().strip(): col for col in df.columns}
                ticker_col = columns_lower.get('ticker') or columns_lower.get('symbol') or columns_lower.get('name')
                weight_col = columns_lower.get('weight') or columns_lower.get('% weight') or columns_lower.get('weight (%)')

                logger.warning(f"Using columns - ticker: {ticker_col}, weight: {weight_col}")

                if not ticker_col or not weight_col:
                    return {"success": False, "error": f"Could not find ticker/weight columns. Available: {df.columns.tolist()}"}

                # Build holdings dict to deduplicate by ticker
                holdings_dict = {}
                raw_count = 0
                for _, row in df.iterrows():
                    ticker = row.get(ticker_col)
                    weight = row.get(weight_col)

                    if pd.notna(ticker) and pd.notna(weight):
                        raw_count += 1
                        ticker_str = str(ticker).strip()
                        normalized_ticker = TickerNormalizer.normalize(ticker_str)

                        # Skip non-equity entries
                        if normalized_ticker in ['USD', 'CASH', '', 'NAN']:
                            continue

                        # Parse weight - State Street file uses percentages (7.7 = 7.7%)
                        # Always divide by 100 to convert to decimal
                        if isinstance(weight, str):
                            weight_val = float(weight.strip('%')) / 100.0
                        else:
                            weight_val = float(weight) / 100.0

                        # Only keep the first occurrence of each ticker (dedup)
                        if normalized_ticker not in holdings_dict:
                            sector_info = ClassificationService.STATIC_MAPPING.get(normalized_ticker, {})
                            sector = sector_info.get("sector") if sector_info else None

                            # If not in static mapping, try SectorClassification table
                            if not sector:
                                security = self.db.query(Security).filter(
                                    Security.symbol == normalized_ticker
                                ).first()
                                if security:
                                    classification = self.db.query(SectorClassification).filter(
                                        SectorClassification.security_id == security.id
                                    ).first()
                                    if classification and classification.sector:
                                        sector = classification.sector

                            holdings_dict[normalized_ticker] = {
                                "ticker": normalized_ticker,
                                "weight": weight_val,
                                "sector": sector,
                            }

                holdings = list(holdings_dict.values())
                total_weight = sum(h["weight"] for h in holdings)

                logger.warning(f"Raw rows with data: {raw_count}, Unique tickers: {len(holdings)}, Total weight: {total_weight:.4f}")

                # If total is way off, the file format might be different
                if total_weight < 0.5 or total_weight > 1.5:
                    logger.warning(f"Total weight {total_weight:.4f} is outside expected range [0.5, 1.5]")
                    # Log sample of what we parsed
                    sample = list(holdings_dict.items())[:5]
                    logger.warning(f"Sample holdings: {sample}")

                if len(holdings) == 0:
                    return {"success": False, "error": "No holdings parsed from Excel file"}

                return self._save_benchmark_holdings("SP500", holdings, self.SPY_HOLDINGS_URL)

        except Exception as e:
            logger.error(f"Failed to refresh S&P 500: {str(e)}", exc_info=True)
            return {"success": False, "error": str(e)}

    def _save_benchmark_holdings(
        self, benchmark_code: str, holdings: List[Dict[str, Any]], source_url: str
    ) -> Dict[str, Any]:
        """Save benchmark holdings to database"""
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

            # Insert new holdings
            for holding in holdings:
                constituent = BenchmarkConstituent(
                    benchmark_code=benchmark_code,
                    symbol=holding["ticker"],
                    weight=holding["weight"],
                    sector=holding.get("sector"),  # Store sector from static mapping
                    as_of_date=as_of,
                    source_url=source_url,
                )
                self.db.add(constituent)

            self.db.commit()

            logger.info(f"Saved {len(holdings)} holdings for {benchmark_code}")
            return {
                "success": True,
                "benchmark": benchmark_code,
                "count": len(holdings),
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
