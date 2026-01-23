"""
Data sourcing services for classifications, benchmark constituents, and factor returns.
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

from app.models.sector_models import SectorClassification, BenchmarkConstituent, FactorReturns
from app.models import Security
from app.utils.ticker_utils import TickerNormalizer, SectorMapper
import logging

logger = logging.getLogger(__name__)


class ClassificationService:
    """
    Service for fetching and storing security classifications from multiple sources.
    """

    def __init__(self, db: Session):
        self.db = db
        self.polygon_api_key = os.getenv("POLYGON_API_KEY")
        self.iex_api_key = os.getenv("IEX_API_KEY")

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

        # Try Polygon.io first
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
        # Basic static mapping for major tech/fin stocks
        STATIC_MAPPING = {
            "AAPL": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Technology Hardware"},
            "MSFT": {"sector": "Technology", "gics_sector": "Information Technology", "gics_industry": "Software"},
            "GOOGL": {"sector": "Communication", "gics_sector": "Communication Services", "gics_industry": "Interactive Media"},
            "AMZN": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Internet Retail"},
            "TSLA": {"sector": "Consumer Discretionary", "gics_sector": "Consumer Discretionary", "gics_industry": "Automobiles"},
            "JPM": {"sector": "Financials", "gics_sector": "Financials", "gics_industry": "Banking"},
            "JNJ": {"sector": "Healthcare", "gics_sector": "Health Care", "gics_industry": "Pharmaceuticals"},
        }

        normalized = TickerNormalizer.normalize(ticker)
        return STATIC_MAPPING.get(normalized)

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

                # Expecting columns: Ticker, Name, Weight, etc.
                holdings = []
                for _, row in df.iterrows():
                    ticker = row.get("Ticker") or row.get("Symbol")
                    weight = row.get("Weight") or row.get("% Weight")

                    if pd.notna(ticker) and pd.notna(weight):
                        holdings.append({
                            "ticker": TickerNormalizer.normalize(str(ticker)),
                            "weight": float(weight) if isinstance(weight, (int, float)) else float(weight.strip('%')) / 100.0,
                        })

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
