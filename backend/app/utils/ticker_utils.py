"""
Ticker normalization and utility functions for data sourcing.
"""
import re
from typing import Dict, List, Tuple


class TickerNormalizer:
    """
    Normalizes ticker symbols across different data sources and formats.
    """

    # Common ticker transformations
    SPECIAL_CASES: Dict[str, List[str]] = {
        "BRK.B": ["BRK-B", "BRK/B", "BRK B"],
        "BRK.A": ["BRK-A", "BRK/A", "BRK A"],
        "BF.B": ["BF-B", "BF/B", "BF B"],
        "BF.A": ["BF-A", "BF/A", "BF A"],
    }

    @staticmethod
    def normalize(ticker: str) -> str:
        """
        Normalize a ticker symbol to a standard format.

        Args:
            ticker: Raw ticker symbol

        Returns:
            Normalized ticker symbol (uppercase, with dots for share classes)
        """
        if not ticker:
            return ""

        # Uppercase and strip
        ticker = ticker.upper().strip()

        # Replace common separators with dots
        ticker = ticker.replace("-", ".").replace("/", ".").replace(" ", ".")

        # Remove multiple consecutive dots
        ticker = re.sub(r'\.+', '.', ticker)

        # Remove trailing dots
        ticker = ticker.rstrip('.')

        return ticker

    @staticmethod
    def get_variants(ticker: str) -> List[str]:
        """
        Get all known variants of a ticker symbol.

        Args:
            ticker: Normalized ticker symbol

        Returns:
            List of ticker variants
        """
        normalized = TickerNormalizer.normalize(ticker)
        variants = [normalized]

        # Check if this is a known special case
        for canonical, alternates in TickerNormalizer.SPECIAL_CASES.items():
            if normalized == canonical or normalized in alternates:
                variants.extend([canonical] + alternates)
                break

        # Add common format variations
        if "." in normalized:
            # Add dash version
            variants.append(normalized.replace(".", "-"))
            # Add slash version
            variants.append(normalized.replace(".", "/"))
            # Add space version
            variants.append(normalized.replace(".", " "))

        # Remove duplicates and return
        return list(set(variants))

    @staticmethod
    def match_tickers(ticker1: str, ticker2: str) -> bool:
        """
        Check if two tickers represent the same security.

        Args:
            ticker1: First ticker
            ticker2: Second ticker

        Returns:
            True if tickers match
        """
        norm1 = TickerNormalizer.normalize(ticker1)
        norm2 = TickerNormalizer.normalize(ticker2)

        if norm1 == norm2:
            return True

        # Check if either is in the other's variants
        variants1 = TickerNormalizer.get_variants(norm1)
        variants2 = TickerNormalizer.get_variants(norm2)

        return bool(set(variants1) & set(variants2))


class SectorMapper:
    """
    Maps between different sector classification schemes.
    """

    # Mapping from various sector names to simplified categories
    SECTOR_MAPPING: Dict[str, str] = {
        # Technology
        "Information Technology": "Technology",
        "Technology": "Technology",
        "Software": "Technology",
        "Hardware": "Technology",
        "Semiconductors": "Technology",
        "IT Services": "Technology",

        # Healthcare
        "Health Care": "Healthcare",
        "Healthcare": "Healthcare",
        "Pharmaceuticals": "Healthcare",
        "Biotechnology": "Healthcare",
        "Medical Devices": "Healthcare",

        # Financials
        "Financials": "Financials",
        "Banking": "Financials",
        "Insurance": "Financials",
        "Capital Markets": "Financials",

        # Consumer
        "Consumer Discretionary": "Consumer Discretionary",
        "Consumer Staples": "Consumer Staples",
        "Retail": "Consumer Discretionary",

        # Industrials
        "Industrials": "Industrials",
        "Industrial": "Industrials",
        "Manufacturing": "Industrials",

        # Energy
        "Energy": "Energy",
        "Oil & Gas": "Energy",

        # Materials
        "Materials": "Materials",
        "Basic Materials": "Materials",

        # Communication
        "Communication Services": "Communication",
        "Telecommunications": "Communication",
        "Media": "Communication",

        # Real Estate
        "Real Estate": "Real Estate",

        # Utilities
        "Utilities": "Utilities",
    }

    @staticmethod
    def normalize_sector(sector: str) -> str:
        """
        Normalize a sector name to a standard category.

        Args:
            sector: Raw sector name

        Returns:
            Normalized sector name
        """
        if not sector:
            return "Other"

        sector = sector.strip()

        # Direct match
        if sector in SectorMapper.SECTOR_MAPPING:
            return SectorMapper.SECTOR_MAPPING[sector]

        # Case-insensitive partial match
        sector_lower = sector.lower()
        for key, value in SectorMapper.SECTOR_MAPPING.items():
            if key.lower() in sector_lower or sector_lower in key.lower():
                return value

        return sector  # Return original if no mapping found


def parse_csv_line(line: str, delimiter: str = ",") -> List[str]:
    """
    Parse a CSV line, handling quoted fields correctly.

    Args:
        line: CSV line to parse
        delimiter: Field delimiter

    Returns:
        List of fields
    """
    import csv
    import io

    reader = csv.reader(io.StringIO(line), delimiter=delimiter)
    return next(reader)
