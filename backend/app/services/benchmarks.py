import pandas as pd
import numpy as np
from typing import List, Dict, Optional
from datetime import date, timedelta
from sqlalchemy.orm import Session
from sqlalchemy import and_
from app.models import (
    BenchmarkDefinition, BenchmarkLevel, BenchmarkReturn,
    BenchmarkMetric, ReturnsEOD, ViewType
)
import logging

logger = logging.getLogger(__name__)


class BenchmarksEngine:
    """Manages benchmark data and metrics"""

    def __init__(self, db: Session):
        self.db = db

    def ensure_default_benchmarks(self):
        """Ensure default benchmarks exist"""
        defaults = [
            {'code': 'SPY', 'name': 'S&P 500 (SPY)', 'provider_symbol': 'SPY.US'},
            {'code': 'QQQ', 'name': 'Nasdaq 100 (QQQ)', 'provider_symbol': 'QQQ.US'},
            {'code': 'INDU', 'name': 'Dow Jones Industrial Average', 'provider_symbol': '^DJI'},
        ]

        for bench in defaults:
            existing = self.db.query(BenchmarkDefinition).filter(
                BenchmarkDefinition.code == bench['code']
            ).first()

            if not existing:
                benchmark = BenchmarkDefinition(**bench)
                self.db.add(benchmark)

        self.db.commit()

    def compute_benchmark_returns(self, benchmark_code: str) -> int:
        """Compute daily returns for a benchmark"""
        levels = self.db.query(BenchmarkLevel).filter(
            BenchmarkLevel.code == benchmark_code
        ).order_by(BenchmarkLevel.date).all()

        if len(levels) < 2:
            return 0

        count = 0
        for i in range(1, len(levels)):
            prev_level = levels[i-1].level
            curr_level = levels[i].level
            curr_date = levels[i].date

            if prev_level > 0:
                ret = (curr_level / prev_level) - 1

                existing = self.db.query(BenchmarkReturn).filter(
                    and_(
                        BenchmarkReturn.code == benchmark_code,
                        BenchmarkReturn.date == curr_date
                    )
                ).first()

                if existing:
                    if existing.return_value != ret:
                        existing.return_value = ret
                else:
                    benchmark_return = BenchmarkReturn(
                        code=benchmark_code,
                        date=curr_date,
                        return_value=ret
                    )
                    self.db.add(benchmark_return)
                    count += 1

        self.db.commit()
        logger.info(f"Created {count} returns for benchmark {benchmark_code}")
        return count

    def compute_benchmark_metrics(
        self,
        view_type: ViewType,
        view_id: int,
        benchmark_code: str,
        as_of_date: date,
        window: int = 252
    ) -> Optional[Dict]:
        """
        Compute benchmark metrics: beta, alpha, tracking error, correlation
        Uses trailing 'window' trading days
        """
        # Get portfolio returns
        portfolio_returns = self.db.query(ReturnsEOD).filter(
            and_(
                ReturnsEOD.view_type == view_type,
                ReturnsEOD.view_id == view_id,
                ReturnsEOD.date <= as_of_date
            )
        ).order_by(ReturnsEOD.date.desc()).limit(window).all()

        if not portfolio_returns:
            return None

        # Get benchmark returns for same dates
        dates = [r.date for r in portfolio_returns]
        benchmark_returns = self.db.query(BenchmarkReturn).filter(
            and_(
                BenchmarkReturn.code == benchmark_code,
                BenchmarkReturn.date.in_(dates)
            )
        ).all()

        if not benchmark_returns:
            return None

        # Convert to DataFrames
        port_df = pd.DataFrame([
            {'date': r.date, 'port_return': r.twr_return}
            for r in portfolio_returns
        ])

        bench_df = pd.DataFrame([
            {'date': r.date, 'bench_return': r.return_value}
            for r in benchmark_returns
        ])

        # Merge
        merged = port_df.merge(bench_df, on='date', how='inner')

        if len(merged) < 20:  # Need minimum observations
            return None

        # Compute metrics
        port_returns = merged['port_return'].values
        bench_returns = merged['bench_return'].values

        # Beta (using OLS)
        covariance = np.cov(port_returns, bench_returns)[0, 1]
        benchmark_variance = np.var(bench_returns)
        beta = covariance / benchmark_variance if benchmark_variance > 0 else None

        # Alpha (annualized)
        avg_port_return = np.mean(port_returns)
        avg_bench_return = np.mean(bench_returns)
        if beta is not None:
            alpha = (avg_port_return - beta * avg_bench_return) * 252
        else:
            alpha = None

        # Tracking error (annualized)
        active_returns = port_returns - bench_returns
        te = np.std(active_returns) * np.sqrt(252)

        # Correlation
        corr = np.corrcoef(port_returns, bench_returns)[0, 1]

        metrics = {
            'beta_252': beta,
            'alpha_252': alpha,
            'te_252': te,
            'corr_252': corr
        }

        # Store metrics
        existing = self.db.query(BenchmarkMetric).filter(
            and_(
                BenchmarkMetric.view_type == view_type,
                BenchmarkMetric.view_id == view_id,
                BenchmarkMetric.benchmark_code == benchmark_code,
                BenchmarkMetric.as_of_date == as_of_date
            )
        ).first()

        if existing:
            for key, value in metrics.items():
                setattr(existing, key, value)
        else:
            metric = BenchmarkMetric(
                view_type=view_type,
                view_id=view_id,
                benchmark_code=benchmark_code,
                as_of_date=as_of_date,
                **metrics
            )
            self.db.add(metric)

        self.db.commit()

        return metrics

    def compute_all_benchmark_returns(self) -> Dict[str, int]:
        """Compute returns for all benchmarks"""
        self.ensure_default_benchmarks()

        benchmarks = self.db.query(BenchmarkDefinition).all()

        results = {
            'total_benchmarks': len(benchmarks),
            'updated': 0,
            'failed': 0
        }

        for benchmark in benchmarks:
            try:
                count = self.compute_benchmark_returns(benchmark.code)
                if count > 0:
                    results['updated'] += 1
            except Exception as e:
                logger.error(f"Failed to compute returns for {benchmark.code}: {e}")
                results['failed'] += 1

        return results
