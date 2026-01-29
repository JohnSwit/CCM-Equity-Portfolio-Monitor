import { useState, useEffect, useMemo } from 'react';
import Layout from '@/components/Layout';
import { api } from '@/lib/api';
import { format, subDays, subMonths, startOfYear } from 'date-fns';
import Select from 'react-select';

// Brinson time period options
type BrinsonPeriod = '1M' | '3M' | 'YTD' | '1Y' | 'custom';

const getBrinsonDateRange = (period: BrinsonPeriod): { start: Date; end: Date } => {
  const end = new Date();
  let start: Date;

  switch (period) {
    case '1M':
      start = subMonths(end, 1);
      break;
    case '3M':
      start = subMonths(end, 3);
      break;
    case 'YTD':
      start = startOfYear(end);
      break;
    case '1Y':
      start = subMonths(end, 12);
      break;
    default:
      start = subMonths(end, 3);
  }

  return { start, end };
};

export default function PortfolioStatisticsPage() {
  const [views, setViews] = useState<any[]>([]);
  const [selectedView, setSelectedView] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [benchmark, setBenchmark] = useState('SP500');
  const [window, setWindow] = useState(252);

  // Statistics data
  const [contributionData, setContributionData] = useState<any>(null);
  const [volatilityData, setVolatilityData] = useState<any>(null);
  const [drawdownData, setDrawdownData] = useState<any>(null);
  const [varData, setVarData] = useState<any>(null);
  const [factorData, setFactorData] = useState<any>(null);

  // Phase 2 data
  const [turnoverData, setTurnoverData] = useState<any>(null);
  const [sectorData, setSectorData] = useState<any>(null);
  const [sectorComparisonData, setSectorComparisonData] = useState<any>(null);
  const [brinsonData, setBrinsonData] = useState<any>(null);
  const [factorAttributionData, setFactorAttributionData] = useState<any>(null);

  // Enhanced Factor Analysis
  const [factorRiskData, setFactorRiskData] = useState<any>(null);
  const [historicalFactorData, setHistoricalFactorData] = useState<any>(null);

  // Brinson time period
  const [brinsonPeriod, setBrinsonPeriod] = useState<BrinsonPeriod>('3M');
  const [brinsonLoading, setBrinsonLoading] = useState(false);
  const [expandedSectors, setExpandedSectors] = useState<Set<string>>(new Set());

  // Data status
  const [dataStatus, setDataStatus] = useState<any>(null);
  const [refreshingData, setRefreshingData] = useState<string | null>(null);

  useEffect(() => {
    loadViews();
    loadDataStatus();
  }, []);

  useEffect(() => {
    if (selectedView) {
      loadStatistics();
    }
  }, [selectedView, benchmark, window]);

  // Reload Brinson when period changes
  useEffect(() => {
    if (selectedView) {
      loadBrinsonData();
    }
  }, [selectedView, brinsonPeriod]);

  const loadBrinsonData = async () => {
    if (!selectedView) return;

    setBrinsonLoading(true);
    try {
      const { start, end } = getBrinsonDateRange(brinsonPeriod);
      const brinson = await api.getBrinsonAttribution(
        selectedView.view_type,
        selectedView.view_id,
        'SP500',
        format(start, 'yyyy-MM-dd'),
        format(end, 'yyyy-MM-dd')
      );
      setBrinsonData(brinson);
    } catch (error) {
      console.error('Failed to load Brinson data:', error);
    } finally {
      setBrinsonLoading(false);
    }
  };

  const loadViews = async () => {
    try {
      const data = await api.getAllViews();
      setViews(data);
      if (data.length > 0) {
        const firmView = data.find((v: any) => v.view_type === 'firm');
        setSelectedView(firmView || data[0]);
      }
    } catch (error) {
      console.error('Failed to load views:', error);
    }
  };

  const loadStatistics = async () => {
    if (!selectedView) return;

    setLoading(true);
    try {
      const [contrib, vol, dd, varCvar, factors, turnover, sectors, sectorComp, factorAttr, factorRisk, histFactors] = await Promise.all([
        api.getContributionToReturns(selectedView.view_type, selectedView.view_id, undefined, undefined, 20).catch(() => null),
        api.getVolatilityMetrics(selectedView.view_type, selectedView.view_id, benchmark, window).catch(() => null),
        api.getDrawdownAnalysis(selectedView.view_type, selectedView.view_id).catch(() => null),
        api.getVarCvar(selectedView.view_type, selectedView.view_id, '95,99', window).catch(() => null),
        api.getFactorAnalysis(selectedView.view_type, selectedView.view_id).catch(() => null),
        // Phase 2
        api.getTurnoverAnalysis(selectedView.view_type, selectedView.view_id, undefined, undefined, 'monthly').catch(() => null),
        api.getSectorWeights(selectedView.view_type, selectedView.view_id).catch(() => null),
        api.getSectorComparison(selectedView.view_type, selectedView.view_id, 'SP500').catch(() => null),
        // Note: Brinson is loaded separately with time period via loadBrinsonData
        api.getFactorAttribution(selectedView.view_type, selectedView.view_id).catch(() => null),
        // Enhanced Factor Analysis
        api.getFactorRiskDecomposition(selectedView.view_type, selectedView.view_id).catch(() => null),
        api.getHistoricalFactorExposures(selectedView.view_type, selectedView.view_id).catch(() => null),
      ]);

      setContributionData(contrib);
      setVolatilityData(vol);
      setDrawdownData(dd);
      setVarData(varCvar);
      setFactorData(factors);
      setTurnoverData(turnover);
      setSectorData(sectors);
      setSectorComparisonData(sectorComp);
      setFactorAttributionData(factorAttr);
      setFactorRiskData(factorRisk);
      setHistoricalFactorData(histFactors);
    } catch (error) {
      console.error('Failed to load statistics:', error);
    } finally {
      setLoading(false);
    }
  };

  const loadDataStatus = async () => {
    try {
      const status = await api.getDataStatus();
      setDataStatus(status);
    } catch (error) {
      console.error('Failed to load data status:', error);
    }
  };

  const handleRefreshClassifications = async () => {
    setRefreshingData('classifications');
    try {
      const result = await api.refreshClassifications();
      alert(`Classification refresh complete: ${result.success}/${result.total} succeeded`);
      loadDataStatus();
      if (selectedView) loadStatistics();
    } catch (error) {
      console.error('Failed to refresh classifications:', error);
      alert('Failed to refresh classifications. See console for details.');
    } finally {
      setRefreshingData(null);
    }
  };

  const handleRefreshBenchmarks = async () => {
    setRefreshingData('benchmarks');
    try {
      const result = await api.refreshSP500Benchmark();
      alert(`S&P 500 refreshed: ${result.count} constituents loaded`);
      loadDataStatus();
      if (selectedView) loadStatistics();
    } catch (error) {
      console.error('Failed to refresh S&P 500:', error);
      alert('Failed to refresh S&P 500. See console for details.');
    } finally {
      setRefreshingData(null);
    }
  };

  const handleRefreshFactors = async () => {
    setRefreshingData('factors');
    try {
      const result = await api.refreshFactorReturns();
      alert(`Factor returns refreshed: ${result.success} records loaded`);
      loadDataStatus();
      if (selectedView) loadStatistics();
    } catch (error) {
      console.error('Failed to refresh factor returns:', error);
      alert('Failed to refresh factor returns. See console for details.');
    } finally {
      setRefreshingData(null);
    }
  };

  const formatPercent = (value: number | null | undefined) => {
    if (value === null || value === undefined) return 'N/A';
    return (value * 100).toFixed(2) + '%';
  };

  const formatNumber = (value: number | null | undefined, decimals: number = 2) => {
    if (value === null || value === undefined) return 'N/A';
    return value.toFixed(decimals);
  };

  const viewOptions = views.map((v) => ({
    value: v,
    label: v.view_name,
    group: v.view_type,
  }));

  return (
    <Layout>
      <div className="space-y-6">
        {/* Header */}
        <div className="card">
          <h1 className="text-2xl font-bold mb-4">Portfolio Statistics & Analytics</h1>

          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">Portfolio View</label>
              <Select
                options={viewOptions}
                value={viewOptions.find((o) => o.value === selectedView)}
                onChange={(option) => setSelectedView(option?.value)}
                placeholder="Select view..."
                className="w-full"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">Benchmark</label>
              <select
                value={benchmark}
                onChange={(e) => setBenchmark(e.target.value)}
                className="w-full px-3 py-2 border border-gray-300 rounded-md"
              >
                <option value="SP500">S&P 500</option>
              </select>
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">Analysis Window (Days)</label>
              <select
                value={window}
                onChange={(e) => setWindow(Number(e.target.value))}
                className="w-full px-3 py-2 border border-gray-300 rounded-md"
              >
                <option value="63">3 Months (~63 days)</option>
                <option value="126">6 Months (~126 days)</option>
                <option value="252">1 Year (~252 days)</option>
                <option value="504">2 Years (~504 days)</option>
              </select>
            </div>
          </div>
        </div>

        {/* Data Status */}
        {dataStatus && (
          <div className="card">
            <h2 className="text-xl font-bold mb-4">Data Status & Management</h2>

            <div className="grid grid-cols-1 md:grid-cols-3 gap-6">
              {/* Classifications Status */}
              <div className="border rounded p-4">
                <h3 className="font-semibold text-gray-700 mb-2">Security Classifications</h3>
                <div className="space-y-2 text-sm">
                  <div>
                    <span className="text-gray-600">Coverage:</span>{' '}
                    <span className="font-medium">
                      {dataStatus.classifications.classified_securities} / {dataStatus.classifications.total_securities} ({dataStatus.classifications.coverage_percent}%)
                    </span>
                  </div>
                  {dataStatus.classifications.last_updated && (
                    <div>
                      <span className="text-gray-600">Last Updated:</span>{' '}
                      <span className="font-medium text-xs">{new Date(dataStatus.classifications.last_updated).toLocaleString()}</span>
                    </div>
                  )}
                  <div>
                    <span className="text-gray-600">Sources:</span>{' '}
                    <div className="text-xs mt-1">
                      {Object.entries(dataStatus.classifications.sources || {}).map(([source, count]: [string, any]) => (
                        <div key={source}>{source}: {count}</div>
                      ))}
                    </div>
                  </div>
                  <button
                    onClick={handleRefreshClassifications}
                    disabled={refreshingData === 'classifications'}
                    className="mt-3 w-full px-3 py-2 bg-blue-600 text-white text-sm rounded hover:bg-blue-700 disabled:bg-gray-400"
                  >
                    {refreshingData === 'classifications' ? 'Refreshing...' : 'Refresh Classifications'}
                  </button>
                </div>
              </div>

              {/* Benchmark Status */}
              <div className="border rounded p-4">
                <h3 className="font-semibold text-gray-700 mb-2">S&P 500 Benchmark</h3>
                <div className="space-y-2 text-sm">
                  {Object.entries(dataStatus.benchmarks || {}).map(([code, data]: [string, any]) => (
                    <div key={code} className="border-b pb-2 last:border-b-0">
                      <div className="font-medium">{code}</div>
                      <div className="text-xs text-gray-600">
                        {data.constituent_count} constituents
                      </div>
                      {data.as_of_date && (
                        <div className="text-xs text-gray-600">
                          As of: {data.as_of_date}
                        </div>
                      )}
                    </div>
                  ))}
                  <button
                    onClick={handleRefreshBenchmarks}
                    disabled={refreshingData === 'benchmarks'}
                    className="mt-3 w-full px-3 py-2 bg-blue-600 text-white text-sm rounded hover:bg-blue-700 disabled:bg-gray-400"
                  >
                    {refreshingData === 'benchmarks' ? 'Refreshing...' : 'Refresh S&P 500'}
                  </button>
                </div>
              </div>

              {/* Factor Returns Status */}
              <div className="border rounded p-4">
                <h3 className="font-semibold text-gray-700 mb-2">Factor Returns</h3>
                <div className="space-y-2 text-sm">
                  {dataStatus.factor_returns.start_date && dataStatus.factor_returns.end_date ? (
                    <>
                      <div>
                        <span className="text-gray-600">Date Range:</span>{' '}
                        <div className="text-xs mt-1">
                          {dataStatus.factor_returns.start_date} to {dataStatus.factor_returns.end_date}
                        </div>
                      </div>
                      <div>
                        <span className="text-gray-600">Trading Days:</span>{' '}
                        <span className="font-medium">{dataStatus.factor_returns.trading_days}</span>
                      </div>
                      <div>
                        <span className="text-gray-600">Factors:</span>
                        <div className="text-xs mt-1">
                          {Object.keys(dataStatus.factor_returns.factors || {}).join(', ')}
                        </div>
                      </div>
                      {dataStatus.factor_returns.last_updated && (
                        <div>
                          <span className="text-gray-600">Last Updated:</span>{' '}
                          <span className="font-medium text-xs">{new Date(dataStatus.factor_returns.last_updated).toLocaleString()}</span>
                        </div>
                      )}
                    </>
                  ) : (
                    <div className="text-red-600">No factor data available</div>
                  )}
                  <button
                    onClick={handleRefreshFactors}
                    disabled={refreshingData === 'factors'}
                    className="mt-3 w-full px-3 py-2 bg-blue-600 text-white text-sm rounded hover:bg-blue-700 disabled:bg-gray-400"
                  >
                    {refreshingData === 'factors' ? 'Refreshing...' : 'Refresh Factor Returns'}
                  </button>
                </div>
              </div>
            </div>

            {/* Data Readiness Indicators */}
            <div className="mt-6 p-4 bg-gray-50 rounded">
              <h3 className="font-semibold text-gray-700 mb-2">Feature Readiness</h3>
              <div className="flex gap-4">
                <div className="flex items-center gap-2">
                  <span className={`w-3 h-3 rounded-full ${dataStatus.data_readiness.brinson_attribution_ready ? 'bg-green-500' : 'bg-red-500'}`}></span>
                  <span className="text-sm">Brinson Attribution</span>
                </div>
                <div className="flex items-center gap-2">
                  <span className={`w-3 h-3 rounded-full ${dataStatus.data_readiness.factor_attribution_ready ? 'bg-green-500' : 'bg-red-500'}`}></span>
                  <span className="text-sm">Factor Attribution</span>
                </div>
              </div>
            </div>
          </div>
        )}

        {loading && <div className="text-center py-8">Loading statistics...</div>}

        {!loading && selectedView && (
          <>
            {/* Volatility & Risk Metrics */}
            {volatilityData && !volatilityData.error && (
              <div className="card">
                <h2 className="text-xl font-bold mb-4">Risk & Volatility Metrics</h2>
                <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                  <div className="border-l-4 border-red-500 pl-4">
                    <div className="text-sm text-gray-600">Annualized Volatility</div>
                    <div className="text-2xl font-bold">{formatPercent(volatilityData.annualized_volatility)}</div>
                  </div>
                  <div className="border-l-4 border-orange-500 pl-4">
                    <div className="text-sm text-gray-600">Tracking Error</div>
                    <div className="text-2xl font-bold">{formatPercent(volatilityData.tracking_error)}</div>
                  </div>
                  <div className="border-l-4 border-blue-500 pl-4">
                    <div className="text-sm text-gray-600">Information Ratio</div>
                    <div className="text-2xl font-bold">{formatNumber(volatilityData.information_ratio)}</div>
                  </div>
                  <div className="border-l-4 border-purple-500 pl-4">
                    <div className="text-sm text-gray-600">Sortino Ratio</div>
                    <div className="text-2xl font-bold">{formatNumber(volatilityData.sortino_ratio)}</div>
                  </div>
                </div>

                <div className="mt-6 grid grid-cols-2 md:grid-cols-4 gap-4">
                  <div>
                    <div className="text-sm text-gray-600">Downside Deviation</div>
                    <div className="text-lg font-semibold">{formatPercent(volatilityData.downside_deviation)}</div>
                  </div>
                  <div>
                    <div className="text-sm text-gray-600">Mean Return (Ann.)</div>
                    <div className="text-lg font-semibold">{formatPercent(volatilityData.mean_return)}</div>
                  </div>
                  <div>
                    <div className="text-sm text-gray-600">Skewness</div>
                    <div className="text-lg font-semibold">{formatNumber(volatilityData.skewness)}</div>
                  </div>
                  <div>
                    <div className="text-sm text-gray-600">Kurtosis</div>
                    <div className="text-lg font-semibold">{formatNumber(volatilityData.kurtosis)}</div>
                  </div>
                </div>

                <div className="mt-4 text-xs text-gray-500">
                  vs {volatilityData.benchmark} | Window: {volatilityData.window_days} days
                </div>
              </div>
            )}

            {/* Drawdown Analysis */}
            {drawdownData && !drawdownData.error && (
              <div className="card">
                <h2 className="text-xl font-bold mb-4">Drawdown Analysis</h2>
                <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
                  <div className="border-l-4 border-red-600 pl-4">
                    <div className="text-sm text-gray-600">Max Drawdown</div>
                    <div className="text-2xl font-bold text-red-600">{formatPercent(drawdownData.max_drawdown)}</div>
                    <div className="text-xs text-gray-500 mt-1">
                      {drawdownData.max_drawdown_date && format(new Date(drawdownData.max_drawdown_date), 'MMM d, yyyy')}
                    </div>
                  </div>
                  <div className="border-l-4 border-orange-500 pl-4">
                    <div className="text-sm text-gray-600">Current Drawdown</div>
                    <div className="text-2xl font-bold">{formatPercent(drawdownData.current_drawdown)}</div>
                  </div>
                  <div className="border-l-4 border-yellow-500 pl-4">
                    <div className="text-sm text-gray-600">Ulcer Index</div>
                    <div className="text-2xl font-bold">{formatNumber(drawdownData.ulcer_index)}</div>
                    <div className="text-xs text-gray-500 mt-1">RMS of drawdowns</div>
                  </div>
                </div>

                {drawdownData.days_to_recovery && (
                  <div className="mb-4 p-3 bg-blue-50 rounded">
                    <div className="text-sm font-medium text-blue-800">
                      Recovery from max drawdown took {drawdownData.days_to_recovery} days
                      {drawdownData.recovery_date && ` (recovered on ${format(new Date(drawdownData.recovery_date), 'MMM d, yyyy')})`}
                    </div>
                  </div>
                )}

                {drawdownData.drawdown_periods && drawdownData.drawdown_periods.length > 0 && (
                  <div>
                    <h3 className="text-sm font-semibold mb-2">Top Drawdown Periods</h3>
                    <table className="table text-sm">
                      <thead>
                        <tr>
                          <th>Start Date</th>
                          <th>End Date</th>
                          <th>Max Drawdown</th>
                          <th>Duration (Days)</th>
                          <th>Status</th>
                        </tr>
                      </thead>
                      <tbody>
                        {drawdownData.drawdown_periods.slice(0, 5).map((period: any, idx: number) => (
                          <tr key={idx}>
                            <td>{format(new Date(period.start_date), 'MMM d, yyyy')}</td>
                            <td>{format(new Date(period.end_date), 'MMM d, yyyy')}</td>
                            <td className="font-semibold text-red-600">{formatPercent(period.max_drawdown)}</td>
                            <td>{period.days}</td>
                            <td>
                              {period.ongoing ? (
                                <span className="px-2 py-1 bg-yellow-100 text-yellow-800 rounded text-xs">Ongoing</span>
                              ) : (
                                <span className="px-2 py-1 bg-green-100 text-green-800 rounded text-xs">Recovered</span>
                              )}
                            </td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
            )}

            {/* VaR & CVaR */}
            {varData && !varData.error && (
              <div className="card">
                <h2 className="text-xl font-bold mb-4">Tail Risk (VaR & CVaR)</h2>
                <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                  <div className="border-l-4 border-red-500 pl-4">
                    <div className="text-sm text-gray-600">VaR 95%</div>
                    <div className="text-2xl font-bold">{formatPercent(varData.var_95)}</div>
                    <div className="text-xs text-gray-500 mt-1">1-day, 95% confidence</div>
                  </div>
                  <div className="border-l-4 border-red-600 pl-4">
                    <div className="text-sm text-gray-600">CVaR 95%</div>
                    <div className="text-2xl font-bold">{formatPercent(varData.cvar_95)}</div>
                    <div className="text-xs text-gray-500 mt-1">Expected shortfall</div>
                  </div>
                  <div className="border-l-4 border-red-700 pl-4">
                    <div className="text-sm text-gray-600">VaR 99%</div>
                    <div className="text-2xl font-bold">{formatPercent(varData.var_99)}</div>
                    <div className="text-xs text-gray-500 mt-1">1-day, 99% confidence</div>
                  </div>
                  <div className="border-l-4 border-red-800 pl-4">
                    <div className="text-sm text-gray-600">CVaR 99%</div>
                    <div className="text-2xl font-bold">{formatPercent(varData.cvar_99)}</div>
                    <div className="text-xs text-gray-500 mt-1">Expected shortfall</div>
                  </div>
                </div>
                <div className="mt-4 text-xs text-gray-500">
                  Historical simulation method | Window: {window} days
                </div>
              </div>
            )}

            {/* Enhanced Factor Analysis */}
            {factorData && !factorData.error && (
              <div className="card">
                <h2 className="text-xl font-bold mb-4">Factor Analysis (Fama-French + Momentum)</h2>

                {/* Summary Metrics */}
                <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
                  <div className="border-l-4 border-blue-500 pl-4">
                    <div className="text-sm text-gray-600">Alpha (Annualized)</div>
                    <div className="text-2xl font-bold">{formatPercent(factorData.alpha_annualized)}</div>
                  </div>
                  <div className="border-l-4 border-green-500 pl-4">
                    <div className="text-sm text-gray-600">R-Squared</div>
                    <div className="text-2xl font-bold">{formatPercent(factorData.r_squared)}</div>
                  </div>
                  <div className="border-l-4 border-purple-500 pl-4">
                    <div className="text-sm text-gray-600">Factor Risk</div>
                    <div className="text-2xl font-bold">{formatNumber(factorData.factor_variance_pct, 1)}%</div>
                  </div>
                  <div className="border-l-4 border-orange-500 pl-4">
                    <div className="text-sm text-gray-600">Idiosyncratic Risk</div>
                    <div className="text-2xl font-bold">{formatNumber(factorData.idiosyncratic_variance_pct, 1)}%</div>
                  </div>
                </div>

                {/* Factor Exposure Bar Chart */}
                {factorData.factor_exposures && Object.keys(factorData.factor_exposures).length > 0 && (
                  <div className="mb-6">
                    <h3 className="text-sm font-semibold mb-3">Factor Exposures (Beta)</h3>
                    <div className="space-y-2">
                      {Object.entries(factorData.factor_exposures).map(([factor, beta]: [string, any]) => {
                        const maxBeta = Math.max(...Object.values(factorData.factor_exposures).map((b: any) => Math.abs(b)));
                        const barWidth = Math.abs(beta) / (maxBeta || 1) * 100;
                        const isPositive = beta >= 0;
                        return (
                          <div key={factor} className="flex items-center gap-3">
                            <div className="w-28 text-sm text-gray-700">{factor}</div>
                            <div className="flex-1 h-6 bg-gray-100 rounded relative">
                              <div className="absolute inset-y-0 left-1/2 w-px bg-gray-300"></div>
                              <div
                                className={`absolute top-0 h-full rounded ${isPositive ? 'bg-green-500 left-1/2' : 'bg-red-500 right-1/2'}`}
                                style={{ width: `${barWidth / 2}%` }}
                              ></div>
                            </div>
                            <div className={`w-16 text-right text-sm font-semibold ${isPositive ? 'text-green-600' : 'text-red-600'}`}>
                              {formatNumber(beta, 2)}
                            </div>
                          </div>
                        );
                      })}
                    </div>
                    <div className="flex justify-between text-xs text-gray-500 mt-1 px-28">
                      <span>← Negative</span>
                      <span>Positive →</span>
                    </div>
                  </div>
                )}

                <div className="mt-4 text-xs text-gray-500">
                  As of {factorData.as_of_date && format(new Date(factorData.as_of_date), 'MMM d, yyyy')} |
                  Window: {factorData.window_days} days
                </div>
              </div>
            )}

            {/* Factor Attribution Waterfall */}
            {factorAttributionData && !factorAttributionData.error && factorAttributionData.factor_contributions && (
              <div className="card">
                <h2 className="text-xl font-bold mb-4">Factor Contribution to Returns</h2>
                <div className="mb-4 p-3 bg-blue-50 rounded flex justify-between items-center">
                  <div className="text-sm">
                    <span className="font-semibold">Total Return:</span> {formatPercent(factorAttributionData.total_return)}
                  </div>
                  <div className="text-sm">
                    <span className="font-semibold">Alpha:</span>{' '}
                    <span className={factorAttributionData.alpha >= 0 ? 'text-green-600' : 'text-red-600'}>
                      {formatPercent(factorAttributionData.alpha)}
                    </span>
                  </div>
                </div>

                {/* Waterfall Chart */}
                <div className="space-y-2 mb-4">
                  {Object.entries(factorAttributionData.factor_contributions)
                    .sort(([,a]: any, [,b]: any) => Math.abs(b) - Math.abs(a))
                    .map(([factor, contrib]: [string, any]) => {
                      const maxContrib = Math.max(
                        ...Object.values(factorAttributionData.factor_contributions).map((c: any) => Math.abs(c)),
                        Math.abs(factorAttributionData.alpha || 0)
                      );
                      const barWidth = Math.abs(contrib) / (maxContrib || 0.01) * 100;
                      const isPositive = contrib >= 0;
                      return (
                        <div key={factor} className="flex items-center gap-3">
                          <div className="w-32 text-sm text-gray-700">{factor}</div>
                          <div className="flex-1 h-5 bg-gray-100 rounded relative">
                            <div className="absolute inset-y-0 left-1/2 w-px bg-gray-300"></div>
                            <div
                              className={`absolute top-0 h-full rounded ${isPositive ? 'bg-green-500 left-1/2' : 'bg-red-500 right-1/2'}`}
                              style={{ width: `${Math.min(barWidth / 2, 50)}%` }}
                            ></div>
                          </div>
                          <div className={`w-20 text-right text-sm font-semibold ${isPositive ? 'text-green-600' : 'text-red-600'}`}>
                            {isPositive ? '+' : ''}{formatPercent(contrib)}
                          </div>
                        </div>
                      );
                    })}
                  {/* Alpha row */}
                  <div className="flex items-center gap-3 border-t pt-2 mt-2">
                    <div className="w-32 text-sm text-gray-700 font-semibold">Alpha (Selection)</div>
                    <div className="flex-1 h-5 bg-gray-100 rounded relative">
                      <div className="absolute inset-y-0 left-1/2 w-px bg-gray-300"></div>
                      <div
                        className={`absolute top-0 h-full rounded ${(factorAttributionData.alpha || 0) >= 0 ? 'bg-blue-500 left-1/2' : 'bg-orange-500 right-1/2'}`}
                        style={{ width: `${Math.min(Math.abs(factorAttributionData.alpha || 0) / (Math.max(...Object.values(factorAttributionData.factor_contributions).map((c: any) => Math.abs(c)), 0.01)) * 50, 50)}%` }}
                      ></div>
                    </div>
                    <div className={`w-20 text-right text-sm font-semibold ${(factorAttributionData.alpha || 0) >= 0 ? 'text-blue-600' : 'text-orange-600'}`}>
                      {(factorAttributionData.alpha || 0) >= 0 ? '+' : ''}{formatPercent(factorAttributionData.alpha)}
                    </div>
                  </div>
                </div>

                <div className="text-xs text-gray-500">
                  R²: {formatPercent(factorAttributionData.r_squared)} | {factorAttributionData.observation_count} observations
                </div>
              </div>
            )}

            {/* Risk Decomposition */}
            {factorRiskData && !factorRiskData.error && (
              <div className="card">
                <h2 className="text-xl font-bold mb-4">Factor Risk Decomposition</h2>

                {/* Summary */}
                <div className="grid grid-cols-3 gap-4 mb-6">
                  <div className="text-center p-4 bg-gray-50 rounded">
                    <div className="text-2xl font-bold">{formatPercent(factorRiskData.total_volatility / 100)}</div>
                    <div className="text-sm text-gray-600">Total Volatility</div>
                  </div>
                  <div className="text-center p-4 bg-purple-50 rounded">
                    <div className="text-2xl font-bold text-purple-700">{formatPercent(factorRiskData.factor_volatility / 100)}</div>
                    <div className="text-sm text-gray-600">Factor Volatility</div>
                  </div>
                  <div className="text-center p-4 bg-orange-50 rounded">
                    <div className="text-2xl font-bold text-orange-700">{formatPercent(factorRiskData.specific_volatility / 100)}</div>
                    <div className="text-sm text-gray-600">Specific Volatility</div>
                  </div>
                </div>

                {/* Risk Pie Breakdown */}
                <div className="mb-4">
                  <h3 className="text-sm font-semibold mb-2">Risk Attribution</h3>
                  <div className="flex items-center gap-4">
                    <div className="w-full h-6 rounded-full overflow-hidden bg-gray-200 flex">
                      <div
                        className="bg-purple-500 h-full"
                        style={{ width: `${factorRiskData.factor_risk_pct}%` }}
                      ></div>
                      <div
                        className="bg-orange-400 h-full"
                        style={{ width: `${factorRiskData.specific_risk_pct}%` }}
                      ></div>
                    </div>
                  </div>
                  <div className="flex justify-between mt-1 text-xs">
                    <span className="text-purple-700">Factor Risk: {formatNumber(factorRiskData.factor_risk_pct, 1)}%</span>
                    <span className="text-orange-700">Specific Risk: {formatNumber(factorRiskData.specific_risk_pct, 1)}%</span>
                  </div>
                </div>

                {/* Individual Factor Risk Contributions */}
                {factorRiskData.factor_contributions && (
                  <div>
                    <h3 className="text-sm font-semibold mb-2">Risk by Factor</h3>
                    <div className="space-y-2">
                      {Object.entries(factorRiskData.factor_contributions)
                        .sort(([,a]: any, [,b]: any) => b.pct_of_total - a.pct_of_total)
                        .map(([factor, data]: [string, any]) => (
                          <div key={factor} className="flex items-center gap-3">
                            <div className="w-28 text-sm text-gray-700">{factor}</div>
                            <div className="flex-1 h-4 bg-gray-100 rounded overflow-hidden">
                              <div
                                className="bg-indigo-500 h-full"
                                style={{ width: `${Math.min(data.pct_of_total, 100)}%` }}
                              ></div>
                            </div>
                            <div className="w-16 text-right text-sm">{formatNumber(data.pct_of_total, 1)}%</div>
                          </div>
                        ))}
                    </div>
                  </div>
                )}
              </div>
            )}

            {/* Historical Factor Exposures */}
            {historicalFactorData && !historicalFactorData.error && historicalFactorData.historical_exposures && historicalFactorData.historical_exposures.length > 0 && (
              <div className="card">
                <h2 className="text-xl font-bold mb-4">Historical Factor Exposures</h2>
                <p className="text-sm text-gray-600 mb-4">
                  Rolling {historicalFactorData.rolling_window_days}-day factor betas over time
                </p>

                {/* Mini sparkline table */}
                <div className="overflow-x-auto">
                  <table className="w-full text-sm">
                    <thead>
                      <tr className="border-b">
                        <th className="text-left py-2 pr-4">Factor</th>
                        <th className="text-center py-2 px-2">Trend (Last 12 points)</th>
                        <th className="text-right py-2 px-2">Current</th>
                        <th className="text-right py-2 px-2">Avg</th>
                        <th className="text-right py-2 pl-2">Min/Max</th>
                      </tr>
                    </thead>
                    <tbody>
                      {historicalFactorData.factors && historicalFactorData.factors.map((factor: string) => {
                        const values = historicalFactorData.historical_exposures.map((h: any) => h[factor] || 0);
                        const current = values[values.length - 1] || 0;
                        const avg = values.reduce((a: number, b: number) => a + b, 0) / values.length;
                        const min = Math.min(...values);
                        const max = Math.max(...values);
                        const lastN = values.slice(-12);
                        const sparklineMax = Math.max(...lastN.map(Math.abs));

                        return (
                          <tr key={factor} className="border-b border-gray-100">
                            <td className="py-2 pr-4 font-medium">{factor}</td>
                            <td className="py-2 px-2">
                              <div className="flex items-end justify-center h-8 gap-px">
                                {lastN.map((v: number, i: number) => (
                                  <div
                                    key={i}
                                    className={`w-2 ${v >= 0 ? 'bg-green-400' : 'bg-red-400'}`}
                                    style={{ height: `${Math.abs(v) / (sparklineMax || 1) * 100}%`, minHeight: '2px' }}
                                  ></div>
                                ))}
                              </div>
                            </td>
                            <td className={`py-2 px-2 text-right font-semibold ${current >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                              {formatNumber(current, 2)}
                            </td>
                            <td className="py-2 px-2 text-right text-gray-600">{formatNumber(avg, 2)}</td>
                            <td className="py-2 pl-2 text-right text-gray-500 text-xs">
                              {formatNumber(min, 2)} / {formatNumber(max, 2)}
                            </td>
                          </tr>
                        );
                      })}
                    </tbody>
                  </table>
                </div>

                <div className="mt-4 text-xs text-gray-500">
                  {historicalFactorData.historical_exposures.length} data points
                </div>
              </div>
            )}

            {/* Contribution to Returns */}
            {contributionData && !contributionData.error && contributionData.contributions && contributionData.contributions.length > 0 && (
              <div className="card">
                <h2 className="text-xl font-bold mb-4">Contribution to Returns</h2>
                <div className="mb-4 p-3 bg-blue-50 rounded">
                  <div className="text-sm">
                    <span className="font-semibold">Total Return:</span> {formatPercent(contributionData.total_return)}
                    <span className="ml-4 text-gray-600">
                      Period: {contributionData.period_start && format(new Date(contributionData.period_start), 'MMM d, yyyy')} -
                      {contributionData.period_end && format(new Date(contributionData.period_end), 'MMM d, yyyy')}
                    </span>
                  </div>
                </div>

                <table className="table text-sm">
                  <thead>
                    <tr>
                      <th>Symbol</th>
                      <th>Name</th>
                      <th className="text-right">Avg Weight</th>
                      <th className="text-right">Contribution</th>
                      <th className="text-right">% of Total Return</th>
                    </tr>
                  </thead>
                  <tbody>
                    {contributionData.contributions.map((contrib: any, idx: number) => (
                      <tr key={idx}>
                        <td className="font-semibold">{contrib.symbol}</td>
                        <td className="max-w-xs truncate">{contrib.asset_name}</td>
                        <td className="text-right">{formatPercent(contrib.avg_weight)}</td>
                        <td className={`text-right font-semibold ${contrib.contribution >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                          {formatPercent(contrib.contribution)}
                        </td>
                        <td className="text-right">{formatNumber(contrib.contribution_pct, 1)}%</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}

            {/* ===== PHASE 2: ADVANCED ANALYTICS ===== */}

            {/* Turnover Analysis */}
            {turnoverData && !turnoverData.error && turnoverData.overall && (
              <div className="card">
                <h2 className="text-xl font-bold mb-4">Trading & Turnover</h2>
                <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
                  <div className="border-l-4 border-blue-500 pl-4">
                    <div className="text-sm text-gray-600">Annualized Gross Turnover</div>
                    <div className="text-2xl font-bold">{formatPercent(turnoverData.overall.annualized_gross_turnover)}</div>
                  </div>
                  <div className="border-l-4 border-green-500 pl-4">
                    <div className="text-sm text-gray-600">Annualized Net Turnover</div>
                    <div className="text-2xl font-bold">{formatPercent(turnoverData.overall.annualized_net_turnover)}</div>
                  </div>
                  <div className="border-l-4 border-purple-500 pl-4">
                    <div className="text-sm text-gray-600">Total Trades</div>
                    <div className="text-2xl font-bold">{turnoverData.overall.trade_count}</div>
                  </div>
                  <div className="border-l-4 border-orange-500 pl-4">
                    <div className="text-sm text-gray-600">Avg Portfolio Value</div>
                    <div className="text-2xl font-bold">${(turnoverData.overall.avg_portfolio_value / 1000000).toFixed(1)}M</div>
                  </div>
                </div>

                {turnoverData.by_period && turnoverData.by_period.length > 0 && (
                  <div>
                    <h3 className="text-sm font-semibold mb-2">Turnover by Period</h3>
                    <table className="table text-sm">
                      <thead>
                        <tr>
                          <th>Period</th>
                          <th className="text-right">Gross Turnover</th>
                          <th className="text-right">Net Turnover</th>
                          <th className="text-right">Trades</th>
                        </tr>
                      </thead>
                      <tbody>
                        {turnoverData.by_period.slice(0, 12).map((period: any, idx: number) => (
                          <tr key={idx}>
                            <td>{period.period}</td>
                            <td className="text-right font-semibold">{formatPercent(period.gross_turnover)}</td>
                            <td className="text-right">{formatPercent(period.net_turnover)}</td>
                            <td className="text-right">{period.trade_count}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                )}
              </div>
            )}

            {/* Sector Comparison */}
            {sectorComparisonData && !sectorComparisonData.error && sectorComparisonData.comparison && (
              <div className="card">
                <h2 className="text-xl font-bold mb-4">Sector Analysis vs {sectorComparisonData.benchmark}</h2>
                <table className="table text-sm">
                  <thead>
                    <tr>
                      <th>Sector</th>
                      <th className="text-right">Portfolio Weight</th>
                      <th className="text-right">Benchmark Weight</th>
                      <th className="text-right">Active Weight</th>
                      <th>Position</th>
                    </tr>
                  </thead>
                  <tbody>
                    {sectorComparisonData.comparison.map((sector: any, idx: number) => (
                      <tr key={idx}>
                        <td className="font-semibold">{sector.sector}</td>
                        <td className="text-right">{formatPercent(sector.portfolio_weight)}</td>
                        <td className="text-right text-gray-600">{formatPercent(sector.benchmark_weight)}</td>
                        <td className={`text-right font-semibold ${sector.active_weight >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                          {sector.active_weight >= 0 ? '+' : ''}{formatPercent(sector.active_weight)}
                        </td>
                        <td>
                          <span className={`px-2 py-1 rounded text-xs font-medium ${
                            sector.over_under === 'Overweight' ? 'bg-green-100 text-green-800' :
                            sector.over_under === 'Underweight' ? 'bg-red-100 text-red-800' :
                            'bg-gray-100 text-gray-800'
                          }`}>
                            {sector.over_under}
                          </span>
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
            )}

            {/* Enhanced Brinson Attribution */}
            {brinsonData && (
              <div className="card">
                {/* Header with Time Toggle */}
                <div className="flex justify-between items-start mb-4">
                  <h2 className="text-xl font-bold">Brinson Attribution Analysis</h2>
                  <div className="flex gap-1">
                    {(['1M', '3M', 'YTD', '1Y'] as BrinsonPeriod[]).map((period) => (
                      <button
                        key={period}
                        onClick={() => setBrinsonPeriod(period)}
                        className={`px-3 py-1 text-sm rounded ${
                          brinsonPeriod === period
                            ? 'bg-blue-600 text-white'
                            : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                        }`}
                      >
                        {period}
                      </button>
                    ))}
                  </div>
                </div>

                {brinsonLoading ? (
                  <div className="text-center py-4 text-gray-500">Loading attribution data...</div>
                ) : brinsonData.error ? (
                  <div className="p-4 bg-red-50 rounded border border-red-200">
                    <p className="text-sm font-semibold text-red-800 mb-2">{brinsonData.error}</p>
                    {brinsonData.missing_data && (
                      <p className="text-xs text-red-700 mt-1">
                        Missing Data: <span className="font-mono">{brinsonData.missing_data}</span>
                      </p>
                    )}
                    {brinsonData.action_required && (
                      <div className="mt-3 p-2 bg-white rounded border border-red-300">
                        <p className="text-xs text-gray-700 font-medium">Action Required:</p>
                        <p className="text-xs text-gray-600 font-mono mt-1">{brinsonData.action_required}</p>
                      </div>
                    )}
                  </div>
                ) : brinsonData.note ? (
                  <div className="p-4 bg-yellow-50 rounded border border-yellow-200">
                    <p className="text-sm text-yellow-800">{brinsonData.note}</p>
                    <p className="text-xs text-yellow-700 mt-2">
                      Full Brinson attribution requires sector-level return tracking over time. This feature is available
                      once sector classifications and historical sector returns are configured.
                    </p>
                  </div>
                ) : (
                  <>
                    {/* Active Return Headline */}
                    <div className="mb-6 p-4 bg-gradient-to-r from-blue-50 to-indigo-50 rounded-lg border border-blue-200">
                      <div className="text-center">
                        <div className="text-sm text-gray-600 mb-1">Active Return vs {brinsonData.benchmark || 'Benchmark'}</div>
                        <div className={`text-4xl font-bold ${(brinsonData.total_active_return || 0) >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                          {(brinsonData.total_active_return || 0) >= 0 ? '+' : ''}{formatPercent(brinsonData.total_active_return)}
                        </div>
                        <div className="text-xs text-gray-500 mt-2">
                          {brinsonData.start_date && format(new Date(brinsonData.start_date), 'MMM d, yyyy')} - {brinsonData.end_date && format(new Date(brinsonData.end_date), 'MMM d, yyyy')}
                          {' | '}Grouping: GICS Sector
                        </div>
                      </div>
                    </div>

                    {/* Three Drivers */}
                    <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
                      <div className="border-l-4 border-blue-500 pl-4">
                        <div className="text-sm text-gray-600">Allocation Effect</div>
                        <div className={`text-2xl font-bold ${(brinsonData.allocation_effect || 0) >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                          {(brinsonData.allocation_effect || 0) >= 0 ? '+' : ''}{formatPercent(brinsonData.allocation_effect)}
                        </div>
                        <div className="text-xs text-gray-500 mt-1">Sector weighting decisions</div>
                      </div>
                      <div className="border-l-4 border-green-500 pl-4">
                        <div className="text-sm text-gray-600">Selection Effect</div>
                        <div className={`text-2xl font-bold ${(brinsonData.selection_effect || 0) >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                          {(brinsonData.selection_effect || 0) >= 0 ? '+' : ''}{formatPercent(brinsonData.selection_effect)}
                        </div>
                        <div className="text-xs text-gray-500 mt-1">Security selection within sectors</div>
                      </div>
                      <div className="border-l-4 border-purple-500 pl-4">
                        <div className="text-sm text-gray-600">Interaction Effect</div>
                        <div className={`text-2xl font-bold ${(brinsonData.interaction_effect || 0) >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                          {(brinsonData.interaction_effect || 0) >= 0 ? '+' : ''}{formatPercent(brinsonData.interaction_effect)}
                        </div>
                        <div className="text-xs text-gray-500 mt-1">Combined allocation & selection</div>
                      </div>
                    </div>

                    {/* Waterfall Chart */}
                    <div className="mb-6">
                      <h3 className="text-sm font-semibold mb-3">Attribution Waterfall</h3>
                      <div className="space-y-2">
                        {[
                          { label: 'Allocation', value: brinsonData.allocation_effect, color: 'blue' },
                          { label: 'Selection', value: brinsonData.selection_effect, color: 'green' },
                          { label: 'Interaction', value: brinsonData.interaction_effect, color: 'purple' },
                        ].map(({ label, value, color }) => {
                          const maxVal = Math.max(
                            Math.abs(brinsonData.allocation_effect || 0),
                            Math.abs(brinsonData.selection_effect || 0),
                            Math.abs(brinsonData.interaction_effect || 0),
                            0.001
                          );
                          const barWidth = Math.abs(value || 0) / maxVal * 100;
                          const isPositive = (value || 0) >= 0;
                          const colorClass = isPositive ? `bg-${color}-500` : `bg-${color}-300`;

                          return (
                            <div key={label} className="flex items-center gap-3">
                              <div className="w-24 text-sm text-gray-700">{label}</div>
                              <div className="flex-1 h-6 bg-gray-100 rounded relative">
                                <div className="absolute inset-y-0 left-1/2 w-px bg-gray-300"></div>
                                <div
                                  className={`absolute top-0 h-full rounded ${
                                    isPositive
                                      ? `bg-${color}-500 left-1/2`
                                      : `bg-${color}-400 right-1/2`
                                  }`}
                                  style={{
                                    width: `${barWidth / 2}%`,
                                    backgroundColor: isPositive
                                      ? (color === 'blue' ? '#3b82f6' : color === 'green' ? '#22c55e' : '#a855f7')
                                      : (color === 'blue' ? '#93c5fd' : color === 'green' ? '#86efac' : '#d8b4fe'),
                                  }}
                                ></div>
                              </div>
                              <div className={`w-20 text-right text-sm font-semibold ${isPositive ? 'text-green-600' : 'text-red-600'}`}>
                                {isPositive ? '+' : ''}{formatPercent(value)}
                              </div>
                            </div>
                          );
                        })}
                        {/* Total row */}
                        <div className="flex items-center gap-3 pt-2 border-t mt-2">
                          <div className="w-24 text-sm font-semibold text-gray-900">Total</div>
                          <div className="flex-1"></div>
                          <div className={`w-20 text-right text-sm font-bold ${(brinsonData.total_active_return || 0) >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                            {(brinsonData.total_active_return || 0) >= 0 ? '+' : ''}{formatPercent(brinsonData.total_active_return)}
                          </div>
                        </div>
                      </div>
                    </div>

                    {/* Sector Drill-Down Table */}
                    {brinsonData.by_sector && brinsonData.by_sector.length > 0 && (
                      <div>
                        <h3 className="text-sm font-semibold mb-3">Attribution by Sector</h3>
                        <div className="overflow-x-auto">
                          <table className="w-full text-sm">
                            <thead className="bg-gray-50">
                              <tr>
                                <th className="text-left py-2 px-2">Sector</th>
                                <th className="text-right py-2 px-2">Port Wt</th>
                                <th className="text-right py-2 px-2">Bench Wt</th>
                                <th className="text-right py-2 px-2">Active Wt</th>
                                <th className="text-right py-2 px-2">Port Ret</th>
                                <th className="text-right py-2 px-2">Bench Ret</th>
                                <th className="text-right py-2 px-2">Alloc</th>
                                <th className="text-right py-2 px-2">Select</th>
                                <th className="text-right py-2 px-2">Inter</th>
                                <th className="text-right py-2 px-2 font-semibold">Total</th>
                              </tr>
                            </thead>
                            <tbody>
                              {brinsonData.by_sector.map((sector: any, idx: number) => {
                                const activeWeight = (sector.portfolio_weight || 0) - (sector.benchmark_weight || 0);
                                return (
                                  <tr key={idx} className="border-b border-gray-100 hover:bg-gray-50">
                                    <td className="py-2 px-2 font-medium">{sector.sector}</td>
                                    <td className="py-2 px-2 text-right">{formatPercent(sector.portfolio_weight)}</td>
                                    <td className="py-2 px-2 text-right text-gray-600">{formatPercent(sector.benchmark_weight)}</td>
                                    <td className={`py-2 px-2 text-right ${activeWeight >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                                      {activeWeight >= 0 ? '+' : ''}{formatPercent(activeWeight)}
                                    </td>
                                    <td className="py-2 px-2 text-right">{formatPercent(sector.portfolio_return)}</td>
                                    <td className="py-2 px-2 text-right text-gray-600">{formatPercent(sector.benchmark_return)}</td>
                                    <td className={`py-2 px-2 text-right ${(sector.allocation_effect || 0) >= 0 ? 'text-blue-600' : 'text-blue-400'}`}>
                                      {(sector.allocation_effect || 0) >= 0 ? '+' : ''}{formatPercent(sector.allocation_effect)}
                                    </td>
                                    <td className={`py-2 px-2 text-right ${(sector.selection_effect || 0) >= 0 ? 'text-green-600' : 'text-green-400'}`}>
                                      {(sector.selection_effect || 0) >= 0 ? '+' : ''}{formatPercent(sector.selection_effect)}
                                    </td>
                                    <td className={`py-2 px-2 text-right ${(sector.interaction_effect || 0) >= 0 ? 'text-purple-600' : 'text-purple-400'}`}>
                                      {(sector.interaction_effect || 0) >= 0 ? '+' : ''}{formatPercent(sector.interaction_effect)}
                                    </td>
                                    <td className={`py-2 px-2 text-right font-semibold ${(sector.total_effect || 0) >= 0 ? 'text-green-700' : 'text-red-700'}`}>
                                      {(sector.total_effect || 0) >= 0 ? '+' : ''}{formatPercent(sector.total_effect)}
                                    </td>
                                  </tr>
                                );
                              })}
                            </tbody>
                            <tfoot className="bg-gray-100 font-semibold">
                              <tr>
                                <td className="py-2 px-2">Total</td>
                                <td className="py-2 px-2 text-right">100%</td>
                                <td className="py-2 px-2 text-right">100%</td>
                                <td className="py-2 px-2 text-right">-</td>
                                <td className="py-2 px-2 text-right">-</td>
                                <td className="py-2 px-2 text-right">-</td>
                                <td className={`py-2 px-2 text-right ${(brinsonData.allocation_effect || 0) >= 0 ? 'text-blue-600' : 'text-blue-400'}`}>
                                  {(brinsonData.allocation_effect || 0) >= 0 ? '+' : ''}{formatPercent(brinsonData.allocation_effect)}
                                </td>
                                <td className={`py-2 px-2 text-right ${(brinsonData.selection_effect || 0) >= 0 ? 'text-green-600' : 'text-green-400'}`}>
                                  {(brinsonData.selection_effect || 0) >= 0 ? '+' : ''}{formatPercent(brinsonData.selection_effect)}
                                </td>
                                <td className={`py-2 px-2 text-right ${(brinsonData.interaction_effect || 0) >= 0 ? 'text-purple-600' : 'text-purple-400'}`}>
                                  {(brinsonData.interaction_effect || 0) >= 0 ? '+' : ''}{formatPercent(brinsonData.interaction_effect)}
                                </td>
                                <td className={`py-2 px-2 text-right ${(brinsonData.total_active_return || 0) >= 0 ? 'text-green-700' : 'text-red-700'}`}>
                                  {(brinsonData.total_active_return || 0) >= 0 ? '+' : ''}{formatPercent(brinsonData.total_active_return)}
                                </td>
                              </tr>
                            </tfoot>
                          </table>
                        </div>
                      </div>
                    )}

                    {/* Context Footer */}
                    <div className="mt-4 text-xs text-gray-500 flex justify-between">
                      <span>Benchmark: {brinsonData.benchmark || 'SP500'}</span>
                      <span>Benchmark data as of: {brinsonData.benchmark_data_date || 'N/A'}</span>
                    </div>
                  </>
                )}
              </div>
            )}

            {/* Factor Attribution */}
            {factorAttributionData && (
              <div className="card">
                <h2 className="text-xl font-bold mb-4">Factor Attribution of Returns</h2>
                {factorAttributionData.error ? (
                  <div className="p-4 bg-red-50 rounded border border-red-200">
                    <p className="text-sm font-semibold text-red-800 mb-2">{factorAttributionData.error}</p>
                    {factorAttributionData.missing_data && (
                      <p className="text-xs text-red-700 mt-1">
                        Missing Data: <span className="font-mono">{factorAttributionData.missing_data}</span>
                      </p>
                    )}
                    {factorAttributionData.action_required && (
                      <div className="mt-3 p-2 bg-white rounded border border-red-300">
                        <p className="text-xs text-gray-700 font-medium">Action Required:</p>
                        <p className="text-xs text-gray-600 font-mono mt-1">{factorAttributionData.action_required}</p>
                      </div>
                    )}
                  </div>
                ) : factorAttributionData.note ? (
                  <div className="p-4 bg-yellow-50 rounded border border-yellow-200">
                    <p className="text-sm text-yellow-800">{factorAttributionData.note}</p>
                    <p className="text-xs text-yellow-700 mt-2">
                      Factor attribution requires factor return data over time. This feature will be enabled once factor
                      return history is available.
                    </p>
                  </div>
                ) : (
                  <div>
                    <div className="mb-4 p-3 bg-blue-50 rounded">
                      <div className="text-sm">
                        <span className="font-semibold">Total Return:</span> {formatPercent(factorAttributionData.total_return)}
                      </div>
                    </div>
                    <h3 className="text-sm font-semibold mb-2">Factor Contributions</h3>
                    <div className="grid grid-cols-2 md:grid-cols-3 gap-3">
                      {Object.entries(factorAttributionData.factor_contributions || {}).map(([factor, contrib]: [string, any]) => (
                        <div key={factor} className="p-3 bg-gray-50 rounded">
                          <div className="text-xs text-gray-600 uppercase">{factor}</div>
                          <div className="text-lg font-semibold">{formatPercent(contrib)}</div>
                        </div>
                      ))}
                    </div>
                    <div className="mt-4 p-3 bg-green-50 rounded">
                      <div className="text-sm">
                        <span className="font-semibold">Alpha:</span> {formatPercent(factorAttributionData.alpha)}
                      </div>
                      <div className="text-xs text-gray-600 mt-1">Return from security selection beyond factor tilts</div>
                    </div>
                  </div>
                )}
              </div>
            )}
          </>
        )}
      </div>
    </Layout>
  );
}
