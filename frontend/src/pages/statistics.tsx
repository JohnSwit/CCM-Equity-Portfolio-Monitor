import { useState, useEffect, useMemo } from 'react';
import Layout from '@/components/Layout';
import { api } from '@/lib/api';
import { format, subDays, subMonths, startOfYear } from 'date-fns';
import Select from 'react-select';

// Time period options
type TimePeriod = '1M' | '3M' | 'YTD' | '1Y' | 'ALL';
type BrinsonPeriod = '1M' | '3M' | 'YTD' | '1Y' | 'ALL';
type FactorBenchPeriod = '1M' | '3M' | '6M' | 'YTD' | '1Y' | 'ALL';

const getDateRange = (period: TimePeriod): { start: Date | null; end: Date } => {
  const end = new Date();
  let start: Date | null;

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
    case 'ALL':
      start = null;
      break;
    default:
      start = subMonths(end, 3);
  }

  return { start, end };
};

const getBrinsonDateRange = (period: BrinsonPeriod): { start: Date | null; end: Date } => {
  const range = getDateRange(period as TimePeriod);
  return { start: range.start, end: range.end };
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

  // Phase 2 data
  const [turnoverData, setTurnoverData] = useState<any>(null);
  const [sectorData, setSectorData] = useState<any>(null);
  const [sectorComparisonData, setSectorComparisonData] = useState<any>(null);
  const [brinsonData, setBrinsonData] = useState<any>(null);

  // Factor Benchmarking + Attribution (uses free data sources)
  const [factorBenchmarking, setFactorBenchmarking] = useState<any>(null);
  const [factorBenchPeriod, setFactorBenchPeriod] = useState<FactorBenchPeriod>('1Y');
  const [factorBenchLoading, setFactorBenchLoading] = useState(false);
  const [factorBenchError, setFactorBenchError] = useState<string | null>(null);
  const [useExcessReturns, setUseExcessReturns] = useState(false);
  const [useRobustMode, setUseRobustMode] = useState(false);
  const [selectedBenchmark, setSelectedBenchmark] = useState<string | null>(null);
  const [availableBenchmarks, setAvailableBenchmarks] = useState<any[]>([]);
  const [factorRollingData, setFactorRollingData] = useState<any>(null);
  const [factorContribOverTime, setFactorContribOverTime] = useState<any>(null);
  const [rollingWindow, setRollingWindow] = useState<number>(63);
  const [contribFrequency, setContribFrequency] = useState<'M' | 'Q'>('M');
  const [showDiagnostics, setShowDiagnostics] = useState(false);
  const [factorSortBy, setFactorSortBy] = useState<string>('contribution');
  const [hideInsignificant, setHideInsignificant] = useState(false);

  // Contribution to Returns time period
  const [contributionPeriod, setContributionPeriod] = useState<TimePeriod>('ALL');
  const [contributionLoading, setContributionLoading] = useState(false);

  // Brinson time period
  const [brinsonPeriod, setBrinsonPeriod] = useState<BrinsonPeriod>('3M');
  const [brinsonLoading, setBrinsonLoading] = useState(false);
  const [expandedSectors, setExpandedSectors] = useState<Set<string>>(new Set());
  const [expandedDrivers, setExpandedDrivers] = useState<Set<string>>(new Set());

  // Data status
  const [dataStatus, setDataStatus] = useState<any>(null);
  const [refreshingData, setRefreshingData] = useState<string | null>(null);

  useEffect(() => {
    loadViews();
    loadDataStatus();
    loadAvailableBenchmarks();
  }, []);

  const loadAvailableBenchmarks = async () => {
    try {
      const benchmarks = await api.getAvailableBenchmarks();
      setAvailableBenchmarks(benchmarks);
    } catch (error) {
      console.error('Failed to load benchmarks:', error);
    }
  };

  useEffect(() => {
    if (selectedView) {
      loadStatistics();
    }
  }, [selectedView, benchmark, window]);

  // Reload Contribution to Returns when period changes
  useEffect(() => {
    if (selectedView) {
      loadContributionData();
    }
  }, [selectedView, contributionPeriod]);

  // Reload Brinson when period changes
  useEffect(() => {
    if (selectedView) {
      loadBrinsonData();
    }
  }, [selectedView, brinsonPeriod]);

  // Reload Factor Benchmarking when settings change
  useEffect(() => {
    if (selectedView) {
      loadFactorBenchmarking();
    }
  }, [selectedView, factorBenchPeriod, useExcessReturns, useRobustMode, selectedBenchmark]);

  // Load rolling analysis when settings change
  useEffect(() => {
    if (selectedView && showDiagnostics) {
      loadFactorRollingAnalysis();
      loadFactorContributionOverTime();
    }
  }, [selectedView, factorBenchPeriod, useExcessReturns, rollingWindow, contribFrequency, showDiagnostics]);

  const loadFactorBenchmarking = async () => {
    if (!selectedView) return;

    setFactorBenchLoading(true);
    setFactorBenchError(null);
    try {
      const data = await api.getFactorBenchmarking(
        selectedView.view_type,
        selectedView.view_id,
        'US_CORE',
        factorBenchPeriod,
        useExcessReturns,
        useRobustMode,
        selectedBenchmark || undefined
      );
      setFactorBenchmarking(data);
    } catch (error: any) {
      console.error('Failed to load factor benchmarking:', error);
      setFactorBenchmarking(null);
      // Extract detailed error message from API response
      const errorDetail = error.response?.data?.detail || error.message || 'Unknown error';
      const statusCode = error.response?.status;
      if (statusCode === 503) {
        setFactorBenchError(`Factor data unavailable: ${errorDetail}`);
      } else if (statusCode === 404) {
        setFactorBenchError(`${errorDetail}`);
      } else {
        setFactorBenchError(`Failed to load factor analysis: ${errorDetail}`);
      }
    } finally {
      setFactorBenchLoading(false);
    }
  };

  const loadFactorRollingAnalysis = async () => {
    if (!selectedView) return;

    try {
      const data = await api.getFactorRollingAnalysis(
        selectedView.view_type,
        selectedView.view_id,
        'US_CORE',
        factorBenchPeriod,
        rollingWindow,
        useExcessReturns
      );
      setFactorRollingData(data);
    } catch (error) {
      console.error('Failed to load rolling analysis:', error);
      setFactorRollingData(null);
    }
  };

  const loadFactorContributionOverTime = async () => {
    if (!selectedView) return;

    try {
      const data = await api.getFactorContributionOverTime(
        selectedView.view_type,
        selectedView.view_id,
        'US_CORE',
        factorBenchPeriod,
        contribFrequency,
        useExcessReturns
      );
      setFactorContribOverTime(data);
    } catch (error) {
      console.error('Failed to load contribution over time:', error);
      setFactorContribOverTime(null);
    }
  };

  const loadContributionData = async () => {
    if (!selectedView) return;

    setContributionLoading(true);
    try {
      const { start, end } = getDateRange(contributionPeriod);
      const contrib = await api.getContributionToReturns(
        selectedView.view_type,
        selectedView.view_id,
        start ? format(start, 'yyyy-MM-dd') : undefined,
        format(end, 'yyyy-MM-dd'),
        20
      );
      setContributionData(contrib);
    } catch (error) {
      console.error('Failed to load contribution data:', error);
    } finally {
      setContributionLoading(false);
    }
  };

  const loadBrinsonData = async () => {
    if (!selectedView) return;

    setBrinsonLoading(true);
    try {
      const { start, end } = getBrinsonDateRange(brinsonPeriod);
      const brinson = await api.getBrinsonAttribution(
        selectedView.view_type,
        selectedView.view_id,
        'SP500',
        start ? format(start, 'yyyy-MM-dd') : undefined,
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
      const [vol, dd, varCvar, turnover, sectors, sectorComp] = await Promise.all([
        api.getVolatilityMetrics(selectedView.view_type, selectedView.view_id, benchmark, window).catch(() => null),
        api.getDrawdownAnalysis(selectedView.view_type, selectedView.view_id).catch(() => null),
        api.getVarCvar(selectedView.view_type, selectedView.view_id, '95,99', window).catch(() => null),
        // Phase 2
        api.getTurnoverAnalysis(selectedView.view_type, selectedView.view_id, undefined, undefined, 'monthly').catch(() => null),
        api.getSectorWeights(selectedView.view_type, selectedView.view_id).catch(() => null),
        api.getSectorComparison(selectedView.view_type, selectedView.view_id, 'SP500').catch(() => null),
        // Note: Brinson, Contribution, and Factor Benchmarking are loaded separately with time period
      ]);

      setVolatilityData(vol);
      setDrawdownData(dd);
      setVarData(varCvar);
      setTurnoverData(turnover);
      setSectorData(sectors);
      setSectorComparisonData(sectorComp);
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

  const formatPercent = (value: number | null | undefined) => {
    if (value === null || value === undefined) return 'N/A';
    return (value * 100).toFixed(2) + '%';
  };

  // Format as basis points (percentage points) - clearer for attribution
  const formatBps = (value: number | null | undefined) => {
    if (value === null || value === undefined) return 'N/A';
    return (value * 100).toFixed(2) + ' pp';
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

            </div>

            {/* Data Readiness Indicators */}
            <div className="mt-6 p-4 bg-gray-50 rounded">
              <h3 className="font-semibold text-gray-700 mb-2">Feature Readiness</h3>
              <div className="flex gap-4">
                <div className="flex items-center gap-2">
                  <span className={`w-3 h-3 rounded-full ${dataStatus.data_readiness.brinson_attribution_ready ? 'bg-green-500' : 'bg-red-500'}`}></span>
                  <span className="text-sm">Brinson Attribution</span>
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

            {/* Contribution to Returns */}
            {(contributionData || contributionLoading) && (
              <div className="card">
                <div className="flex justify-between items-start mb-4">
                  <h2 className="text-xl font-bold">Contribution to Returns</h2>
                  <div className="flex gap-1">
                    {(['1M', '3M', 'YTD', '1Y', 'ALL'] as TimePeriod[]).map((period) => (
                      <button
                        key={period}
                        onClick={() => setContributionPeriod(period)}
                        className={`px-3 py-1 text-sm rounded ${
                          contributionPeriod === period
                            ? 'bg-blue-600 text-white'
                            : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                        }`}
                      >
                        {period === 'ALL' ? 'All Time' : period}
                      </button>
                    ))}
                  </div>
                </div>

                {contributionLoading ? (
                  <div className="text-center py-4 text-gray-500">Loading contribution data...</div>
                ) : contributionData && !contributionData.error && contributionData.contributions && contributionData.contributions.length > 0 ? (
                  <>
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
                  </>
                ) : contributionData?.error ? (
                  <div className="p-4 bg-red-50 rounded border border-red-200">
                    <p className="text-sm text-red-800">{contributionData.error}</p>
                  </div>
                ) : (
                  <div className="text-center py-4 text-gray-500">No contribution data available for this period.</div>
                )}
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
                    {(['1M', '3M', 'YTD', '1Y', 'ALL'] as BrinsonPeriod[]).map((period) => (
                      <button
                        key={period}
                        onClick={() => setBrinsonPeriod(period)}
                        className={`px-3 py-1 text-sm rounded ${
                          brinsonPeriod === period
                            ? 'bg-blue-600 text-white'
                            : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                        }`}
                      >
                        {period === 'ALL' ? 'All Time' : period}
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
                    {/* Period Returns + Active Return Headline */}
                    <div className="mb-6 p-4 bg-gradient-to-r from-blue-50 to-indigo-50 rounded-lg border border-blue-200">
                      <div className="grid grid-cols-1 md:grid-cols-3 gap-4 items-center">
                        {/* Portfolio Return */}
                        <div className="text-center md:text-left">
                          <div className="text-xs text-gray-500 uppercase tracking-wide">Portfolio Return</div>
                          <div className={`text-2xl font-bold ${(brinsonData.portfolio_return || 0) >= 0 ? 'text-gray-800' : 'text-red-600'}`}>
                            {(brinsonData.portfolio_return || 0) >= 0 ? '+' : ''}{formatPercent(brinsonData.portfolio_return)}
                          </div>
                        </div>
                        {/* Active Return (center, larger) */}
                        <div className="text-center">
                          <div className="text-sm text-gray-600 mb-1">Active Return vs {brinsonData.benchmark || 'Benchmark'}</div>
                          <div className={`text-4xl font-bold ${(brinsonData.total_active_return || 0) >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                            {(brinsonData.total_active_return || 0) >= 0 ? '+' : ''}{formatBps(brinsonData.total_active_return)}
                          </div>
                          <div className="text-xs text-gray-500 mt-2">
                            {brinsonData.start_date && format(new Date(brinsonData.start_date), 'MMM d, yyyy')} - {brinsonData.end_date && format(new Date(brinsonData.end_date), 'MMM d, yyyy')}
                          </div>
                        </div>
                        {/* Benchmark Return */}
                        <div className="text-center md:text-right">
                          <div className="text-xs text-gray-500 uppercase tracking-wide">{brinsonData.benchmark || 'Benchmark'} Return</div>
                          <div className={`text-2xl font-bold ${(brinsonData.benchmark_return || 0) >= 0 ? 'text-gray-600' : 'text-red-600'}`}>
                            {(brinsonData.benchmark_return || 0) >= 0 ? '+' : ''}{formatPercent(brinsonData.benchmark_return)}
                          </div>
                        </div>
                      </div>
                    </div>

                    {/* Three Drivers */}
                    <div className="grid grid-cols-1 md:grid-cols-3 gap-4 mb-6">
                      <div className="border-l-4 border-blue-500 pl-4">
                        <div className="text-sm text-gray-600">Allocation Effect</div>
                        <div className={`text-2xl font-bold ${(brinsonData.allocation_effect || 0) >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                          {(brinsonData.allocation_effect || 0) >= 0 ? '+' : ''}{formatBps(brinsonData.allocation_effect)}
                        </div>
                        <div className="text-xs text-gray-500 mt-1">Sector weighting decisions</div>
                      </div>
                      <div className="border-l-4 border-green-500 pl-4">
                        <div className="text-sm text-gray-600">Selection Effect</div>
                        <div className={`text-2xl font-bold ${(brinsonData.selection_effect || 0) >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                          {(brinsonData.selection_effect || 0) >= 0 ? '+' : ''}{formatBps(brinsonData.selection_effect)}
                        </div>
                        <div className="text-xs text-gray-500 mt-1">Security selection within sectors</div>
                      </div>
                      <div className="border-l-4 border-purple-500 pl-4">
                        <div className="text-sm text-gray-600">Interaction Effect</div>
                        <div className={`text-2xl font-bold ${(brinsonData.interaction_effect || 0) >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                          {(brinsonData.interaction_effect || 0) >= 0 ? '+' : ''}{formatBps(brinsonData.interaction_effect)}
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
                          // Only show unattributed if it exists and is significant (> 0.1%)
                          ...(brinsonData.unattributed && Math.abs(brinsonData.unattributed) > 0.001
                            ? [{ label: 'Other/Unattrib.', value: brinsonData.unattributed, color: 'gray' }]
                            : []),
                        ].map(({ label, value, color }) => {
                          const maxVal = Math.max(
                            Math.abs(brinsonData.allocation_effect || 0),
                            Math.abs(brinsonData.selection_effect || 0),
                            Math.abs(brinsonData.interaction_effect || 0),
                            Math.abs(brinsonData.unattributed || 0),
                            0.001
                          );
                          const barWidth = Math.abs(value || 0) / maxVal * 100;
                          const isPositive = (value || 0) >= 0;

                          return (
                            <div key={label} className="flex items-center gap-3">
                              <div className="w-24 text-sm text-gray-700">{label}</div>
                              <div className="flex-1 h-6 bg-gray-100 rounded relative">
                                <div className="absolute inset-y-0 left-1/2 w-px bg-gray-300"></div>
                                <div
                                  className={`absolute top-0 h-full rounded ${
                                    isPositive ? 'left-1/2' : 'right-1/2'
                                  }`}
                                  style={{
                                    width: `${barWidth / 2}%`,
                                    backgroundColor: isPositive
                                      ? (color === 'blue' ? '#3b82f6' : color === 'green' ? '#22c55e' : color === 'purple' ? '#a855f7' : '#9ca3af')
                                      : (color === 'blue' ? '#93c5fd' : color === 'green' ? '#86efac' : color === 'purple' ? '#d8b4fe' : '#d1d5db'),
                                  }}
                                ></div>
                              </div>
                              <div className={`w-24 text-right text-sm font-semibold ${isPositive ? 'text-green-600' : 'text-red-600'}`}>
                                {isPositive ? '+' : ''}{formatBps(value)}
                              </div>
                            </div>
                          );
                        })}
                        {/* Total row */}
                        <div className="flex items-center gap-3 pt-2 border-t mt-2">
                          <div className="w-24 text-sm font-semibold text-gray-900">Total Active</div>
                          <div className="flex-1"></div>
                          <div className={`w-24 text-right text-sm font-bold ${(brinsonData.total_active_return || 0) >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                            {(brinsonData.total_active_return || 0) >= 0 ? '+' : ''}{formatBps(brinsonData.total_active_return)}
                          </div>
                        </div>
                      </div>
                    </div>

                    {/* Top Drivers Panels */}
                    <div className="grid grid-cols-1 md:grid-cols-2 gap-4 mb-6">
                      {/* Top Contributors */}
                      <div className="border rounded-lg p-4 bg-green-50">
                        <h3 className="text-sm font-semibold text-green-800 mb-3">Top 5 Contributors</h3>
                        {brinsonData.top_contributors && brinsonData.top_contributors.length > 0 ? (
                          <div className="space-y-2">
                            {brinsonData.top_contributors.map((sector: any, idx: number) => {
                              const isExpanded = expandedDrivers.has(`contrib-${sector.sector}`);
                              return (
                                <div key={idx} className="bg-white rounded border border-green-200">
                                  <button
                                    onClick={() => {
                                      const newSet = new Set(expandedDrivers);
                                      if (isExpanded) newSet.delete(`contrib-${sector.sector}`);
                                      else newSet.add(`contrib-${sector.sector}`);
                                      setExpandedDrivers(newSet);
                                    }}
                                    className="w-full flex items-center justify-between p-2 hover:bg-green-50"
                                  >
                                    <span className="text-sm font-medium text-gray-800">{sector.sector}</span>
                                    <span className="text-sm font-bold text-green-600">+{formatBps(sector.total_effect)}</span>
                                  </button>
                                  {isExpanded && (
                                    <div className="px-3 pb-2 pt-1 border-t text-xs text-gray-600 space-y-1">
                                      <div className="flex justify-between">
                                        <span>Portfolio Weight:</span>
                                        <span className="font-medium">{formatPercent(sector.portfolio_weight)}</span>
                                      </div>
                                      <div className="flex justify-between">
                                        <span>Benchmark Weight:</span>
                                        <span className="font-medium">{formatPercent(sector.benchmark_weight)}</span>
                                      </div>
                                      <div className="flex justify-between">
                                        <span>Active Weight:</span>
                                        <span className={`font-medium ${(sector.portfolio_weight - sector.benchmark_weight) >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                                          {(sector.portfolio_weight - sector.benchmark_weight) >= 0 ? '+' : ''}{formatPercent(sector.portfolio_weight - sector.benchmark_weight)}
                                        </span>
                                      </div>
                                      <div className="flex justify-between">
                                        <span>Portfolio Return:</span>
                                        <span className="font-medium">{formatPercent(sector.portfolio_return)}</span>
                                      </div>
                                      <div className="flex justify-between">
                                        <span>Benchmark Return:</span>
                                        <span className="font-medium">{formatPercent(sector.benchmark_return)}</span>
                                      </div>
                                      <div className="border-t pt-1 mt-1">
                                        <div className="flex justify-between">
                                          <span>Allocation:</span>
                                          <span className={`font-medium ${(sector.allocation_effect || 0) >= 0 ? 'text-blue-600' : 'text-blue-400'}`}>{formatBps(sector.allocation_effect)}</span>
                                        </div>
                                        <div className="flex justify-between">
                                          <span>Selection:</span>
                                          <span className={`font-medium ${(sector.selection_effect || 0) >= 0 ? 'text-green-600' : 'text-green-400'}`}>{formatBps(sector.selection_effect)}</span>
                                        </div>
                                        <div className="flex justify-between">
                                          <span>Interaction:</span>
                                          <span className={`font-medium ${(sector.interaction_effect || 0) >= 0 ? 'text-purple-600' : 'text-purple-400'}`}>{formatBps(sector.interaction_effect)}</span>
                                        </div>
                                      </div>
                                    </div>
                                  )}
                                </div>
                              );
                            })}
                          </div>
                        ) : (
                          <p className="text-sm text-gray-500 italic">No positive contributors</p>
                        )}
                      </div>

                      {/* Top Detractors */}
                      <div className="border rounded-lg p-4 bg-red-50">
                        <h3 className="text-sm font-semibold text-red-800 mb-3">Top 5 Detractors</h3>
                        {brinsonData.top_detractors && brinsonData.top_detractors.length > 0 ? (
                          <div className="space-y-2">
                            {brinsonData.top_detractors.map((sector: any, idx: number) => {
                              const isExpanded = expandedDrivers.has(`detract-${sector.sector}`);
                              return (
                                <div key={idx} className="bg-white rounded border border-red-200">
                                  <button
                                    onClick={() => {
                                      const newSet = new Set(expandedDrivers);
                                      if (isExpanded) newSet.delete(`detract-${sector.sector}`);
                                      else newSet.add(`detract-${sector.sector}`);
                                      setExpandedDrivers(newSet);
                                    }}
                                    className="w-full flex items-center justify-between p-2 hover:bg-red-50"
                                  >
                                    <span className="text-sm font-medium text-gray-800">{sector.sector}</span>
                                    <span className="text-sm font-bold text-red-600">{formatBps(sector.total_effect)}</span>
                                  </button>
                                  {isExpanded && (
                                    <div className="px-3 pb-2 pt-1 border-t text-xs text-gray-600 space-y-1">
                                      <div className="flex justify-between">
                                        <span>Portfolio Weight:</span>
                                        <span className="font-medium">{formatPercent(sector.portfolio_weight)}</span>
                                      </div>
                                      <div className="flex justify-between">
                                        <span>Benchmark Weight:</span>
                                        <span className="font-medium">{formatPercent(sector.benchmark_weight)}</span>
                                      </div>
                                      <div className="flex justify-between">
                                        <span>Active Weight:</span>
                                        <span className={`font-medium ${(sector.portfolio_weight - sector.benchmark_weight) >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                                          {(sector.portfolio_weight - sector.benchmark_weight) >= 0 ? '+' : ''}{formatPercent(sector.portfolio_weight - sector.benchmark_weight)}
                                        </span>
                                      </div>
                                      <div className="flex justify-between">
                                        <span>Portfolio Return:</span>
                                        <span className="font-medium">{formatPercent(sector.portfolio_return)}</span>
                                      </div>
                                      <div className="flex justify-between">
                                        <span>Benchmark Return:</span>
                                        <span className="font-medium">{formatPercent(sector.benchmark_return)}</span>
                                      </div>
                                      <div className="border-t pt-1 mt-1">
                                        <div className="flex justify-between">
                                          <span>Allocation:</span>
                                          <span className={`font-medium ${(sector.allocation_effect || 0) >= 0 ? 'text-blue-600' : 'text-blue-400'}`}>{formatBps(sector.allocation_effect)}</span>
                                        </div>
                                        <div className="flex justify-between">
                                          <span>Selection:</span>
                                          <span className={`font-medium ${(sector.selection_effect || 0) >= 0 ? 'text-green-600' : 'text-green-400'}`}>{formatBps(sector.selection_effect)}</span>
                                        </div>
                                        <div className="flex justify-between">
                                          <span>Interaction:</span>
                                          <span className={`font-medium ${(sector.interaction_effect || 0) >= 0 ? 'text-purple-600' : 'text-purple-400'}`}>{formatBps(sector.interaction_effect)}</span>
                                        </div>
                                      </div>
                                    </div>
                                  )}
                                </div>
                              );
                            })}
                          </div>
                        ) : (
                          <p className="text-sm text-gray-500 italic">No negative detractors</p>
                        )}
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
                                <th className="text-right py-2 px-2" title="Allocation Effect (pp)">Alloc (pp)</th>
                                <th className="text-right py-2 px-2" title="Selection Effect (pp)">Select (pp)</th>
                                <th className="text-right py-2 px-2" title="Interaction Effect (pp)">Inter (pp)</th>
                                <th className="text-right py-2 px-2 font-semibold" title="Total Effect (pp)">Total (pp)</th>
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
                                      {(sector.allocation_effect || 0) >= 0 ? '+' : ''}{formatBps(sector.allocation_effect)}
                                    </td>
                                    <td className={`py-2 px-2 text-right ${(sector.selection_effect || 0) >= 0 ? 'text-green-600' : 'text-green-400'}`}>
                                      {(sector.selection_effect || 0) >= 0 ? '+' : ''}{formatBps(sector.selection_effect)}
                                    </td>
                                    <td className={`py-2 px-2 text-right ${(sector.interaction_effect || 0) >= 0 ? 'text-purple-600' : 'text-purple-400'}`}>
                                      {(sector.interaction_effect || 0) >= 0 ? '+' : ''}{formatBps(sector.interaction_effect)}
                                    </td>
                                    <td className={`py-2 px-2 text-right font-semibold ${(sector.total_effect || 0) >= 0 ? 'text-green-700' : 'text-red-700'}`}>
                                      {(sector.total_effect || 0) >= 0 ? '+' : ''}{formatBps(sector.total_effect)}
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
                                  {(brinsonData.allocation_effect || 0) >= 0 ? '+' : ''}{formatBps(brinsonData.allocation_effect)}
                                </td>
                                <td className={`py-2 px-2 text-right ${(brinsonData.selection_effect || 0) >= 0 ? 'text-green-600' : 'text-green-400'}`}>
                                  {(brinsonData.selection_effect || 0) >= 0 ? '+' : ''}{formatBps(brinsonData.selection_effect)}
                                </td>
                                <td className={`py-2 px-2 text-right ${(brinsonData.interaction_effect || 0) >= 0 ? 'text-purple-600' : 'text-purple-400'}`}>
                                  {(brinsonData.interaction_effect || 0) >= 0 ? '+' : ''}{formatBps(brinsonData.interaction_effect)}
                                </td>
                                <td className={`py-2 px-2 text-right ${(brinsonData.total_active_return || 0) >= 0 ? 'text-green-700' : 'text-red-700'}`}>
                                  {(brinsonData.total_active_return || 0) >= 0 ? '+' : ''}{formatBps(brinsonData.total_active_return)}
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

            {/* Factor Benchmarking + Attribution (uses free data sources: Stooq, yfinance) */}
            <div className="card">
              {/* Header with Controls */}
              <div className="flex flex-wrap justify-between items-start gap-4 mb-4">
                <h2 className="text-xl font-bold">Factor Benchmarking + Attribution</h2>
                <div className="flex flex-wrap gap-2 items-center">
                  {/* Period Toggle */}
                  <div className="flex gap-1">
                    {(['1M', '3M', '6M', 'YTD', '1Y', 'ALL'] as FactorBenchPeriod[]).map((period) => (
                      <button
                        key={period}
                        onClick={() => setFactorBenchPeriod(period)}
                        className={`px-3 py-1 text-sm rounded ${
                          factorBenchPeriod === period
                            ? 'bg-indigo-600 text-white'
                            : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                        }`}
                      >
                        {period === 'ALL' ? 'All' : period}
                      </button>
                    ))}
                  </div>
                </div>
              </div>

              {/* Settings Row */}
              <div className="flex flex-wrap gap-4 mb-4 p-3 bg-gray-50 rounded-lg">
                {/* Excess Returns Toggle */}
                <label className="flex items-center gap-2 cursor-pointer" title="Subtract risk-free rate from returns before regression">
                  <input
                    type="checkbox"
                    checked={useExcessReturns}
                    onChange={(e) => setUseExcessReturns(e.target.checked)}
                    className="w-4 h-4 rounded border-gray-300"
                  />
                  <span className="text-sm text-gray-700">Excess Returns</span>
                  <span className="text-xs text-gray-400" title="Uses 5% annual risk-free rate">(?)</span>
                </label>

                {/* Robust Mode Toggle */}
                <label className="flex items-center gap-2 cursor-pointer" title="Winsorize returns at 2.5% to reduce outlier impact">
                  <input
                    type="checkbox"
                    checked={useRobustMode}
                    onChange={(e) => setUseRobustMode(e.target.checked)}
                    className="w-4 h-4 rounded border-gray-300"
                  />
                  <span className="text-sm text-gray-700">Robust Mode</span>
                </label>

                {/* Benchmark Selector */}
                <div className="flex items-center gap-2">
                  <span className="text-sm text-gray-700">vs Benchmark:</span>
                  <select
                    value={selectedBenchmark || ''}
                    onChange={(e) => setSelectedBenchmark(e.target.value || null)}
                    className="text-sm border border-gray-300 rounded px-2 py-1"
                  >
                    <option value="">None</option>
                    {availableBenchmarks.map((bm) => (
                      <option key={bm.code} value={bm.code}>{bm.name}</option>
                    ))}
                  </select>
                </div>

                {/* Show Diagnostics Toggle */}
                <label className="flex items-center gap-2 cursor-pointer">
                  <input
                    type="checkbox"
                    checked={showDiagnostics}
                    onChange={(e) => setShowDiagnostics(e.target.checked)}
                    className="w-4 h-4 rounded border-gray-300"
                  />
                  <span className="text-sm text-gray-700">Show Diagnostics</span>
                </label>
              </div>

              {factorBenchLoading ? (
                <div className="py-8 text-center text-gray-500">Loading factor analysis...</div>
              ) : factorBenchmarking ? (
                <>
                  {/* Warnings Banner */}
                  {factorBenchmarking.warnings && factorBenchmarking.warnings.length > 0 && (
                    <div className="mb-4 space-y-2">
                      {factorBenchmarking.warnings.map((warning: any, idx: number) => (
                        <div
                          key={idx}
                          className={`p-3 rounded-lg text-sm ${
                            warning.severity === 'error'
                              ? 'bg-red-50 border border-red-200 text-red-800'
                              : 'bg-yellow-50 border border-yellow-200 text-yellow-800'
                          }`}
                        >
                          <span className="font-medium">
                            {warning.severity === 'error' ? '!' : 'Note:'}
                          </span>{' '}
                          {warning.message}
                        </div>
                      ))}
                    </div>
                  )}

                  {/* Summary Stats */}
                  <div className="grid grid-cols-2 md:grid-cols-5 gap-4 mb-6">
                    <div className="bg-gray-50 rounded-lg p-3">
                      <div className="text-sm text-gray-600">
                        {useExcessReturns ? 'Excess Return' : 'Total Return'}
                      </div>
                      <div className={`text-xl font-bold ${factorBenchmarking.total_return >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                        {factorBenchmarking.total_return_pct?.toFixed(2)}%
                      </div>
                    </div>
                    <div className="bg-gray-50 rounded-lg p-3" title={`95% CI: [${factorBenchmarking.regression?.alpha_ci?.lower?.toFixed(2)}%, ${factorBenchmarking.regression?.alpha_ci?.upper?.toFixed(2)}%]`}>
                      <div className="text-sm text-gray-600">Alpha (Ann.)</div>
                      <div className={`text-xl font-bold ${(factorBenchmarking.regression?.alpha_annualized || 0) >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                        {factorBenchmarking.regression?.alpha_annualized?.toFixed(2)}%
                      </div>
                      <div className="text-xs text-gray-400">
                        IR: {factorBenchmarking.regression?.alpha_ir?.toFixed(2)}
                      </div>
                    </div>
                    <div className="bg-gray-50 rounded-lg p-3">
                      <div className="text-sm text-gray-600">Adj. R-Squared</div>
                      <div className="text-xl font-bold text-gray-800">
                        {(factorBenchmarking.regression?.adj_r_squared * 100)?.toFixed(1)}%
                      </div>
                      <div className="text-xs text-gray-400">
                        (R²: {(factorBenchmarking.regression?.r_squared * 100)?.toFixed(1)}%)
                      </div>
                    </div>
                    <div className="bg-gray-50 rounded-lg p-3">
                      <div className="text-sm text-gray-600">Residual Vol (Ann.)</div>
                      <div className="text-xl font-bold text-gray-800">
                        {factorBenchmarking.regression?.residual_std_ann?.toFixed(2)}%
                      </div>
                    </div>
                    <div className="bg-gray-50 rounded-lg p-3">
                      <div className="text-sm text-gray-600">Factor Explained</div>
                      <div className="text-xl font-bold text-indigo-600">
                        {factorBenchmarking.factor_explained?.toFixed(2)}%
                      </div>
                      <div className="text-xs text-gray-400">
                        ({factorBenchmarking.factor_explained_pct?.toFixed(0)}% of return)
                      </div>
                    </div>
                  </div>

                  {/* Table Controls */}
                  <div className="flex flex-wrap gap-4 mb-3">
                    <div className="flex items-center gap-2">
                      <span className="text-sm text-gray-600">Sort by:</span>
                      <select
                        value={factorSortBy}
                        onChange={(e) => setFactorSortBy(e.target.value)}
                        className="text-sm border border-gray-300 rounded px-2 py-1"
                      >
                        <option value="contribution">Contribution</option>
                        <option value="beta">Beta</option>
                        <option value="t_stat">t-Stat</option>
                        <option value="name">Name</option>
                      </select>
                    </div>
                    <label className="flex items-center gap-2 cursor-pointer">
                      <input
                        type="checkbox"
                        checked={hideInsignificant}
                        onChange={(e) => setHideInsignificant(e.target.checked)}
                        className="w-4 h-4 rounded border-gray-300"
                      />
                      <span className="text-sm text-gray-600">Hide insignificant (p &gt; 0.10)</span>
                    </label>
                  </div>

                  {/* Factor Exposures and Attribution Table */}
                  <div className="overflow-x-auto">
                    <table className="w-full text-sm">
                      <thead className="bg-gray-50">
                        <tr>
                          <th className="text-left py-2 px-3">Factor</th>
                          <th className="text-right py-2 px-3" title="Factor exposure coefficient">Beta</th>
                          <th className="text-right py-2 px-3" title="95% confidence interval">Beta CI</th>
                          <th className="text-right py-2 px-3">Factor Return</th>
                          <th className="text-right py-2 px-3" title="Beta × Factor Return">Contribution</th>
                          <th className="text-right py-2 px-3" title="% of total return explained by this factor">% of Total</th>
                          <th className="text-right py-2 px-3">t-Stat</th>
                          <th className="text-right py-2 px-3" title="Variance Inflation Factor - warn if > 5">VIF</th>
                          <th className="text-right py-2 px-3" title="*p<0.10, **p<0.05, ***p<0.01">Sig.</th>
                        </tr>
                      </thead>
                      <tbody>
                        {Object.entries(factorBenchmarking.factor_contributions || {})
                          .filter(([_, factor]: [string, any]) => !hideInsignificant || factor.p_value < 0.10)
                          .sort((a: any, b: any) => {
                            const [_, fa] = a;
                            const [__, fb] = b;
                            if (factorSortBy === 'contribution') return Math.abs(fb.contribution) - Math.abs(fa.contribution);
                            if (factorSortBy === 'beta') return Math.abs(fb.beta) - Math.abs(fa.beta);
                            if (factorSortBy === 't_stat') return Math.abs(fb.t_stat) - Math.abs(fa.t_stat);
                            return fa.name.localeCompare(fb.name);
                          })
                          .map(([key, factor]: [string, any]) => (
                          <tr key={key} className="border-b border-gray-100 hover:bg-gray-50">
                            <td className="py-2 px-3 font-medium">{factor.name}</td>
                            <td className="text-right py-2 px-3">{factor.beta?.toFixed(3)}</td>
                            <td className="text-right py-2 px-3 text-xs text-gray-500">
                              [{factor.beta_ci?.lower?.toFixed(2)}, {factor.beta_ci?.upper?.toFixed(2)}]
                            </td>
                            <td className={`text-right py-2 px-3 ${(factor.factor_return || 0) >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                              {factor.factor_return?.toFixed(2)}%
                            </td>
                            <td className={`text-right py-2 px-3 font-medium ${(factor.contribution || 0) >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                              {factor.contribution?.toFixed(2)}%
                            </td>
                            <td className="text-right py-2 px-3">
                              {factor.contribution_pct?.toFixed(1)}%
                            </td>
                            <td className="text-right py-2 px-3">{factor.t_stat?.toFixed(2)}</td>
                            <td className={`text-right py-2 px-3 ${factor.vif > 10 ? 'text-red-600 font-bold' : factor.vif > 5 ? 'text-yellow-600' : ''}`}>
                              {factor.vif?.toFixed(1)}
                            </td>
                            <td className="text-right py-2 px-3">
                              {factor.p_value < 0.01 ? (
                                <span className="text-green-600 font-semibold">***</span>
                              ) : factor.p_value < 0.05 ? (
                                <span className="text-green-600">**</span>
                              ) : factor.p_value < 0.10 ? (
                                <span className="text-yellow-600">*</span>
                              ) : (
                                <span className="text-gray-400">-</span>
                              )}
                            </td>
                          </tr>
                        ))}
                        {/* Alpha row */}
                        <tr className="border-b border-gray-100 bg-indigo-50">
                          <td className="py-2 px-3 font-medium">Alpha (Skill)</td>
                          <td className="text-right py-2 px-3">-</td>
                          <td className="text-right py-2 px-3 text-xs text-gray-500">
                            [{factorBenchmarking.regression?.alpha_ci?.lower?.toFixed(2)}%, {factorBenchmarking.regression?.alpha_ci?.upper?.toFixed(2)}%]
                          </td>
                          <td className="text-right py-2 px-3">-</td>
                          <td className={`text-right py-2 px-3 font-medium ${(factorBenchmarking.alpha_contribution || 0) >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                            {factorBenchmarking.alpha_contribution?.toFixed(2)}%
                          </td>
                          <td className="text-right py-2 px-3">
                            {factorBenchmarking.alpha_contribution_pct?.toFixed(1)}%
                          </td>
                          <td colSpan={3} className="text-right py-2 px-3 text-xs text-gray-500">
                            IR: {factorBenchmarking.regression?.alpha_ir?.toFixed(2)}
                          </td>
                        </tr>
                        {/* Residual row */}
                        <tr className="border-b border-gray-100 bg-gray-50">
                          <td className="py-2 px-3 font-medium">Residual (Unexplained)</td>
                          <td className="text-right py-2 px-3">-</td>
                          <td className="text-right py-2 px-3">-</td>
                          <td className="text-right py-2 px-3">-</td>
                          <td className={`text-right py-2 px-3 font-medium ${(factorBenchmarking.residual_contribution || 0) >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                            {factorBenchmarking.residual_contribution?.toFixed(2)}%
                          </td>
                          <td className="text-right py-2 px-3">
                            {factorBenchmarking.residual_contribution_pct?.toFixed(1)}%
                          </td>
                          <td colSpan={3}></td>
                        </tr>
                      </tbody>
                    </table>
                  </div>

                  {/* Benchmark-Relative Attribution */}
                  {factorBenchmarking.benchmark_attribution && (
                    <div className="mt-6 p-4 bg-blue-50 rounded-lg border border-blue-200">
                      <h3 className="font-semibold text-blue-900 mb-3">
                        Active Attribution vs {factorBenchmarking.benchmark_attribution.benchmark_name}
                      </h3>
                      <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-4">
                        <div>
                          <div className="text-sm text-blue-700">Portfolio Return</div>
                          <div className={`text-lg font-bold ${factorBenchmarking.benchmark_attribution.portfolio_return >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                            {factorBenchmarking.benchmark_attribution.portfolio_return?.toFixed(2)}%
                          </div>
                        </div>
                        <div>
                          <div className="text-sm text-blue-700">Benchmark Return</div>
                          <div className={`text-lg font-bold ${factorBenchmarking.benchmark_attribution.benchmark_return >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                            {factorBenchmarking.benchmark_attribution.benchmark_return?.toFixed(2)}%
                          </div>
                        </div>
                        <div>
                          <div className="text-sm text-blue-700">Active Return</div>
                          <div className={`text-lg font-bold ${factorBenchmarking.benchmark_attribution.active_return >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                            {factorBenchmarking.benchmark_attribution.active_return >= 0 ? '+' : ''}
                            {factorBenchmarking.benchmark_attribution.active_return?.toFixed(2)}%
                          </div>
                        </div>
                        <div>
                          <div className="text-sm text-blue-700">Active Alpha</div>
                          <div className={`text-lg font-bold ${factorBenchmarking.benchmark_attribution.active_alpha >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                            {factorBenchmarking.benchmark_attribution.active_alpha >= 0 ? '+' : ''}
                            {factorBenchmarking.benchmark_attribution.active_alpha?.toFixed(2)}%
                          </div>
                        </div>
                      </div>

                      {/* Active Factor Tilts */}
                      <div className="mt-3">
                        <div className="text-sm font-medium text-blue-800 mb-2">Active Factor Contributions:</div>
                        <div className="flex flex-wrap gap-2">
                          {Object.entries(factorBenchmarking.benchmark_attribution.active_factor_contributions || {}).map(([factor, contrib]: [string, any]) => (
                            <div key={factor} className={`px-2 py-1 rounded text-xs ${contrib >= 0 ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800'}`}>
                              {factor}: {contrib >= 0 ? '+' : ''}{contrib?.toFixed(2)}%
                            </div>
                          ))}
                        </div>
                      </div>
                    </div>
                  )}

                  {/* Diagnostics Panel */}
                  {showDiagnostics && factorBenchmarking.diagnostics && (
                    <div className="mt-6 border-t pt-4">
                      <h3 className="font-semibold text-gray-700 mb-4">Regression Diagnostics</h3>

                      <div className="grid grid-cols-1 md:grid-cols-2 gap-6">
                        {/* VIF Summary */}
                        <div className="bg-gray-50 rounded-lg p-4">
                          <h4 className="text-sm font-medium text-gray-700 mb-2">Multicollinearity (VIF)</h4>
                          <div className="space-y-1">
                            {Object.entries(factorBenchmarking.diagnostics.vif || {}).map(([factor, vif]: [string, any]) => (
                              <div key={factor} className="flex justify-between text-sm">
                                <span>{factor}</span>
                                <span className={`font-medium ${vif > 10 ? 'text-red-600' : vif > 5 ? 'text-yellow-600' : 'text-green-600'}`}>
                                  {vif?.toFixed(2)}
                                  {vif > 10 ? ' (Severe!)' : vif > 5 ? ' (Warning)' : ''}
                                </span>
                              </div>
                            ))}
                          </div>
                          <div className="mt-2 text-xs text-gray-500">
                            Max VIF: {factorBenchmarking.diagnostics.max_vif?.toFixed(2)} |
                            {factorBenchmarking.diagnostics.multicollinearity_severe
                              ? ' Severe multicollinearity detected!'
                              : factorBenchmarking.diagnostics.multicollinearity_warning
                                ? ' Moderate multicollinearity detected'
                                : ' No multicollinearity issues'}
                          </div>
                        </div>

                        {/* Residual Diagnostics */}
                        <div className="bg-gray-50 rounded-lg p-4">
                          <h4 className="text-sm font-medium text-gray-700 mb-2">Residual Tests</h4>
                          <div className="space-y-2 text-sm">
                            <div className="flex justify-between">
                              <span>Durbin-Watson</span>
                              <span className={`font-medium ${
                                factorBenchmarking.diagnostics.residual_diagnostics?.durbin_watson < 1.5 ||
                                factorBenchmarking.diagnostics.residual_diagnostics?.durbin_watson > 2.5
                                  ? 'text-yellow-600' : 'text-green-600'
                              }`}>
                                {factorBenchmarking.diagnostics.residual_diagnostics?.durbin_watson?.toFixed(2)}
                              </span>
                            </div>
                            <div className="text-xs text-gray-500">
                              {factorBenchmarking.diagnostics.residual_diagnostics?.dw_interpretation}
                            </div>
                            <div className="flex justify-between mt-2">
                              <span>Jarque-Bera (Normality)</span>
                              <span className={`font-medium ${
                                factorBenchmarking.diagnostics.residual_diagnostics?.normality_ok ? 'text-green-600' : 'text-yellow-600'
                              }`}>
                                {factorBenchmarking.diagnostics.residual_diagnostics?.normality_ok ? 'OK' : 'Non-normal'}
                              </span>
                            </div>
                            <div className="flex justify-between">
                              <span>Breusch-Pagan (Homosked.)</span>
                              <span className={`font-medium ${
                                factorBenchmarking.diagnostics.residual_diagnostics?.homoskedasticity_ok ? 'text-green-600' : 'text-yellow-600'
                              }`}>
                                {factorBenchmarking.diagnostics.residual_diagnostics?.homoskedasticity_ok ? 'OK' : 'Heteroskedastic'}
                              </span>
                            </div>
                          </div>
                        </div>

                        {/* Factor Correlation Matrix */}
                        {factorBenchmarking.diagnostics.factor_correlations && (
                          <div className="bg-gray-50 rounded-lg p-4 md:col-span-2">
                            <h4 className="text-sm font-medium text-gray-700 mb-2">Factor Correlation Matrix</h4>
                            <div className="overflow-x-auto">
                              <table className="text-xs">
                                <thead>
                                  <tr>
                                    <th className="px-2 py-1"></th>
                                    {factorBenchmarking.diagnostics.factor_correlations.factors?.map((f: string) => (
                                      <th key={f} className="px-2 py-1 font-medium">{f}</th>
                                    ))}
                                  </tr>
                                </thead>
                                <tbody>
                                  {factorBenchmarking.diagnostics.factor_correlations.factors?.map((row: string) => (
                                    <tr key={row}>
                                      <td className="px-2 py-1 font-medium">{row}</td>
                                      {factorBenchmarking.diagnostics.factor_correlations.factors?.map((col: string) => {
                                        const corr = factorBenchmarking.diagnostics.factor_correlations.matrix?.[row]?.[col];
                                        const absCorr = Math.abs(corr || 0);
                                        const bgColor = row === col ? 'bg-gray-200' :
                                          absCorr > 0.7 ? 'bg-red-100' :
                                          absCorr > 0.5 ? 'bg-yellow-100' : '';
                                        return (
                                          <td key={col} className={`px-2 py-1 text-center ${bgColor}`}>
                                            {corr?.toFixed(2)}
                                          </td>
                                        );
                                      })}
                                    </tr>
                                  ))}
                                </tbody>
                              </table>
                            </div>
                            <div className="mt-2 text-xs text-gray-500">
                              High correlations (&gt;0.7) highlighted in red, moderate (&gt;0.5) in yellow
                            </div>
                          </div>
                        )}

                        {/* Outliers */}
                        {factorBenchmarking.diagnostics.outliers && factorBenchmarking.diagnostics.outliers.length > 0 && (
                          <div className="bg-gray-50 rounded-lg p-4 md:col-span-2">
                            <h4 className="text-sm font-medium text-gray-700 mb-2">
                              Extreme Return Days ({factorBenchmarking.diagnostics.outlier_count} detected)
                            </h4>
                            <div className="flex flex-wrap gap-2">
                              {factorBenchmarking.diagnostics.outliers.slice(0, 5).map((outlier: any, idx: number) => (
                                <div key={idx} className={`px-2 py-1 rounded text-xs ${outlier.return >= 0 ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800'}`}>
                                  {outlier.date}: {outlier.return >= 0 ? '+' : ''}{outlier.return?.toFixed(2)}%
                                </div>
                              ))}
                            </div>
                            {useRobustMode && (
                              <div className="mt-2 text-xs text-green-600">
                                Robust mode enabled: returns winsorized at 2.5% to reduce outlier impact
                              </div>
                            )}
                          </div>
                        )}
                      </div>

                      {/* Rolling Analysis */}
                      {factorRollingData && factorRollingData.rolling_data && (
                        <div className="mt-6">
                          <div className="flex items-center gap-4 mb-3">
                            <h4 className="text-sm font-medium text-gray-700">Rolling Analysis</h4>
                            <select
                              value={rollingWindow}
                              onChange={(e) => setRollingWindow(Number(e.target.value))}
                              className="text-sm border border-gray-300 rounded px-2 py-1"
                            >
                              <option value={30}>30-Day</option>
                              <option value={63}>63-Day (3M)</option>
                              <option value={126}>126-Day (6M)</option>
                              <option value={252}>252-Day (1Y)</option>
                            </select>
                          </div>

                          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
                            {/* Rolling Alpha */}
                            <div className="bg-white border rounded p-3">
                              <div className="text-xs text-gray-500 mb-1">Rolling Alpha (Ann.)</div>
                              <div className="h-24 flex items-end gap-px">
                                {factorRollingData.rolling_data.slice(-30).map((d: any, i: number) => {
                                  const val = d.alpha_ann || 0;
                                  const maxAbs = Math.max(...factorRollingData.rolling_data.slice(-30).map((x: any) => Math.abs(x.alpha_ann || 0)));
                                  const height = Math.abs(val) / (maxAbs || 1) * 100;
                                  return (
                                    <div
                                      key={i}
                                      className={`flex-1 ${val >= 0 ? 'bg-green-400' : 'bg-red-400'}`}
                                      style={{ height: `${Math.max(height, 2)}%` }}
                                      title={`${d.date}: ${val?.toFixed(2)}%`}
                                    />
                                  );
                                })}
                              </div>
                              <div className="text-xs text-gray-500 mt-1">
                                Latest: {factorRollingData.rolling_data[factorRollingData.rolling_data.length - 1]?.alpha_ann?.toFixed(2)}%
                              </div>
                            </div>

                            {/* Rolling R² */}
                            <div className="bg-white border rounded p-3">
                              <div className="text-xs text-gray-500 mb-1">Rolling R²</div>
                              <div className="h-24 flex items-end gap-px">
                                {factorRollingData.rolling_data.slice(-30).map((d: any, i: number) => {
                                  const val = (d.r_squared || 0) * 100;
                                  return (
                                    <div
                                      key={i}
                                      className="flex-1 bg-indigo-400"
                                      style={{ height: `${val}%` }}
                                      title={`${d.date}: ${val?.toFixed(1)}%`}
                                    />
                                  );
                                })}
                              </div>
                              <div className="text-xs text-gray-500 mt-1">
                                Latest: {(factorRollingData.rolling_data[factorRollingData.rolling_data.length - 1]?.r_squared * 100)?.toFixed(1)}%
                              </div>
                            </div>

                            {/* Rolling Residual Vol */}
                            <div className="bg-white border rounded p-3">
                              <div className="text-xs text-gray-500 mb-1">Rolling Tracking Error (Ann.)</div>
                              <div className="h-24 flex items-end gap-px">
                                {factorRollingData.rolling_data.slice(-30).map((d: any, i: number) => {
                                  const val = d.residual_vol_ann || 0;
                                  const maxVal = Math.max(...factorRollingData.rolling_data.slice(-30).map((x: any) => x.residual_vol_ann || 0));
                                  const height = val / (maxVal || 1) * 100;
                                  return (
                                    <div
                                      key={i}
                                      className="flex-1 bg-orange-400"
                                      style={{ height: `${Math.max(height, 2)}%` }}
                                      title={`${d.date}: ${val?.toFixed(2)}%`}
                                    />
                                  );
                                })}
                              </div>
                              <div className="text-xs text-gray-500 mt-1">
                                Latest: {factorRollingData.rolling_data[factorRollingData.rolling_data.length - 1]?.residual_vol_ann?.toFixed(2)}%
                              </div>
                            </div>
                          </div>
                        </div>
                      )}

                      {/* Contribution Over Time */}
                      {factorContribOverTime && factorContribOverTime.periods && (
                        <div className="mt-6">
                          <div className="flex items-center gap-4 mb-3">
                            <h4 className="text-sm font-medium text-gray-700">Factor Contribution Over Time</h4>
                            <select
                              value={contribFrequency}
                              onChange={(e) => setContribFrequency(e.target.value as 'M' | 'Q')}
                              className="text-sm border border-gray-300 rounded px-2 py-1"
                            >
                              <option value="M">Monthly</option>
                              <option value="Q">Quarterly</option>
                            </select>
                          </div>

                          <div className="overflow-x-auto">
                            <table className="w-full text-xs">
                              <thead className="bg-gray-50">
                                <tr>
                                  <th className="text-left py-2 px-2">Period</th>
                                  <th className="text-right py-2 px-2">Portfolio</th>
                                  {factorContribOverTime.factors?.map((f: string) => (
                                    <th key={f} className="text-right py-2 px-2">{f}</th>
                                  ))}
                                  <th className="text-right py-2 px-2">Alpha</th>
                                </tr>
                              </thead>
                              <tbody>
                                {factorContribOverTime.periods.slice(-12).map((period: any, idx: number) => (
                                  <tr key={idx} className="border-b border-gray-100">
                                    <td className="py-2 px-2 font-medium">{period.period}</td>
                                    <td className={`text-right py-2 px-2 font-medium ${period.portfolio_return >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                                      {period.portfolio_return?.toFixed(2)}%
                                    </td>
                                    {factorContribOverTime.factors?.map((f: string) => (
                                      <td key={f} className={`text-right py-2 px-2 ${(period.factor_contributions?.[f] || 0) >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                                        {period.factor_contributions?.[f]?.toFixed(2)}%
                                      </td>
                                    ))}
                                    <td className={`text-right py-2 px-2 ${period.alpha_contribution >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                                      {period.alpha_contribution?.toFixed(2)}%
                                    </td>
                                  </tr>
                                ))}
                              </tbody>
                              <tfoot className="bg-gray-100 font-medium">
                                <tr>
                                  <td className="py-2 px-2">Cumulative</td>
                                  <td className={`text-right py-2 px-2 ${(factorContribOverTime.periods[factorContribOverTime.periods.length - 1]?.cumulative_portfolio || 0) >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                                    {factorContribOverTime.periods[factorContribOverTime.periods.length - 1]?.cumulative_portfolio?.toFixed(2)}%
                                  </td>
                                  <td colSpan={factorContribOverTime.factors?.length || 0} className="text-right py-2 px-2">
                                    Factor: {factorContribOverTime.periods[factorContribOverTime.periods.length - 1]?.cumulative_factor_explained?.toFixed(2)}%
                                  </td>
                                  <td className={`text-right py-2 px-2 ${(factorContribOverTime.periods[factorContribOverTime.periods.length - 1]?.cumulative_alpha || 0) >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                                    {factorContribOverTime.periods[factorContribOverTime.periods.length - 1]?.cumulative_alpha?.toFixed(2)}%
                                  </td>
                                </tr>
                              </tfoot>
                            </table>
                          </div>
                        </div>
                      )}
                    </div>
                  )}

                  {/* Period info */}
                  <div className="mt-4 pt-4 border-t text-sm text-gray-500 flex flex-wrap gap-4">
                    <span>Period: {factorBenchmarking.period?.start_date} to {factorBenchmarking.period?.end_date}</span>
                    <span>{factorBenchmarking.regression?.n_observations} trading days</span>
                    {useExcessReturns && (
                      <span>Risk-free rate: {factorBenchmarking.risk_free_rate_annual?.toFixed(1)}% annual</span>
                    )}
                    {useRobustMode && (
                      <span className="text-indigo-600">Robust mode active</span>
                    )}
                  </div>
                </>
              ) : (
                <div className="py-8 text-center">
                  {factorBenchError ? (
                    <div className="bg-red-50 border border-red-200 rounded-lg p-4 max-w-xl mx-auto">
                      <div className="flex items-start gap-3">
                        <svg className="w-5 h-5 text-red-500 mt-0.5 flex-shrink-0" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 8v4m0 4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                        </svg>
                        <div className="text-left">
                          <p className="text-red-800 text-sm font-medium">Factor Analysis Unavailable</p>
                          <p className="text-red-600 text-sm mt-1">{factorBenchError}</p>
                          <p className="text-gray-500 text-xs mt-2">
                            This may be due to external data sources being temporarily unavailable.
                            Try refreshing data from the Data Management tab or try again later.
                          </p>
                        </div>
                      </div>
                    </div>
                  ) : (
                    <div className="text-gray-500">
                      Factor analysis not available. Ensure sufficient return data exists.
                    </div>
                  )}
                </div>
              )}
            </div>
          </>
        )}
      </div>
    </Layout>
  );
}
