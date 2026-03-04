import { useState, useEffect, useMemo, useRef } from 'react';
import dynamic from 'next/dynamic';
import Layout from '@/components/Layout';
import { api } from '@/lib/api';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, PieChart, Pie, Cell } from 'recharts';
import { format, subMonths, startOfYear } from 'date-fns';

// Lazy-load react-select (not needed until user interacts with dropdown)
const Select = dynamic(() => import('react-select'), { ssr: false }) as any;

// Time period options for the performance chart
type ChartPeriod = '1M' | '3M' | 'YTD' | '1Y' | 'ALL';

// Color palette for pie charts - refined
const CHART_COLORS = [
  '#3b82f6', '#10b981', '#8b5cf6', '#f59e0b', '#ef4444',
  '#06b6d4', '#ec4899', '#84cc16', '#f97316', '#6366f1',
  '#14b8a6', '#a855f7', '#eab308', '#22c55e', '#0ea5e9',
  '#d946ef', '#64748b', '#78716c', '#0891b2', '#7c3aed'
];

// Sector chart view modes
type SectorViewMode = 'sector' | 'industry' | 'country' | 'region' | 'market' | 'assetType';

const TOP_N = 20; // Max slices before grouping as "Other"

// Custom Select styles for react-select
const selectStyles = {
  control: (base: any, state: any) => ({
    ...base,
    borderColor: state.isFocused ? '#3b82f6' : '#e4e4e7',
    boxShadow: state.isFocused ? '0 0 0 2px rgba(59, 130, 246, 0.1)' : 'none',
    '&:hover': { borderColor: '#a1a1aa' },
    borderRadius: '0.5rem',
    minHeight: '42px',
  }),
  option: (base: any, state: any) => ({
    ...base,
    backgroundColor: state.isSelected ? '#3b82f6' : state.isFocused ? '#f4f4f5' : 'white',
    color: state.isSelected ? 'white' : '#27272a',
    fontSize: '0.875rem',
  }),
  placeholder: (base: any) => ({ ...base, color: '#a1a1aa', fontSize: '0.875rem' }),
  singleValue: (base: any) => ({ ...base, color: '#27272a', fontSize: '0.875rem' }),
};

// Pre-create Intl formatters (expensive to construct, reuse across renders)
const currencyFmt = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', minimumFractionDigits: 0, maximumFractionDigits: 0 });
const currencyDecFmt = new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD', minimumFractionDigits: 2, maximumFractionDigits: 2 });
const sharesWholeNumberFmt = new Intl.NumberFormat('en-US', { minimumFractionDigits: 0, maximumFractionDigits: 0 });
const sharesDecimalFmt = new Intl.NumberFormat('en-US', { minimumFractionDigits: 2, maximumFractionDigits: 2 });

// Formatting functions (pure, no component state dependency - defined at module level)
const formatCurrency = (value: number) => currencyFmt.format(value);

const formatPercent = (value: number | null | undefined) => {
  if (value === null || value === undefined) return '-';
  return (value * 100).toFixed(2) + '%';
};

const formatPercentWithSign = (value: number | null | undefined) => {
  if (value === null || value === undefined) return '-';
  const pct = (value * 100).toFixed(2);
  return (value >= 0 ? '+' : '') + pct + '%';
};

const formatIndexValue = (value: number) => ((value - 1) * 100).toFixed(2) + '%';

const formatCurrencyWithDecimals = (value: number) => currencyDecFmt.format(value);

const formatGainPercent = (value: number | null | undefined) => {
  if (value === null || value === undefined) return '-';
  const pct = (value * 100).toFixed(2);
  return (value >= 0 ? '+' : '') + pct + '%';
};

const formatGainCurrency = (value: number | null | undefined) => {
  if (value === null || value === undefined) return '-';
  const formatted = currencyFmt.format(Math.abs(value));
  return (value >= 0 ? '+' : '-') + formatted;
};

const formatSharesDisplay = (value: number) => {
  return value >= 1000 ? sharesWholeNumberFmt.format(value) : sharesDecimalFmt.format(value);
};

const getGainColorClass = (value: number | null | undefined) => {
  if (value === null || value === undefined) return 'text-zinc-400';
  return value >= 0 ? 'value-positive' : 'value-negative';
};

export default function Dashboard() {
  const [views, setViews] = useState<any[]>([]);
  const [selectedView, setSelectedView] = useState<any>(null);
  const [summary, setSummary] = useState<any>(null);
  const [returns, setReturns] = useState<any[]>([]);
  const [benchmarkReturns, setBenchmarkReturns] = useState<any>({});
  const [holdings, setHoldings] = useState<any>(null);
  const [risk, setRisk] = useState<any>(null);
  const [unpriced, setUnpriced] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [returnMode, setReturnMode] = useState<'TWR' | 'Simple'>('TWR');

  // Donut chart states
  const [sectorViewMode, setSectorViewMode] = useState<SectorViewMode>('sector');
  const [sectorData, setSectorData] = useState<any>(null);

  // Request counter to prevent stale responses from overwriting newer data
  const loadRequestRef = useRef(0);

  useEffect(() => {
    loadViews();
  }, []);

  useEffect(() => {
    if (selectedView) {
      loadViewData(selectedView);
    }
  }, [selectedView]);

  // Re-fetch sector data when the grouping mode changes
  useEffect(() => {
    if (selectedView) {
      api.getSectorWeights(selectedView.view_type, selectedView.view_id, undefined, sectorViewMode)
        .then((data) => setSectorData(data))
        .catch(() => setSectorData(null));
    }
  }, [sectorViewMode]);

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

  const loadViewData = async (view: any) => {
    if (!view) return;

    // Increment request counter; only the latest request applies its results
    const requestId = ++loadRequestRef.current;

    // Clear previous data immediately so stale data doesn't persist
    setSummary(null);
    setReturns([]);
    setBenchmarkReturns({});
    setHoldings(null);
    setRisk(null);
    setUnpriced([]);
    setSectorData(null);
    setLoading(true);

    try {
      const [summaryData, returnsData, benchmarksData, holdingsData, riskData, unpricedData, sectorWeights] = await Promise.all([
        api.getSummary(view.view_type, view.view_id).catch(() => null),
        api.getReturns(view.view_type, view.view_id).catch(() => []),
        api.getBenchmarkReturns(['SPY', 'QQQ', 'INDU']).catch(() => ({})),
        api.getHoldings(view.view_type, view.view_id).catch(() => null),
        api.getRisk(view.view_type, view.view_id).catch(() => null),
        api.getUnpricedInstruments().catch(() => []),
        api.getSectorWeights(view.view_type, view.view_id, undefined, sectorViewMode).catch(() => null),
      ]);

      // Only apply results if this is still the latest request
      if (requestId !== loadRequestRef.current) return;

      setSummary(summaryData);
      setReturns(returnsData || []);
      setBenchmarkReturns(benchmarksData);
      setHoldings(holdingsData);
      setRisk(riskData);
      setUnpriced(unpricedData);
      setSectorData(sectorWeights);
    } catch (error) {
      if (requestId !== loadRequestRef.current) return;
      console.error('Failed to load view data:', error);
    } finally {
      if (requestId === loadRequestRef.current) {
        setLoading(false);
      }
    }
  };

  // Memoize chart data computation to avoid recalculating on every render
  const chartData = useMemo(() => {
    if (returns.length === 0) return [];

    if (returnMode === 'TWR') {
      // Merge and normalize TWR data
      const dataByDate: any = {};
      returns.forEach((r: any) => {
        dataByDate[r.date] = { date: r.date, Portfolio: r.index_value };
      });

      ['SPY', 'QQQ', 'INDU'].forEach((code) => {
        if (benchmarkReturns[code]) {
          benchmarkReturns[code].forEach((r: any) => {
            if (!dataByDate[r.date]) dataByDate[r.date] = { date: r.date };
            dataByDate[r.date][code] = r.index_value;
          });
        }
      });

      const merged = Object.values(dataByDate).sort((a: any, b: any) =>
        a.date.localeCompare(b.date)
      );

      if (merged.length === 0) return [];

      const allSeries = ['Portfolio', 'SPY', 'QQQ', 'INDU'];
      const availableSeries = allSeries.filter(s =>
        merged.some((point: any) => point[s] !== undefined && point[s] !== null)
      );

      const firstPortfolioDate = merged.find((point: any) =>
        point.Portfolio !== undefined && point.Portfolio !== null
      ) as { date: string; [key: string]: any } | undefined;

      if (!firstPortfolioDate) return [];

      const baselineValues: any = {};
      availableSeries.forEach(s => {
        const firstWithSeries = merged.find((point: any) =>
          point.date >= firstPortfolioDate.date &&
          point[s] !== undefined &&
          point[s] !== null
        ) as { [key: string]: any } | undefined;
        baselineValues[s] = firstWithSeries ? firstWithSeries[s] : 1.0;
      });

      return merged
        .filter((point: any) => point.date >= firstPortfolioDate.date)
        .map((point: any) => {
          const normalizedPoint: any = { date: point.date };
          availableSeries.forEach(s => {
            if (point[s] !== undefined && point[s] !== null && baselineValues[s]) {
              normalizedPoint[s] = point[s] / baselineValues[s];
            }
          });
          return normalizedPoint;
        });
    } else {
      // Simple mode — daily cumulative simple returns (V_t / V_0)
      // Uses the same daily index values as TWR but shows growth-of-$1 from each series' start
      const dataByDate: any = {};
      returns.forEach((r: any) => {
        dataByDate[r.date] = { date: r.date, Portfolio: r.index_value };
      });

      ['SPY', 'QQQ', 'INDU'].forEach((code) => {
        if (benchmarkReturns[code]) {
          benchmarkReturns[code].forEach((r: any) => {
            if (!dataByDate[r.date]) dataByDate[r.date] = { date: r.date };
            dataByDate[r.date][code] = r.index_value;
          });
        }
      });

      const merged = Object.values(dataByDate).sort((a: any, b: any) =>
        a.date.localeCompare(b.date)
      );

      if (merged.length === 0) return [];

      const allSeries = ['Portfolio', 'SPY', 'QQQ', 'INDU'];
      const availableSeries = allSeries.filter(s =>
        merged.some((point: any) => point[s] !== undefined && point[s] !== null)
      );

      const firstPortfolioDate = merged.find((point: any) =>
        point.Portfolio !== undefined && point.Portfolio !== null
      ) as { date: string; [key: string]: any } | undefined;

      if (!firstPortfolioDate) return [];

      const baselineValues: any = {};
      availableSeries.forEach(s => {
        const firstWithSeries = merged.find((point: any) =>
          point.date >= firstPortfolioDate.date &&
          point[s] !== undefined &&
          point[s] !== null
        ) as { [key: string]: any } | undefined;
        baselineValues[s] = firstWithSeries ? firstWithSeries[s] : 1.0;
      });

      return merged
        .filter((point: any) => point.date >= firstPortfolioDate.date)
        .map((point: any) => {
          const normalizedPoint: any = { date: point.date };
          availableSeries.forEach(s => {
            if (point[s] !== undefined && point[s] !== null && baselineValues[s]) {
              normalizedPoint[s] = point[s] / baselineValues[s];
            }
          });
          return normalizedPoint;
        });
    }
  }, [returns, benchmarkReturns, returnMode]);

  const viewOptions = useMemo(() => views.map((v) => ({
    value: v,
    label: v.view_name,
    group: v.view_type,
  })), [views]);

  // Process holdings data for pie chart — top 20 by market value, rest grouped as "Other"
  const holdingsPieData = useMemo(() => {
    if (!holdings?.holdings) return [];

    const sorted = [...holdings.holdings]
      .filter((h: any) => h.market_value > 0)
      .sort((a: any, b: any) => b.market_value - a.market_value);

    const top = sorted.slice(0, TOP_N);
    const rest = sorted.slice(TOP_N);

    const data = top.map((h: any, idx: number) => ({
      name: h.symbol,
      value: h.market_value,
      weight: h.weight,
      color: CHART_COLORS[idx % CHART_COLORS.length],
    }));

    if (rest.length > 0) {
      const otherValue = rest.reduce((sum: number, h: any) => sum + h.market_value, 0);
      const otherWeight = rest.reduce((sum: number, h: any) => sum + (h.weight || 0), 0);
      data.push({
        name: `Other (${rest.length})`,
        value: otherValue,
        weight: otherWeight,
        color: '#94a3b8',
      });
    }

    return data;
  }, [holdings]);

  // Process sector data for pie chart — top N for industry, all for sector/country
  const sectorPieData = useMemo(() => {
    if (!sectorData?.sectors) return [];

    const sorted = [...sectorData.sectors].sort((a: any, b: any) => b.weight - a.weight);
    const limit = sectorViewMode === 'industry' ? TOP_N : sorted.length;

    const top = sorted.slice(0, limit);
    const rest = sorted.slice(limit);

    const data = top.map((s: any, idx: number) => ({
      name: s.sector,
      value: s.weight,
      marketValue: s.market_value,
      count: s.holdings_count,
      color: CHART_COLORS[idx % CHART_COLORS.length],
    }));

    if (rest.length > 0) {
      const otherWeight = rest.reduce((sum: number, s: any) => sum + s.weight, 0);
      const otherMV = rest.reduce((sum: number, s: any) => sum + (s.market_value || 0), 0);
      const otherCount = rest.reduce((sum: number, s: any) => sum + (s.holdings_count || 0), 0);
      data.push({
        name: `Other (${rest.length})`,
        value: otherWeight,
        marketValue: otherMV,
        count: otherCount,
        color: '#94a3b8',
      });
    }

    return data;
  }, [sectorData, sectorViewMode]);

  // Track which donut slice is hovered for potential highlight
  const [hoveredSliceIdx, setHoveredSliceIdx] = useState<number | null>(null);

  return (
    <Layout>
      <div className="space-y-6">
        {/* Page Header */}
        <div className="flex flex-col sm:flex-row sm:items-center sm:justify-between gap-4">
          <div>
            <h1 className="text-2xl font-bold text-zinc-900">Dashboard</h1>
            <p className="text-sm text-zinc-500 mt-1">Portfolio performance and analytics</p>
          </div>
          <div className="w-full sm:w-80">
            <Select
              options={viewOptions}
              value={viewOptions.find((o) => o.value === selectedView)}
              onChange={(option) => setSelectedView(option?.value)}
              placeholder="Select account or group..."
              styles={selectStyles}
              isSearchable
            />
          </div>
        </div>

        {loading && (
          <div className="card flex items-center justify-center py-16">
            <div className="flex flex-col items-center gap-3">
              <div className="w-8 h-8 border-2 border-blue-600 border-t-transparent rounded-full animate-spin" />
              <span className="text-sm text-zinc-500">Loading data...</span>
            </div>
          </div>
        )}

        {!loading && (
          <>
            {/* Summary Header Card */}
            {summary && (
              <div className="card">
                <div className="flex flex-col lg:flex-row lg:items-start lg:justify-between gap-6">
                  <div>
                    <div className="flex items-center gap-3 mb-1">
                      <h2 className="text-xl font-semibold text-zinc-900">{summary.view_name}</h2>
                      <span className="badge badge-neutral">
                        As of {format(new Date(summary.as_of_date), 'MMM d, yyyy')}
                      </span>
                    </div>
                    <div className="text-3xl font-bold text-zinc-900 tabular-nums">
                      {formatCurrency(summary.total_value)}
                    </div>
                    <p className="text-sm text-zinc-500 mt-1">Total Portfolio Value (Equity Sleeve)</p>
                  </div>

                  {/* Return Metrics */}
                  <div className="grid grid-cols-2 sm:grid-cols-5 gap-4 lg:gap-6">
                    {[
                      { label: '1M', value: summary.return_1m, color: 'metric-card-blue' },
                      { label: '3M', value: summary.return_3m, color: 'metric-card-green' },
                      { label: 'YTD', value: summary.return_ytd, color: 'metric-card-purple' },
                      { label: '1Y', value: summary.return_1y, color: 'metric-card-orange' },
                      { label: 'All', value: summary.return_inception, color: 'metric-card-teal' },
                    ].map((item) => (
                      <div key={item.label} className={`metric-card ${item.color}`}>
                        <div className="metric-label">{item.label}</div>
                        <div className={`metric-value ${item.value != null ? getGainColorClass(item.value) : 'text-zinc-400'}`}>
                          {item.value != null ? formatPercentWithSign(item.value) : 'N/A'}
                        </div>
                      </div>
                    ))}
                  </div>
                </div>
              </div>
            )}

            {/* Allocation Charts */}
            {(holdingsPieData.length > 0 || sectorPieData.length > 0) && (
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6" style={{ alignItems: 'stretch' }}>
                {/* Holdings Breakdown */}
                <div className="card flex flex-col" style={{ minHeight: 320 }}>
                  <div className="card-header flex items-center justify-between">
                    <h3 className="card-title">Holdings Breakdown</h3>
                    <span className="text-xs text-zinc-400">Top {Math.min(TOP_N, holdingsPieData.length)} by value</span>
                  </div>

                  {holdingsPieData.length > 0 ? (
                    <div className="flex items-center gap-6 flex-1 py-2">
                      {/* Donut chart */}
                      <div className="flex-shrink-0" style={{ width: 170, height: 170 }}>
                        <ResponsiveContainer width="100%" height="100%">
                          <PieChart>
                            <Pie
                              data={holdingsPieData}
                              cx="50%"
                              cy="50%"
                              innerRadius={42}
                              outerRadius={78}
                              paddingAngle={1}
                              dataKey="value"
                              stroke="none"
                              onMouseEnter={(_, idx) => setHoveredSliceIdx(idx)}
                              onMouseLeave={() => setHoveredSliceIdx(null)}
                            >
                              {holdingsPieData.map((entry: any, index: number) => (
                                <Cell key={`hc-${index}`} fill={entry.color} />
                              ))}
                            </Pie>
                            <Tooltip
                              formatter={(value: any, _: any, props: any) => [
                                formatCurrency(value),
                                props.payload.name
                              ]}
                              contentStyle={{ borderRadius: '8px', border: '1px solid #e4e4e7', fontSize: '12px' }}
                            />
                          </PieChart>
                        </ResponsiveContainer>
                      </div>
                      {/* Legend list */}
                      <div className="flex-1 min-w-0 max-h-[220px] overflow-y-auto pr-1">
                        <table className="w-full text-xs">
                          <tbody>
                            {holdingsPieData.map((entry: any, idx: number) => (
                              <tr key={idx} className="hover:bg-zinc-50">
                                <td className="py-0.5 pr-1.5">
                                  <span className="inline-block w-2.5 h-2.5 rounded-sm flex-shrink-0" style={{ backgroundColor: entry.color }} />
                                </td>
                                <td className="py-0.5 pr-2 font-medium text-zinc-800 whitespace-nowrap">{entry.name}</td>
                                <td className="py-0.5 text-right tabular-nums text-zinc-500 whitespace-nowrap">
                                  {(entry.weight * 100).toFixed(1)}%
                                </td>
                                <td className="py-0.5 pl-2 text-right tabular-nums text-zinc-600 whitespace-nowrap">
                                  {formatCurrency(entry.value)}
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </div>
                  ) : (
                    <div className="flex-1 flex items-center justify-center text-zinc-400">
                      No holdings data available
                    </div>
                  )}
                </div>

                {/* Sector / Industry / Country Breakdown */}
                <div className="card flex flex-col" style={{ minHeight: 320 }}>
                  <div className="card-header flex items-center justify-between">
                    <h3 className="card-title">Allocation</h3>
                    <div className="pill-tabs flex-wrap">
                      {[
                        { key: 'sector', label: 'Sector' },
                        { key: 'industry', label: 'Industry' },
                        { key: 'country', label: 'Country' },
                      ].map((item) => (
                        <button
                          key={item.key}
                          onClick={() => setSectorViewMode(item.key as SectorViewMode)}
                          className={`pill-tab ${sectorViewMode === item.key ? 'pill-tab-active' : ''}`}
                        >
                          {item.label}
                        </button>
                      ))}
                    </div>
                  </div>

                  {sectorPieData.length > 0 ? (
                    <div className="flex items-center gap-6 flex-1 py-2">
                      {/* Donut chart */}
                      <div className="flex-shrink-0" style={{ width: 170, height: 170 }}>
                        <ResponsiveContainer width="100%" height="100%">
                          <PieChart>
                            <Pie
                              data={sectorPieData}
                              cx="50%"
                              cy="50%"
                              innerRadius={42}
                              outerRadius={78}
                              paddingAngle={1}
                              dataKey="value"
                              stroke="none"
                            >
                              {sectorPieData.map((entry: any, index: number) => (
                                <Cell key={`sc-${index}`} fill={entry.color} />
                              ))}
                            </Pie>
                            <Tooltip
                              formatter={(value: any, _: any, props: any) => [
                                `${(value * 100).toFixed(1)}%`,
                                props.payload.name
                              ]}
                              contentStyle={{ borderRadius: '8px', border: '1px solid #e4e4e7', fontSize: '12px' }}
                            />
                          </PieChart>
                        </ResponsiveContainer>
                      </div>
                      {/* Legend list */}
                      <div className="flex-1 min-w-0 max-h-[220px] overflow-y-auto pr-1">
                        <table className="w-full text-xs">
                          <tbody>
                            {sectorPieData.map((entry: any, idx: number) => (
                              <tr key={idx} className="hover:bg-zinc-50">
                                <td className="py-0.5 pr-1.5">
                                  <span className="inline-block w-2.5 h-2.5 rounded-sm flex-shrink-0" style={{ backgroundColor: entry.color }} />
                                </td>
                                <td className="py-0.5 pr-2 font-medium text-zinc-800 truncate max-w-[120px]" title={entry.name}>{entry.name}</td>
                                <td className="py-0.5 text-right tabular-nums text-zinc-500 whitespace-nowrap">
                                  {(entry.value * 100).toFixed(1)}%
                                </td>
                                <td className="py-0.5 pl-2 text-right tabular-nums text-zinc-600 whitespace-nowrap">
                                  {formatCurrency(entry.marketValue)}
                                </td>
                              </tr>
                            ))}
                          </tbody>
                        </table>
                      </div>
                    </div>
                  ) : (
                    <div className="flex-1 flex items-center justify-center text-zinc-400">
                      No sector data available
                    </div>
                  )}
                </div>
              </div>
            )}

            {/* Performance Chart */}
            {chartData.length > 0 && (
              <div className="card">
                <div className="card-header">
                  <h3 className="card-title">Performance vs Benchmarks</h3>
                  <div className="pill-tabs">
                    <button
                      onClick={() => setReturnMode('TWR')}
                      className={`pill-tab ${returnMode === 'TWR' ? 'pill-tab-active' : ''}`}
                    >
                      TWR
                    </button>
                    <button
                      onClick={() => setReturnMode('Simple')}
                      className={`pill-tab ${returnMode === 'Simple' ? 'pill-tab-active' : ''}`}
                    >
                      Simple
                    </button>
                  </div>
                </div>
                <ResponsiveContainer width="100%" height={360}>
                  <LineChart data={chartData} margin={{ top: 5, right: 5, bottom: 5, left: 5 }}>
                    <CartesianGrid strokeDasharray="3 3" stroke="#e4e4e7" />
                    <XAxis
                      dataKey="date"
                      tickFormatter={(value) => format(new Date(value), 'MMM yy')}
                      tick={{ fontSize: 11, fill: '#71717a' }}
                      axisLine={{ stroke: '#e4e4e7' }}
                    />
                    <YAxis
                      tickFormatter={(value) => formatIndexValue(value)}
                      tick={{ fontSize: 11, fill: '#71717a' }}
                      axisLine={{ stroke: '#e4e4e7' }}
                      width={60}
                    />
                    <Tooltip
                      labelFormatter={(value) => format(new Date(value), 'MMM d, yyyy')}
                      formatter={(value: any) => [formatIndexValue(value), '']}
                      contentStyle={{ borderRadius: '8px', border: '1px solid #e4e4e7', fontSize: '12px' }}
                    />
                    <Legend wrapperStyle={{ fontSize: '12px' }} />
                    <Line type="monotone" dataKey="Portfolio" stroke="#3b82f6" strokeWidth={2.5} name="Portfolio" dot={false} />
                    <Line type="monotone" dataKey="SPY" stroke="#10b981" strokeWidth={1.5} name="S&P 500" dot={false} strokeDasharray="4 2" />
                    <Line type="monotone" dataKey="QQQ" stroke="#8b5cf6" strokeWidth={1.5} name="Nasdaq" dot={false} strokeDasharray="4 2" />
                    <Line type="monotone" dataKey="INDU" stroke="#f59e0b" strokeWidth={1.5} name="Dow Jones" dot={false} strokeDasharray="4 2" />
                  </LineChart>
                </ResponsiveContainer>
              </div>
            )}

            {/* Holdings Table */}
            {holdings && (
              <div className="card">
                <div className="card-header">
                  <h3 className="card-title">Holdings</h3>
                  <span className="text-sm text-zinc-500">{holdings.holdings.length} positions</span>
                </div>
                <div className="table-container">
                  <table className="table">
                    <thead>
                      <tr>
                        <th>Symbol</th>
                        <th className="text-right">Weight</th>
                        <th className="text-right">Price</th>
                        <th className="text-right">Avg Cost</th>
                        <th className="text-right">1D %</th>
                        <th className="text-right">Total %</th>
                        <th className="text-right">1D P/L</th>
                        <th className="text-right">Total P/L</th>
                        <th className="text-right">Value</th>
                        <th className="text-right">Shares</th>
                      </tr>
                    </thead>
                    <tbody>
                      {holdings.holdings.map((h: any) => (
                        <tr key={h.symbol}>
                          <td>
                            <div className="font-medium text-zinc-900">{h.symbol}</div>
                            <div className="text-xs text-zinc-500 truncate max-w-[140px]" title={h.asset_name}>
                              {h.asset_name}
                            </div>
                          </td>
                          <td className="text-right tabular-nums">{formatPercent(h.weight)}</td>
                          <td className="text-right tabular-nums">{formatCurrencyWithDecimals(h.price)}</td>
                          <td className="text-right tabular-nums">
                            {h.avg_cost != null ? formatCurrencyWithDecimals(h.avg_cost) : '-'}
                          </td>
                          <td className={`text-right tabular-nums ${getGainColorClass(h.gain_1d_pct)}`}>
                            {formatGainPercent(h.gain_1d_pct)}
                          </td>
                          <td className={`text-right tabular-nums ${getGainColorClass(h.unr_gain_pct)}`}>
                            {formatGainPercent(h.unr_gain_pct)}
                          </td>
                          <td className={`text-right tabular-nums ${getGainColorClass(h.gain_1d)}`}>
                            {formatGainCurrency(h.gain_1d)}
                          </td>
                          <td className={`text-right tabular-nums ${getGainColorClass(h.unr_gain)}`}>
                            {formatGainCurrency(h.unr_gain)}
                          </td>
                          <td className="text-right tabular-nums font-medium">{formatCurrency(h.market_value)}</td>
                          <td className="text-right tabular-nums text-zinc-500">{formatSharesDisplay(h.shares)}</td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}

            {/* Risk Metrics */}
            {risk && (
              <div className="card">
                <div className="card-header">
                  <h3 className="card-title">Risk Metrics</h3>
                </div>
                <div className="grid grid-cols-2 md:grid-cols-4 gap-6">
                  {[
                    { label: '21-Day Vol', value: risk.vol_21d, color: 'metric-card-blue' },
                    { label: '63-Day Vol', value: risk.vol_63d, color: 'metric-card-purple' },
                    { label: 'Max DD (1Y)', value: risk.max_drawdown_1y, color: 'metric-card-red' },
                    { label: 'VaR 95%', value: risk.var_95_1d_hist, color: 'metric-card-orange' },
                  ].map((item) => (
                    <div key={item.label} className={`metric-card ${item.color}`}>
                      <div className="metric-label">{item.label}</div>
                      <div className="metric-value">
                        {item.value ? formatPercent(item.value) : 'N/A'}
                      </div>
                    </div>
                  ))}
                </div>
              </div>
            )}

            {/* Unpriced Instruments Warning */}
            {unpriced.length > 0 && (
              <div className="alert alert-warning">
                <div className="flex items-start gap-3">
                  <svg className="w-5 h-5 text-amber-600 mt-0.5 flex-shrink-0" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M12 9v2m0 4h.01m-6.938 4h13.856c1.54 0 2.502-1.667 1.732-3L13.732 4c-.77-1.333-2.694-1.333-3.464 0L3.34 16c-.77 1.333.192 3 1.732 3z" />
                  </svg>
                  <div>
                    <h4 className="font-medium text-amber-800">Unpriced Instruments</h4>
                    <p className="text-sm mt-1">
                      {unpriced.length} securities have positions but no pricing data and are excluded from analytics.
                    </p>
                    <div className="mt-2 flex flex-wrap gap-2">
                      {unpriced.slice(0, 10).map((item: any) => (
                        <span key={item.symbol} className="badge badge-warning">
                          {item.symbol}
                        </span>
                      ))}
                      {unpriced.length > 10 && (
                        <span className="text-sm text-amber-700">+{unpriced.length - 10} more</span>
                      )}
                    </div>
                  </div>
                </div>
              </div>
            )}

            {/* No data message when nothing loaded */}
            {!summary && chartData.length === 0 && !holdings && !risk && (
              <div className="card flex items-center justify-center py-16">
                <div className="text-center">
                  <p className="text-zinc-500">No data available for this view.</p>
                  <p className="text-sm text-zinc-400 mt-1">Try running analytics or selecting a different view.</p>
                </div>
              </div>
            )}
          </>
        )}
      </div>
    </Layout>
  );
}
