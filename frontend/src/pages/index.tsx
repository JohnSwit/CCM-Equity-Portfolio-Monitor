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

// Holdings chart view modes
type HoldingsViewMode = 'marketValue' | 'cost' | 'gain' | 'loss';
// Sector chart view modes
type SectorViewMode = 'sector' | 'industry' | 'country' | 'region' | 'market' | 'assetType';

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
  const [holdingsViewMode, setHoldingsViewMode] = useState<HoldingsViewMode>('marketValue');
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
        api.getSectorWeights(view.view_type, view.view_id).catch(() => null),
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
      // Simple mode
      const firstDate = returns[0].date;
      const lastDate = returns[returns.length - 1].date;
      const portfolioStart = returns[0].index_value;
      const portfolioEnd = returns[returns.length - 1].index_value;

      const getSimpleReturn = (code: string) => {
        const benchData = benchmarkReturns[code];
        if (!benchData || benchData.length === 0) return { start: null, end: null };
        const dataInRange = benchData.filter((r: any) => r.date >= firstDate && r.date <= lastDate);
        if (dataInRange.length === 0) return { start: null, end: null };
        return { start: dataInRange[0].index_value, end: dataInRange[dataInRange.length - 1].index_value };
      };

      const spy = getSimpleReturn('SPY');
      const qqq = getSimpleReturn('QQQ');
      const indu = getSimpleReturn('INDU');

      return [
        {
          date: firstDate,
          Portfolio: 1.0,
          ...(spy.start && { SPY: 1.0 }),
          ...(qqq.start && { QQQ: 1.0 }),
          ...(indu.start && { INDU: 1.0 }),
        },
        {
          date: lastDate,
          Portfolio: portfolioEnd / portfolioStart,
          ...(spy.start && spy.end && { SPY: spy.end / spy.start }),
          ...(qqq.start && qqq.end && { QQQ: qqq.end / qqq.start }),
          ...(indu.start && indu.end && { INDU: indu.end / indu.start }),
        },
      ];
    }
  }, [returns, benchmarkReturns, returnMode]);

  const viewOptions = useMemo(() => views.map((v) => ({
    value: v,
    label: v.view_name,
    group: v.view_type,
  })), [views]);

  // Process holdings data for pie chart
  const holdingsPieData = useMemo(() => {
    if (!holdings?.holdings) return [];

    const data = holdings.holdings.map((h: any, idx: number) => {
      const cost = h.cost_basis || h.market_value;
      const gain = Math.max(0, h.market_value - cost);
      const loss = Math.max(0, cost - h.market_value);

      return {
        name: h.symbol,
        marketValue: h.market_value,
        cost: cost,
        gain: gain,
        loss: loss,
        weight: h.weight,
        color: CHART_COLORS[idx % CHART_COLORS.length],
      };
    });

    switch (holdingsViewMode) {
      case 'cost':
        return data.filter((d: any) => d.cost > 0).map((d: any) => ({ ...d, value: d.cost }));
      case 'gain':
        return data.filter((d: any) => d.gain > 0).map((d: any) => ({ ...d, value: d.gain }));
      case 'loss':
        return data.filter((d: any) => d.loss > 0).map((d: any) => ({ ...d, value: d.loss }));
      default:
        return data.filter((d: any) => d.marketValue > 0).map((d: any) => ({ ...d, value: d.marketValue }));
    }
  }, [holdings, holdingsViewMode]);

  // Process sector data for pie chart
  const sectorPieData = useMemo(() => {
    if (!sectorData?.sectors) return [];

    return sectorData.sectors.map((s: any, idx: number) => ({
      name: s.sector,
      value: s.weight,
      marketValue: s.market_value,
      count: s.holdings_count,
      color: CHART_COLORS[idx % CHART_COLORS.length],
    }));
  }, [sectorData]);

  // Custom label for pie chart
  const renderCustomLabel = ({ name, value, cx, cy, midAngle, innerRadius, outerRadius, percent }: any) => {
    if (percent < 0.03) return null;
    const RADIAN = Math.PI / 180;
    const radius = outerRadius * 1.2;
    const x = cx + radius * Math.cos(-midAngle * RADIAN);
    const y = cy + radius * Math.sin(-midAngle * RADIAN);
    const pct = (percent * 100).toFixed(1);

    return (
      <text
        x={x}
        y={y}
        fill="#52525b"
        textAnchor={x > cx ? 'start' : 'end'}
        dominantBaseline="central"
        fontSize={11}
        fontWeight={500}
      >
        {`${name}: ${pct}%`}
      </text>
    );
  };

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
              <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                {/* Holdings Breakdown */}
                <div className="card">
                  <div className="card-header">
                    <h3 className="card-title">Holdings Breakdown</h3>
                    <button
                      onClick={() => loadViewData(selectedView)}
                      className="btn btn-ghost btn-sm"
                      title="Refresh"
                    >
                      <svg className="w-4 h-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                        <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
                      </svg>
                    </button>
                  </div>
                  <div className="pill-tabs mb-4">
                    {[
                      { key: 'marketValue', label: 'Value' },
                      { key: 'cost', label: 'Cost' },
                      { key: 'gain', label: 'Gains' },
                      { key: 'loss', label: 'Losses' },
                    ].map((item) => (
                      <button
                        key={item.key}
                        onClick={() => setHoldingsViewMode(item.key as HoldingsViewMode)}
                        className={`pill-tab ${holdingsViewMode === item.key ? 'pill-tab-active' : ''}`}
                      >
                        {item.label}
                      </button>
                    ))}
                  </div>

                  {holdingsPieData.length > 0 ? (
                    <ResponsiveContainer width="100%" height={280}>
                      <PieChart>
                        <Pie
                          data={holdingsPieData}
                          cx="50%"
                          cy="50%"
                          innerRadius={55}
                          outerRadius={90}
                          paddingAngle={2}
                          dataKey="value"
                          label={renderCustomLabel}
                          labelLine={{ stroke: '#d4d4d8', strokeWidth: 1 }}
                        >
                          {holdingsPieData.map((entry: any, index: number) => (
                            <Cell key={`cell-${index}`} fill={entry.color} />
                          ))}
                        </Pie>
                        <Tooltip
                          formatter={(value: any, name: any, props: any) => [
                            formatCurrency(value),
                            props.payload.name
                          ]}
                          contentStyle={{ borderRadius: '8px', border: '1px solid #e4e4e7' }}
                        />
                      </PieChart>
                    </ResponsiveContainer>
                  ) : (
                    <div className="h-72 flex items-center justify-center text-zinc-400">
                      No holdings data available
                    </div>
                  )}
                </div>

                {/* Sector Breakdown */}
                <div className="card">
                  <div className="card-header">
                    <h3 className="card-title">Sector Allocation</h3>
                  </div>
                  <div className="pill-tabs mb-4 flex-wrap">
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

                  {sectorPieData.length > 0 ? (
                    <ResponsiveContainer width="100%" height={280}>
                      <PieChart>
                        <Pie
                          data={sectorPieData}
                          cx="50%"
                          cy="50%"
                          innerRadius={55}
                          outerRadius={90}
                          paddingAngle={2}
                          dataKey="value"
                          label={renderCustomLabel}
                          labelLine={{ stroke: '#d4d4d8', strokeWidth: 1 }}
                        >
                          {sectorPieData.map((entry: any, index: number) => (
                            <Cell key={`cell-${index}`} fill={entry.color} />
                          ))}
                        </Pie>
                        <Tooltip
                          formatter={(value: any, name: any, props: any) => [
                            formatPercent(value),
                            props.payload.name
                          ]}
                          contentStyle={{ borderRadius: '8px', border: '1px solid #e4e4e7' }}
                        />
                      </PieChart>
                    </ResponsiveContainer>
                  ) : (
                    <div className="h-72 flex items-center justify-center text-zinc-400">
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
