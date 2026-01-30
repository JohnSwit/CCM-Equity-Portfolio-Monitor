import { useState, useEffect, useMemo } from 'react';
import Layout from '@/components/Layout';
import { api } from '@/lib/api';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, PieChart, Pie, Cell } from 'recharts';
import Select from 'react-select';
import { format, subMonths, startOfYear } from 'date-fns';

// Time period options for the performance chart
type ChartPeriod = '1M' | '3M' | 'YTD' | '1Y' | 'ALL';
// Factor analysis period options
type FactorPeriod = '1M' | '3M' | '6M' | 'YTD' | '1Y' | 'ALL';

// Color palette for pie charts
const CHART_COLORS = [
  '#3b82f6', '#10b981', '#f59e0b', '#ef4444', '#8b5cf6',
  '#06b6d4', '#ec4899', '#84cc16', '#f97316', '#6366f1',
  '#14b8a6', '#a855f7', '#eab308', '#22c55e', '#0ea5e9',
  '#d946ef', '#64748b', '#78716c', '#0891b2', '#7c3aed'
];

// Holdings chart view modes
type HoldingsViewMode = 'marketValue' | 'cost' | 'gain' | 'loss';
// Sector chart view modes
type SectorViewMode = 'sector' | 'industry' | 'country' | 'region' | 'market' | 'assetType';

export default function Dashboard() {
  const [views, setViews] = useState<any[]>([]);
  const [selectedView, setSelectedView] = useState<any>(null);
  const [summary, setSummary] = useState<any>(null);
  const [returns, setReturns] = useState<any[]>([]);
  const [benchmarkReturns, setBenchmarkReturns] = useState<any>({});
  const [chartData, setChartData] = useState<any[]>([]);
  const [holdings, setHoldings] = useState<any>(null);
  const [risk, setRisk] = useState<any>(null);
  const [factorBenchmarking, setFactorBenchmarking] = useState<any>(null);
  const [factorPeriod, setFactorPeriod] = useState<FactorPeriod>('1Y');
  const [factorLoading, setFactorLoading] = useState(false);
  const [unpriced, setUnpriced] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [returnMode, setReturnMode] = useState<'TWR' | 'Simple'>('TWR');

  // Donut chart states
  const [holdingsViewMode, setHoldingsViewMode] = useState<HoldingsViewMode>('marketValue');
  const [sectorViewMode, setSectorViewMode] = useState<SectorViewMode>('sector');
  const [sectorData, setSectorData] = useState<any>(null);

  useEffect(() => {
    loadViews();
  }, []);

  useEffect(() => {
    if (selectedView) {
      loadViewData();
    }
  }, [selectedView]);

  useEffect(() => {
    // Merge portfolio and benchmark data for chart
    if (returns.length > 0) {
      if (returnMode === 'TWR') {
        mergeAndNormalizeTWR();
      } else {
        mergeAndNormalizeSimple();
      }
    }
  }, [returns, benchmarkReturns, returnMode]);

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

  const loadViewData = async () => {
    if (!selectedView) return;

    setLoading(true);
    try {
      const [summaryData, returnsData, benchmarksData, holdingsData, riskData, unpricedData, sectorWeights] = await Promise.all([
        api.getSummary(selectedView.view_type, selectedView.view_id),
        api.getReturns(selectedView.view_type, selectedView.view_id),
        api.getBenchmarkReturns(['SPY', 'QQQ', 'INDU']).catch(() => ({})),
        api.getHoldings(selectedView.view_type, selectedView.view_id),
        api.getRisk(selectedView.view_type, selectedView.view_id).catch(() => null),
        api.getUnpricedInstruments().catch(() => []),
        api.getSectorWeights(selectedView.view_type, selectedView.view_id).catch(() => null),
      ]);

      setSummary(summaryData);
      setReturns(returnsData);
      setBenchmarkReturns(benchmarksData);
      setHoldings(holdingsData);
      setRisk(riskData);
      setUnpriced(unpricedData);
      setSectorData(sectorWeights);

      // Load factor benchmarking data separately
      loadFactorBenchmarking(selectedView.view_type, selectedView.view_id, factorPeriod);
    } catch (error) {
      console.error('Failed to load view data:', error);
    } finally {
      setLoading(false);
    }
  };

  const loadFactorBenchmarking = async (viewType: string, viewId: number, period: string) => {
    setFactorLoading(true);
    try {
      const data = await api.getFactorBenchmarking(viewType, viewId, 'US_CORE', period);
      setFactorBenchmarking(data);
    } catch (error) {
      console.error('Failed to load factor benchmarking:', error);
      setFactorBenchmarking(null);
    } finally {
      setFactorLoading(false);
    }
  };

  // Reload factor data when period changes
  useEffect(() => {
    if (selectedView) {
      loadFactorBenchmarking(selectedView.view_type, selectedView.view_id, factorPeriod);
    }
  }, [factorPeriod]);

  const mergeAndNormalizeTWR = () => {
    // Create a map of date to data point
    const dataByDate: any = {};

    // Add portfolio returns
    returns.forEach((r: any) => {
      const dateStr = r.date;
      dataByDate[dateStr] = {
        date: dateStr,
        Portfolio: r.index_value,
      };
    });

    // Add benchmark returns
    ['SPY', 'QQQ', 'INDU'].forEach((code) => {
      if (benchmarkReturns[code]) {
        benchmarkReturns[code].forEach((r: any) => {
          const dateStr = r.date;
          if (!dataByDate[dateStr]) {
            dataByDate[dateStr] = { date: dateStr };
          }
          dataByDate[dateStr][code] = r.index_value;
        });
      }
    });

    // Convert to array and sort by date
    let merged = Object.values(dataByDate).sort((a: any, b: any) =>
      a.date.localeCompare(b.date)
    );

    if (merged.length === 0) {
      setChartData([]);
      return;
    }

    // Determine which series are available
    const allSeries = ['Portfolio', 'SPY', 'QQQ', 'INDU'];
    const availableSeries = allSeries.filter(s =>
      merged.some((point: any) => point[s] !== undefined && point[s] !== null)
    );

    // Find the first date where portfolio has data (minimum requirement)
    const firstPortfolioDate = merged.find((point: any) =>
      point.Portfolio !== undefined && point.Portfolio !== null
    ) as { date: string; [key: string]: any } | undefined;

    if (!firstPortfolioDate) {
      setChartData([]);
      return;
    }

    // Get baseline values for each series from their first available value
    // within or after the portfolio's first date
    const baselineValues: any = {};
    availableSeries.forEach(s => {
      const firstWithSeries = merged.find((point: any) =>
        point.date >= firstPortfolioDate.date &&
        point[s] !== undefined &&
        point[s] !== null
      ) as { [key: string]: any } | undefined;
      baselineValues[s] = firstWithSeries ? firstWithSeries[s] : 1.0;
    });

    // Normalize all series to start at 1.0 from portfolio's first date
    const normalized = merged
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

    setChartData(normalized);
  };

  const mergeAndNormalizeSimple = () => {
    if (returns.length === 0) return;

    const firstDate = returns[0].date;
    const lastDate = returns[returns.length - 1].date;

    // Get start and end values for portfolio
    const portfolioStart = returns[0].index_value;
    const portfolioEnd = returns[returns.length - 1].index_value;

    // Get start and end values for benchmarks
    const getSimpleReturn = (code: string) => {
      const benchData = benchmarkReturns[code];
      if (!benchData || benchData.length === 0) return { start: null, end: null };

      // Find values closest to our date range
      const dataInRange = benchData.filter((r: any) => r.date >= firstDate && r.date <= lastDate);
      if (dataInRange.length === 0) return { start: null, end: null };

      const start = dataInRange[0].index_value;
      const end = dataInRange[dataInRange.length - 1].index_value;
      return { start, end };
    };

    const spy = getSimpleReturn('SPY');
    const qqq = getSimpleReturn('QQQ');
    const indu = getSimpleReturn('INDU');

    // Create two data points: start (all at 1.0) and end (relative performance)
    const simpleData = [
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

    setChartData(simpleData);
  };

  const formatCurrency = (value: number) => {
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD',
      minimumFractionDigits: 0,
      maximumFractionDigits: 0,
    }).format(value);
  };

  const formatPercent = (value: number) => {
    return (value * 100).toFixed(2) + '%';
  };

  const formatIndexValue = (value: number) => {
    // Convert index value to percentage return
    // e.g., 1.05 becomes +5.00%
    return ((value - 1) * 100).toFixed(2) + '%';
  };

  const formatCurrencyWithDecimals = (value: number) => {
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD',
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    }).format(value);
  };

  const formatGainPercent = (value: number | null | undefined) => {
    if (value === null || value === undefined) return '-';
    const pct = (value * 100).toFixed(2);
    return (value >= 0 ? '+' : '') + pct + '%';
  };

  const formatGainCurrency = (value: number | null | undefined) => {
    if (value === null || value === undefined) return '-';
    const formatted = new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD',
      minimumFractionDigits: 0,
      maximumFractionDigits: 0,
    }).format(Math.abs(value));
    return (value >= 0 ? '+' : '-') + formatted;
  };

  const formatSharesDisplay = (value: number) => {
    if (value >= 1000) {
      return new Intl.NumberFormat('en-US', {
        minimumFractionDigits: 0,
        maximumFractionDigits: 0,
      }).format(value);
    }
    return new Intl.NumberFormat('en-US', {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    }).format(value);
  };

  const getGainColorClass = (value: number | null | undefined) => {
    if (value === null || value === undefined) return 'text-gray-500';
    return value >= 0 ? 'text-green-600' : 'text-red-600';
  };

  const viewOptions = views.map((v) => ({
    value: v,
    label: v.view_name,
    group: v.view_type,
  }));

  // Process holdings data for pie chart
  const holdingsPieData = useMemo(() => {
    if (!holdings?.holdings) return [];

    const data = holdings.holdings.map((h: any, idx: number) => {
      // Calculate gain/loss (market_value - cost)
      const cost = h.cost_basis || h.market_value; // fallback to market value if no cost
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

    // Get data based on view mode
    switch (holdingsViewMode) {
      case 'cost':
        return data.filter((d: any) => d.cost > 0).map((d: any) => ({ ...d, value: d.cost }));
      case 'gain':
        return data.filter((d: any) => d.gain > 0).map((d: any) => ({ ...d, value: d.gain }));
      case 'loss':
        return data.filter((d: any) => d.loss > 0).map((d: any) => ({ ...d, value: d.loss }));
      default: // marketValue
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
    if (percent < 0.025) return null; // Don't show labels for slices < 2.5%
    const RADIAN = Math.PI / 180;
    const radius = outerRadius * 1.25;
    const x = cx + radius * Math.cos(-midAngle * RADIAN);
    const y = cy + radius * Math.sin(-midAngle * RADIAN);
    const pct = (percent * 100).toFixed(1);

    return (
      <text
        x={x}
        y={y}
        fill="#374151"
        textAnchor={x > cx ? 'start' : 'end'}
        dominantBaseline="central"
        fontSize={11}
      >
        {`${name}: ${pct}%`}
      </text>
    );
  };

  return (
    <Layout>
      <div className="space-y-6">
        {/* View Selector */}
        <div className="card">
          <h2 className="text-lg font-semibold mb-4">Select View</h2>
          <Select
            options={viewOptions}
            value={viewOptions.find((o) => o.value === selectedView)}
            onChange={(option) => setSelectedView(option?.value)}
            placeholder="Search and select account, group, or firm..."
            className="w-full"
          />
        </div>

        {loading && (
          <div className="text-center py-8">Loading data...</div>
        )}

        {!loading && summary && (
          <>
            {/* Summary KPIs */}
            <div className="card">
              <div className="flex justify-between items-start mb-6">
                <div>
                  <h2 className="text-2xl font-bold">{summary.view_name}</h2>
                  <p className="text-gray-600 text-sm mt-1">
                    Data as of {format(new Date(summary.as_of_date), 'MMM d, yyyy')}
                  </p>
                </div>
                <div className="text-right">
                  <div className="text-3xl font-bold">{formatCurrency(summary.total_value)}</div>
                  <div className="text-sm text-gray-600">Total Value (Equity Sleeve)</div>
                </div>
              </div>

              <div className="grid grid-cols-2 md:grid-cols-5 gap-4">
                <div className="border-l-4 border-blue-500 pl-4">
                  <div className="text-sm text-gray-600">1 Month</div>
                  <div className="text-lg font-semibold">
                    {summary.return_1m ? formatPercent(summary.return_1m) : 'N/A'}
                  </div>
                </div>
                <div className="border-l-4 border-green-500 pl-4">
                  <div className="text-sm text-gray-600">3 Months</div>
                  <div className="text-lg font-semibold">
                    {summary.return_3m ? formatPercent(summary.return_3m) : 'N/A'}
                  </div>
                </div>
                <div className="border-l-4 border-purple-500 pl-4">
                  <div className="text-sm text-gray-600">YTD</div>
                  <div className="text-lg font-semibold">
                    {summary.return_ytd ? formatPercent(summary.return_ytd) : 'N/A'}
                  </div>
                </div>
                <div className="border-l-4 border-orange-500 pl-4">
                  <div className="text-sm text-gray-600">1 Year</div>
                  <div className="text-lg font-semibold">
                    {summary.return_1y ? formatPercent(summary.return_1y) : 'N/A'}
                  </div>
                </div>
                <div className="border-l-4 border-teal-500 pl-4">
                  <div className="text-sm text-gray-600">All Time</div>
                  <div className="text-lg font-semibold">
                    {summary.return_inception != null ? formatPercent(summary.return_inception) : 'N/A'}
                  </div>
                </div>
              </div>
            </div>

            {/* Portfolio Allocation Donut Charts */}
            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
              {/* Holdings Breakdown Chart */}
              <div className="card">
                <div className="flex flex-wrap items-center gap-2 mb-4 border-b pb-3">
                  {[
                    { key: 'marketValue', label: 'Market Value' },
                    { key: 'cost', label: 'Cost' },
                    { key: 'gain', label: 'Gain' },
                    { key: 'loss', label: 'Loss' },
                  ].map((item) => (
                    <button
                      key={item.key}
                      onClick={() => setHoldingsViewMode(item.key as HoldingsViewMode)}
                      className={`px-3 py-1.5 text-sm rounded-md transition-colors ${
                        holdingsViewMode === item.key
                          ? 'bg-blue-600 text-white'
                          : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                      }`}
                    >
                      {item.label}
                    </button>
                  ))}
                  <button
                    onClick={loadViewData}
                    className="ml-auto p-1.5 text-gray-500 hover:text-gray-700 hover:bg-gray-100 rounded"
                    title="Refresh"
                  >
                    <svg className="w-4 h-4" fill="none" stroke="currentColor" viewBox="0 0 24 24">
                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
                    </svg>
                  </button>
                </div>

                {holdingsPieData.length > 0 ? (
                  <ResponsiveContainer width="100%" height={320}>
                    <PieChart>
                      <Pie
                        data={holdingsPieData}
                        cx="50%"
                        cy="50%"
                        innerRadius={60}
                        outerRadius={100}
                        paddingAngle={1}
                        dataKey="value"
                        label={renderCustomLabel}
                        labelLine={{ stroke: '#9ca3af', strokeWidth: 1 }}
                      >
                        {holdingsPieData.map((entry: any, index: number) => (
                          <Cell key={`cell-${index}`} fill={entry.color} />
                        ))}
                      </Pie>
                      <Tooltip
                        formatter={(value: any, name: any, props: any) => [
                          holdingsViewMode === 'marketValue' || holdingsViewMode === 'cost' || holdingsViewMode === 'gain' || holdingsViewMode === 'loss'
                            ? formatCurrency(value)
                            : formatPercent(value),
                          props.payload.name
                        ]}
                      />
                    </PieChart>
                  </ResponsiveContainer>
                ) : (
                  <div className="h-80 flex items-center justify-center text-gray-500">
                    No holdings data available
                  </div>
                )}
              </div>

              {/* Sector Breakdown Chart */}
              <div className="card">
                <div className="flex flex-wrap items-center gap-2 mb-4 border-b pb-3">
                  {[
                    { key: 'sector', label: 'Sector' },
                    { key: 'industry', label: 'Industry' },
                    { key: 'country', label: 'Country' },
                    { key: 'region', label: 'Region' },
                    { key: 'market', label: 'Market' },
                    { key: 'assetType', label: 'Assets Type' },
                  ].map((item) => (
                    <button
                      key={item.key}
                      onClick={() => setSectorViewMode(item.key as SectorViewMode)}
                      className={`px-3 py-1.5 text-sm rounded-md transition-colors ${
                        sectorViewMode === item.key
                          ? 'bg-blue-600 text-white'
                          : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                      }`}
                    >
                      {item.label}
                    </button>
                  ))}
                </div>

                {sectorPieData.length > 0 ? (
                  <ResponsiveContainer width="100%" height={320}>
                    <PieChart>
                      <Pie
                        data={sectorPieData}
                        cx="50%"
                        cy="50%"
                        innerRadius={60}
                        outerRadius={100}
                        paddingAngle={1}
                        dataKey="value"
                        label={renderCustomLabel}
                        labelLine={{ stroke: '#9ca3af', strokeWidth: 1 }}
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
                      />
                    </PieChart>
                  </ResponsiveContainer>
                ) : (
                  <div className="h-80 flex items-center justify-center text-gray-500">
                    No sector data available
                  </div>
                )}
              </div>
            </div>

            {/* Performance Chart */}
            <div className="card">
              <div className="flex justify-between items-center mb-4">
                <h3 className="text-lg font-semibold">Performance vs Benchmarks</h3>
                <div className="flex gap-2">
                  <button
                    onClick={() => setReturnMode('TWR')}
                    className={`px-4 py-2 rounded text-sm font-medium transition-colors ${
                      returnMode === 'TWR'
                        ? 'bg-blue-600 text-white'
                        : 'bg-gray-200 text-gray-700 hover:bg-gray-300'
                    }`}
                  >
                    TWR (Time-Weighted)
                  </button>
                  <button
                    onClick={() => setReturnMode('Simple')}
                    className={`px-4 py-2 rounded text-sm font-medium transition-colors ${
                      returnMode === 'Simple'
                        ? 'bg-blue-600 text-white'
                        : 'bg-gray-200 text-gray-700 hover:bg-gray-300'
                    }`}
                  >
                    Simple Return
                  </button>
                </div>
              </div>
              <ResponsiveContainer width="100%" height={400}>
                <LineChart data={chartData}>
                  <CartesianGrid strokeDasharray="3 3" />
                  <XAxis
                    dataKey="date"
                    tickFormatter={(value) => format(new Date(value), 'MMM yy')}
                  />
                  <YAxis
                    tickFormatter={(value) => formatIndexValue(value)}
                  />
                  <Tooltip
                    labelFormatter={(value) => format(new Date(value), 'MMM d, yyyy')}
                    formatter={(value: any) => [formatIndexValue(value), '']}
                  />
                  <Legend />
                  <Line type="monotone" dataKey="Portfolio" stroke="#3b82f6" strokeWidth={2} name="Portfolio" dot={false} />
                  <Line type="monotone" dataKey="SPY" stroke="#10b981" strokeWidth={1.5} name="S&P 500 (SPY)" dot={false} />
                  <Line type="monotone" dataKey="QQQ" stroke="#8b5cf6" strokeWidth={1.5} name="Nasdaq (QQQ)" dot={false} />
                  <Line type="monotone" dataKey="INDU" stroke="#f59e0b" strokeWidth={1.5} name="Dow (INDU)" dot={false} />
                </LineChart>
              </ResponsiveContainer>
            </div>

            {/* Portfolio Overview */}
            {holdings && (
              <div className="card">
                <h3 className="text-lg font-semibold mb-4">Portfolio Overview</h3>
                <div className="overflow-x-auto">
                  <table className="table w-full text-sm">
                    <thead>
                      <tr className="border-b border-gray-200">
                        <th className="text-left py-2 px-3">Ticker</th>
                        <th className="text-right py-2 px-3">Allocation</th>
                        <th className="text-right py-2 px-3">Last</th>
                        <th className="text-right py-2 px-3">Avg Cost</th>
                        <th className="text-right py-2 px-3">1D Gain %</th>
                        <th className="text-right py-2 px-3">Unr. Gain %</th>
                        <th className="text-right py-2 px-3">1D Gain</th>
                        <th className="text-right py-2 px-3">Unr. Gain</th>
                        <th className="text-right py-2 px-3">Market Value</th>
                        <th className="text-right py-2 px-3">Shares</th>
                      </tr>
                    </thead>
                    <tbody>
                      {holdings.holdings.map((h: any) => (
                        <tr key={h.symbol} className="border-b border-gray-100 hover:bg-gray-50">
                          <td className="py-2 px-3">
                            <div className="font-semibold">{h.symbol}</div>
                            <div className="text-xs text-gray-500 truncate max-w-[150px]" title={h.asset_name}>
                              {h.asset_name}
                            </div>
                          </td>
                          <td className="text-right py-2 px-3">{formatPercent(h.weight)}</td>
                          <td className="text-right py-2 px-3">{formatCurrencyWithDecimals(h.price)}</td>
                          <td className="text-right py-2 px-3">
                            {h.avg_cost != null ? formatCurrencyWithDecimals(h.avg_cost) : '-'}
                          </td>
                          <td className={`text-right py-2 px-3 ${getGainColorClass(h.gain_1d_pct)}`}>
                            {formatGainPercent(h.gain_1d_pct)}
                          </td>
                          <td className={`text-right py-2 px-3 ${getGainColorClass(h.unr_gain_pct)}`}>
                            {formatGainPercent(h.unr_gain_pct)}
                          </td>
                          <td className={`text-right py-2 px-3 ${getGainColorClass(h.gain_1d)}`}>
                            {formatGainCurrency(h.gain_1d)}
                          </td>
                          <td className={`text-right py-2 px-3 ${getGainColorClass(h.unr_gain)}`}>
                            {formatGainCurrency(h.unr_gain)}
                          </td>
                          <td className="text-right py-2 px-3">{formatCurrency(h.market_value)}</td>
                          <td className="text-right py-2 px-3">{formatSharesDisplay(h.shares)}</td>
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
                <h3 className="text-lg font-semibold mb-4">Risk Metrics</h3>
                <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                  <div>
                    <div className="text-sm text-gray-600">21-Day Volatility</div>
                    <div className="text-lg font-semibold">
                      {risk.vol_21d ? formatPercent(risk.vol_21d) : 'N/A'}
                    </div>
                  </div>
                  <div>
                    <div className="text-sm text-gray-600">63-Day Volatility</div>
                    <div className="text-lg font-semibold">
                      {risk.vol_63d ? formatPercent(risk.vol_63d) : 'N/A'}
                    </div>
                  </div>
                  <div>
                    <div className="text-sm text-gray-600">Max Drawdown (1Y)</div>
                    <div className="text-lg font-semibold">
                      {risk.max_drawdown_1y ? formatPercent(risk.max_drawdown_1y) : 'N/A'}
                    </div>
                  </div>
                  <div>
                    <div className="text-sm text-gray-600">VaR 95% (1D)</div>
                    <div className="text-lg font-semibold">
                      {risk.var_95_1d_hist ? formatPercent(risk.var_95_1d_hist) : 'N/A'}
                    </div>
                  </div>
                </div>
              </div>
            )}

            {/* Factor Benchmarking + Attribution */}
            <div className="card">
              <div className="flex justify-between items-center mb-4">
                <h3 className="text-lg font-semibold">Factor Benchmarking + Attribution</h3>
                <div className="flex gap-1">
                  {(['1M', '3M', '6M', 'YTD', '1Y', 'ALL'] as FactorPeriod[]).map((period) => (
                    <button
                      key={period}
                      onClick={() => setFactorPeriod(period)}
                      className={`px-3 py-1 text-sm rounded ${
                        factorPeriod === period
                          ? 'bg-indigo-600 text-white'
                          : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                      }`}
                    >
                      {period === 'ALL' ? 'All Time' : period}
                    </button>
                  ))}
                </div>
              </div>

              {factorLoading ? (
                <div className="py-8 text-center text-gray-500">Loading factor analysis...</div>
              ) : factorBenchmarking ? (
                <>
                  {/* Summary Stats */}
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-6">
                    <div className="bg-gray-50 rounded-lg p-3">
                      <div className="text-sm text-gray-600">Total Return</div>
                      <div className={`text-xl font-bold ${factorBenchmarking.total_return >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                        {factorBenchmarking.total_return_pct?.toFixed(2)}%
                      </div>
                    </div>
                    <div className="bg-gray-50 rounded-lg p-3">
                      <div className="text-sm text-gray-600">Alpha (Ann.)</div>
                      <div className={`text-xl font-bold ${(factorBenchmarking.regression?.alpha_annualized || 0) >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                        {factorBenchmarking.regression?.alpha_annualized?.toFixed(2)}%
                      </div>
                    </div>
                    <div className="bg-gray-50 rounded-lg p-3">
                      <div className="text-sm text-gray-600">R-Squared</div>
                      <div className="text-xl font-bold text-gray-800">
                        {(factorBenchmarking.regression?.r_squared * 100)?.toFixed(1)}%
                      </div>
                    </div>
                    <div className="bg-gray-50 rounded-lg p-3">
                      <div className="text-sm text-gray-600">Residual Vol</div>
                      <div className="text-xl font-bold text-gray-800">
                        {factorBenchmarking.regression?.residual_std?.toFixed(2)}%
                      </div>
                    </div>
                  </div>

                  {/* Factor Exposures and Attribution Table */}
                  <div className="overflow-x-auto">
                    <table className="table w-full text-sm">
                      <thead>
                        <tr className="border-b border-gray-200">
                          <th className="text-left py-2 px-3">Factor</th>
                          <th className="text-right py-2 px-3">Beta</th>
                          <th className="text-right py-2 px-3">Factor Return</th>
                          <th className="text-right py-2 px-3">Contribution</th>
                          <th className="text-right py-2 px-3">% of Total</th>
                          <th className="text-right py-2 px-3">t-Stat</th>
                          <th className="text-right py-2 px-3">Significance</th>
                        </tr>
                      </thead>
                      <tbody>
                        {Object.entries(factorBenchmarking.factor_contributions || {}).map(([key, factor]: [string, any]) => (
                          <tr key={key} className="border-b border-gray-100 hover:bg-gray-50">
                            <td className="py-2 px-3 font-medium">{factor.name}</td>
                            <td className="text-right py-2 px-3">{factor.beta?.toFixed(3)}</td>
                            <td className={`text-right py-2 px-3 ${(factor.factor_return || 0) >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                              {factor.factor_return?.toFixed(2)}%
                            </td>
                            <td className={`text-right py-2 px-3 ${(factor.contribution || 0) >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                              {factor.contribution?.toFixed(2)}%
                            </td>
                            <td className="text-right py-2 px-3">
                              {factor.contribution_pct?.toFixed(1)}%
                            </td>
                            <td className="text-right py-2 px-3">{factor.t_stat?.toFixed(2)}</td>
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
                          <td className="py-2 px-3 font-medium">Alpha</td>
                          <td className="text-right py-2 px-3">-</td>
                          <td className="text-right py-2 px-3">-</td>
                          <td className={`text-right py-2 px-3 ${(factorBenchmarking.alpha_contribution || 0) >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                            {factorBenchmarking.alpha_contribution?.toFixed(2)}%
                          </td>
                          <td className="text-right py-2 px-3">
                            {factorBenchmarking.alpha_contribution_pct?.toFixed(1)}%
                          </td>
                          <td className="text-right py-2 px-3" colSpan={2}>-</td>
                        </tr>
                        {/* Residual row */}
                        <tr className="border-b border-gray-100 bg-gray-50">
                          <td className="py-2 px-3 font-medium">Residual (Unexplained)</td>
                          <td className="text-right py-2 px-3">-</td>
                          <td className="text-right py-2 px-3">-</td>
                          <td className={`text-right py-2 px-3 ${(factorBenchmarking.residual_contribution || 0) >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                            {factorBenchmarking.residual_contribution?.toFixed(2)}%
                          </td>
                          <td className="text-right py-2 px-3">
                            {factorBenchmarking.residual_contribution_pct?.toFixed(1)}%
                          </td>
                          <td className="text-right py-2 px-3" colSpan={2}>-</td>
                        </tr>
                      </tbody>
                    </table>
                  </div>

                  {/* Period info */}
                  <div className="mt-4 pt-4 border-t text-sm text-gray-500">
                    <span>Analysis period: {factorBenchmarking.period?.start_date} to {factorBenchmarking.period?.end_date}</span>
                    <span className="mx-2">|</span>
                    <span>{factorBenchmarking.period?.trading_days || factorBenchmarking.regression?.n_observations} trading days</span>
                    <span className="mx-2">|</span>
                    <span>Durbin-Watson: {factorBenchmarking.regression?.durbin_watson?.toFixed(2)}</span>
                  </div>
                </>
              ) : (
                <div className="py-8 text-center text-gray-500">
                  Factor analysis not available. Ensure sufficient return data exists.
                </div>
              )}
            </div>

            {/* Unpriced Instruments */}
            {unpriced.length > 0 && (
              <div className="card bg-yellow-50">
                <h3 className="text-lg font-semibold mb-2 text-yellow-800">Unpriced Instruments</h3>
                <p className="text-sm text-yellow-700 mb-4">
                  The following securities have positions but no pricing data. They are excluded from analytics.
                </p>
                <div className="space-y-2">
                  {unpriced.map((item: any) => (
                    <div key={item.symbol} className="text-sm">
                      <span className="font-semibold">{item.symbol}</span> ({item.asset_class}) -
                      Last seen: {format(new Date(item.last_seen_date), 'MMM d, yyyy')}
                    </div>
                  ))}
                </div>
              </div>
            )}
          </>
        )}
      </div>
    </Layout>
  );
}
