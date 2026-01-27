import { useState, useEffect, useMemo } from 'react';
import Layout from '@/components/Layout';
import { api } from '@/lib/api';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer, PieChart, Pie, Cell } from 'recharts';
import Select from 'react-select';
import { format, subMonths, startOfYear } from 'date-fns';

// Time period options for the performance chart
type ChartPeriod = '1M' | '3M' | 'YTD' | '1Y' | 'ALL';

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
  const [factors, setFactors] = useState<any>(null);
  const [unpriced, setUnpriced] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [returnMode, setReturnMode] = useState<'TWR' | 'Simple'>('TWR');
  const [chartPeriod, setChartPeriod] = useState<ChartPeriod>('ALL');

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
      const [summaryData, returnsData, benchmarksData, holdingsData, riskData, factorsData, unpricedData, sectorWeights] = await Promise.all([
        api.getSummary(selectedView.view_type, selectedView.view_id),
        api.getReturns(selectedView.view_type, selectedView.view_id),
        api.getBenchmarkReturns(['SPY', 'QQQ', 'INDU']).catch(() => ({})),
        api.getHoldings(selectedView.view_type, selectedView.view_id),
        api.getRisk(selectedView.view_type, selectedView.view_id).catch(() => null),
        api.getFactorExposures(selectedView.view_type, selectedView.view_id).catch(() => null),
        api.getUnpricedInstruments().catch(() => []),
        api.getSectorWeights(selectedView.view_type, selectedView.view_id).catch(() => null),
      ]);

      setSummary(summaryData);
      setReturns(returnsData);
      setBenchmarkReturns(benchmarksData);
      setHoldings(holdingsData);
      setRisk(riskData);
      setFactors(factorsData);
      setUnpriced(unpricedData);
      setSectorData(sectorWeights);
    } catch (error) {
      console.error('Failed to load view data:', error);
    } finally {
      setLoading(false);
    }
  };

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
    );

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
      );
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

  // Filter chart data by selected time period
  const filteredChartData = useMemo(() => {
    if (!chartData.length || chartPeriod === 'ALL') return chartData;

    const now = new Date();
    let cutoffDate: Date;
    switch (chartPeriod) {
      case '1M':
        cutoffDate = subMonths(now, 1);
        break;
      case '3M':
        cutoffDate = subMonths(now, 3);
        break;
      case 'YTD':
        cutoffDate = startOfYear(now);
        break;
      case '1Y':
        cutoffDate = subMonths(now, 12);
        break;
      default:
        return chartData;
    }

    const cutoffStr = format(cutoffDate, 'yyyy-MM-dd');
    const filtered = chartData.filter((d: any) => d.date >= cutoffStr);

    if (filtered.length === 0) return chartData;

    // Re-normalize so the filtered window starts at 1.0
    const first = filtered[0];
    const allSeries = ['Portfolio', 'SPY', 'QQQ', 'INDU'];
    const baselineValues: any = {};
    allSeries.forEach(s => {
      if (first[s] !== undefined && first[s] !== null) {
        baselineValues[s] = first[s];
      }
    });

    return filtered.map((point: any) => {
      const normalized: any = { date: point.date };
      allSeries.forEach(s => {
        if (point[s] !== undefined && point[s] !== null && baselineValues[s]) {
          normalized[s] = point[s] / baselineValues[s];
        }
      });
      return normalized;
    });
  }, [chartData, chartPeriod]);

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

              <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
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
                <div className="flex items-center gap-4">
                  <div className="flex gap-1">
                    {(['1M', '3M', 'YTD', '1Y', 'ALL'] as ChartPeriod[]).map((period) => (
                      <button
                        key={period}
                        onClick={() => setChartPeriod(period)}
                        className={`px-3 py-1 text-sm rounded ${
                          chartPeriod === period
                            ? 'bg-blue-600 text-white'
                            : 'bg-gray-100 text-gray-700 hover:bg-gray-200'
                        }`}
                      >
                        {period === 'ALL' ? 'All Time' : period}
                      </button>
                    ))}
                  </div>
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
              </div>
              <ResponsiveContainer width="100%" height={400}>
                <LineChart data={filteredChartData}>
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

            {/* Holdings */}
            {holdings && (
              <div className="card">
                <h3 className="text-lg font-semibold mb-4">Top Holdings</h3>
                <table className="table">
                  <thead>
                    <tr>
                      <th>Symbol</th>
                      <th>Name</th>
                      <th className="text-right">Shares</th>
                      <th className="text-right">Price</th>
                      <th className="text-right">Market Value</th>
                      <th className="text-right">Weight</th>
                    </tr>
                  </thead>
                  <tbody>
                    {holdings.holdings.slice(0, 10).map((h: any) => (
                      <tr key={h.symbol}>
                        <td className="font-semibold">{h.symbol}</td>
                        <td>{h.asset_name}</td>
                        <td className="text-right">{h.shares.toFixed(2)}</td>
                        <td className="text-right">{formatCurrency(h.price)}</td>
                        <td className="text-right">{formatCurrency(h.market_value)}</td>
                        <td className="text-right">{formatPercent(h.weight)}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
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

            {/* Factor Exposures */}
            {factors && (
              <div className="card">
                <h3 className="text-lg font-semibold mb-4">Factor Exposures (STYLE7)</h3>
                <div className="grid grid-cols-2 md:grid-cols-4 gap-4">
                  {factors.exposures.map((exp: any) => (
                    <div key={exp.factor_name} className="border-l-4 border-indigo-500 pl-4">
                      <div className="text-sm text-gray-600">{exp.factor_name}</div>
                      <div className="text-lg font-semibold">{exp.beta.toFixed(3)}</div>
                    </div>
                  ))}
                </div>
                <div className="mt-4 pt-4 border-t">
                  <div className="grid grid-cols-2 gap-4">
                    <div>
                      <div className="text-sm text-gray-600">Alpha (Annualized)</div>
                      <div className="text-lg font-semibold">
                        {factors.alpha ? formatPercent(factors.alpha) : 'N/A'}
                      </div>
                    </div>
                    <div>
                      <div className="text-sm text-gray-600">R-Squared</div>
                      <div className="text-lg font-semibold">
                        {factors.r_squared ? factors.r_squared.toFixed(3) : 'N/A'}
                      </div>
                    </div>
                  </div>
                </div>
              </div>
            )}

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
