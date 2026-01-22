import { useState, useEffect } from 'react';
import Layout from '@/components/Layout';
import { api } from '@/lib/api';
import { LineChart, Line, XAxis, YAxis, CartesianGrid, Tooltip, Legend, ResponsiveContainer } from 'recharts';
import Select from 'react-select';
import { format } from 'date-fns';

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
      mergeChartData();
    }
  }, [returns, benchmarkReturns]);

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
      const [summaryData, returnsData, benchmarksData, holdingsData, riskData, factorsData, unpricedData] = await Promise.all([
        api.getSummary(selectedView.view_type, selectedView.view_id),
        api.getReturns(selectedView.view_type, selectedView.view_id),
        api.getBenchmarkReturns(['SPY', 'QQQ', 'INDU']).catch(() => ({})),
        api.getHoldings(selectedView.view_type, selectedView.view_id),
        api.getRisk(selectedView.view_type, selectedView.view_id).catch(() => null),
        api.getFactorExposures(selectedView.view_type, selectedView.view_id).catch(() => null),
        api.getUnpricedInstruments().catch(() => []),
      ]);

      setSummary(summaryData);
      setReturns(returnsData);
      setBenchmarkReturns(benchmarksData);
      setHoldings(holdingsData);
      setRisk(riskData);
      setFactors(factorsData);
      setUnpriced(unpricedData);
    } catch (error) {
      console.error('Failed to load view data:', error);
    } finally {
      setLoading(false);
    }
  };

  const mergeChartData = () => {
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
          if (dataByDate[dateStr]) {
            dataByDate[dateStr][code] = r.index_value;
          }
        });
      }
    });

    // Convert to array and sort by date
    const merged = Object.values(dataByDate).sort((a: any, b: any) =>
      a.date.localeCompare(b.date)
    );

    setChartData(merged);
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

            {/* Performance Chart */}
            <div className="card">
              <h3 className="text-lg font-semibold mb-4">Performance vs Benchmarks</h3>
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
