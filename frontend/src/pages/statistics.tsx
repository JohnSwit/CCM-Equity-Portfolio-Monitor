import { useState, useEffect } from 'react';
import Layout from '@/components/Layout';
import { api } from '@/lib/api';
import { format } from 'date-fns';
import Select from 'react-select';

export default function PortfolioStatisticsPage() {
  const [views, setViews] = useState<any[]>([]);
  const [selectedView, setSelectedView] = useState<any>(null);
  const [loading, setLoading] = useState(false);
  const [benchmark, setBenchmark] = useState('SPY');
  const [window, setWindow] = useState(252);

  // Statistics data
  const [contributionData, setContributionData] = useState<any>(null);
  const [volatilityData, setVolatilityData] = useState<any>(null);
  const [drawdownData, setDrawdownData] = useState<any>(null);
  const [varData, setVarData] = useState<any>(null);
  const [factorData, setFactorData] = useState<any>(null);

  useEffect(() => {
    loadViews();
  }, []);

  useEffect(() => {
    if (selectedView) {
      loadStatistics();
    }
  }, [selectedView, benchmark, window]);

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
      const [contrib, vol, dd, varCvar, factors] = await Promise.all([
        api.getContributionToReturns(selectedView.view_type, selectedView.view_id, undefined, undefined, 20).catch(() => null),
        api.getVolatilityMetrics(selectedView.view_type, selectedView.view_id, benchmark, window).catch(() => null),
        api.getDrawdownAnalysis(selectedView.view_type, selectedView.view_id).catch(() => null),
        api.getVarCvar(selectedView.view_type, selectedView.view_id, '95,99', window).catch(() => null),
        api.getFactorAnalysis(selectedView.view_type, selectedView.view_id).catch(() => null),
      ]);

      setContributionData(contrib);
      setVolatilityData(vol);
      setDrawdownData(dd);
      setVarData(varCvar);
      setFactorData(factors);
    } catch (error) {
      console.error('Failed to load statistics:', error);
    } finally {
      setLoading(false);
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
                <option value="SPY">S&P 500 (SPY)</option>
                <option value="QQQ">Nasdaq (QQQ)</option>
                <option value="INDU">Dow Jones (INDU)</option>
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

            {/* Factor Analysis */}
            {factorData && !factorData.error && (
              <div className="card">
                <h2 className="text-xl font-bold mb-4">Factor Analysis (STYLE7)</h2>
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

                {factorData.factor_exposures && Object.keys(factorData.factor_exposures).length > 0 && (
                  <div>
                    <h3 className="text-sm font-semibold mb-2">Factor Exposures (Beta)</h3>
                    <div className="grid grid-cols-2 md:grid-cols-4 gap-3">
                      {Object.entries(factorData.factor_exposures).map(([factor, beta]: [string, any]) => (
                        <div key={factor} className="p-3 bg-gray-50 rounded">
                          <div className="text-xs text-gray-600 uppercase">{factor}</div>
                          <div className="text-lg font-semibold">{formatNumber(beta, 3)}</div>
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                <div className="mt-4 text-xs text-gray-500">
                  As of {factorData.as_of_date && format(new Date(factorData.as_of_date), 'MMM d, yyyy')} |
                  Window: {factorData.window_days} days
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
          </>
        )}
      </div>
    </Layout>
  );
}
