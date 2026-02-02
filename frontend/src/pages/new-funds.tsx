import { useState, useEffect, useCallback } from 'react';
import Layout from '../components/Layout';
import { api } from '../lib/api';

interface Industry {
  industry: string;
  sp500_weight: number;
  ccm_weight: number;
  adjustment_bps: number;
  proforma_weight: number;
  active_weight: number;
  dollar_allocation: number;
}

interface TickerAllocation {
  id: string;
  ticker: string;
  industry: string;
  pct_of_industry: number;
  dollar_amount: number;
  shares: number;
  price: number;
  security_name?: string;
}

interface Account {
  id: number;
  account_number: string;
  name: string;
}

export default function NewFundsPage() {
  const [industries, setIndustries] = useState<Industry[]>([]);
  const [accounts, setAccounts] = useState<Account[]>([]);
  const [selectedAccount, setSelectedAccount] = useState<Account | null>(null);
  const [totalAmount, setTotalAmount] = useState<number>(0);
  const [tickerAllocations, setTickerAllocations] = useState<TickerAllocation[]>([]);
  const [expandedIndustry, setExpandedIndustry] = useState<string | null>(null);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [successMessage, setSuccessMessage] = useState<string | null>(null);

  // Load accounts on mount
  useEffect(() => {
    loadAccounts();
  }, []);

  const loadAccounts = async () => {
    try {
      const data = await api.getNewFundsAccounts();
      setAccounts(data);
    } catch (err: any) {
      setError('Failed to load accounts');
    }
  };

  // Calculate totals
  const totalAllocated = tickerAllocations.reduce((sum, a) => sum + a.dollar_amount, 0);
  const remainingToAllocate = totalAmount - totalAllocated;

  // Get allocations for a specific industry
  const getIndustryAllocations = (industry: string) => {
    return tickerAllocations.filter(a => a.industry === industry);
  };

  // Get remaining amount for an industry
  const getIndustryRemaining = (industry: Industry) => {
    const allocated = getIndustryAllocations(industry.industry)
      .reduce((sum, a) => sum + a.dollar_amount, 0);
    return industry.dollar_allocation - allocated;
  };

  // Handle CSV upload
  const handleFileUpload = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) return;

    setLoading(true);
    setError(null);

    try {
      const data = await api.parseIndustryCSV(file);
      if (data.success) {
        setIndustries(data.industries);
        setTickerAllocations([]);
        setSuccessMessage(`Loaded ${data.industries.length} industries`);
        setTimeout(() => setSuccessMessage(null), 3000);
      }
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to parse CSV');
    } finally {
      setLoading(false);
    }
  };

  // Handle total amount change
  const handleTotalAmountChange = (value: string) => {
    const amount = parseFloat(value.replace(/,/g, '')) || 0;
    setTotalAmount(amount);

    // Recalculate dollar allocations for industries
    if (industries.length > 0) {
      const updated = industries.map(ind => ({
        ...ind,
        dollar_allocation: amount * ind.proforma_weight
      }));
      setIndustries(updated);
    }
  };

  // Handle adjustment change (basis points)
  const handleAdjustmentChange = (industry: string, bps: number) => {
    const updated = industries.map(ind => {
      if (ind.industry === industry) {
        const newProforma = ind.ccm_weight + (bps / 10000);
        return {
          ...ind,
          adjustment_bps: bps,
          proforma_weight: newProforma,
          active_weight: newProforma - ind.sp500_weight,
          dollar_allocation: totalAmount * newProforma
        };
      }
      return ind;
    });

    // Normalize if needed
    const totalProforma = updated.reduce((sum, i) => sum + i.proforma_weight, 0);
    if (Math.abs(totalProforma - 1) > 0.0001) {
      const normalized = updated.map(ind => ({
        ...ind,
        proforma_weight: ind.proforma_weight / totalProforma,
        active_weight: (ind.proforma_weight / totalProforma) - ind.sp500_weight,
        dollar_allocation: totalAmount * (ind.proforma_weight / totalProforma)
      }));
      setIndustries(normalized);
    } else {
      setIndustries(updated);
    }
  };

  // Add ticker to industry
  const addTickerToIndustry = (industry: string) => {
    const newAllocation: TickerAllocation = {
      id: `${industry}-${Date.now()}`,
      ticker: '',
      industry,
      pct_of_industry: 0,
      dollar_amount: 0,
      shares: 0,
      price: 0
    };
    setTickerAllocations([...tickerAllocations, newAllocation]);
  };

  // Update ticker allocation
  const updateTickerAllocation = async (id: string, field: string, value: any) => {
    const allocation = tickerAllocations.find(a => a.id === id);
    if (!allocation) return;

    const industry = industries.find(i => i.industry === allocation.industry);
    if (!industry) return;

    let updatedAllocation = { ...allocation, [field]: value };

    // If ticker changed, fetch price
    if (field === 'ticker' && value) {
      try {
        const priceData = await api.getTickerPrice(value);
        updatedAllocation.price = priceData.price;
        updatedAllocation.security_name = priceData.security_name;

        // Recalculate shares if we have a dollar amount
        if (updatedAllocation.dollar_amount > 0) {
          updatedAllocation.shares = Math.floor(updatedAllocation.dollar_amount / priceData.price);
        }
      } catch (err) {
        setError(`Ticker ${value} not found`);
        setTimeout(() => setError(null), 3000);
      }
    }

    // If percentage changed, recalculate dollar amount and shares
    if (field === 'pct_of_industry') {
      const pct = parseFloat(value) || 0;
      updatedAllocation.pct_of_industry = pct;
      updatedAllocation.dollar_amount = industry.dollar_allocation * (pct / 100);

      if (updatedAllocation.price > 0) {
        updatedAllocation.shares = Math.floor(updatedAllocation.dollar_amount / updatedAllocation.price);
      }
    }

    setTickerAllocations(tickerAllocations.map(a => a.id === id ? updatedAllocation : a));
  };

  // Remove ticker allocation
  const removeTickerAllocation = (id: string) => {
    setTickerAllocations(tickerAllocations.filter(a => a.id !== id));
  };

  // Generate Schwab CSV
  const handleExecute = async () => {
    if (!selectedAccount) {
      setError('Please select an account');
      return;
    }

    const validAllocations = tickerAllocations.filter(a => a.ticker && a.shares > 0);
    if (validAllocations.length === 0) {
      setError('No valid allocations to export');
      return;
    }

    setLoading(true);
    try {
      const blob = await api.generateSchwabCSV(selectedAccount.account_number, validAllocations);

      // Download the file
      const url = window.URL.createObjectURL(new Blob([blob]));
      const link = document.createElement('a');
      link.href = url;
      link.setAttribute('download', `schwab_allocation_${selectedAccount.account_number}_${new Date().toISOString().split('T')[0]}.csv`);
      document.body.appendChild(link);
      link.click();
      link.remove();

      setSuccessMessage('Schwab CSV generated successfully!');
      setTimeout(() => setSuccessMessage(null), 5000);
    } catch (err: any) {
      setError('Failed to generate CSV');
    } finally {
      setLoading(false);
    }
  };

  // Format currency
  const formatCurrency = (value: number) => {
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD',
      minimumFractionDigits: 0,
      maximumFractionDigits: 0
    }).format(value);
  };

  // Format percentage
  const formatPct = (value: number, decimals: number = 2) => {
    return `${(value * 100).toFixed(decimals)}%`;
  };

  return (
    <Layout>
      <div className="space-y-6">
        <div className="flex justify-between items-center">
          <h1 className="text-2xl font-bold text-gray-900">New Funds Allocation</h1>
        </div>

        {error && (
          <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded">
            {error}
          </div>
        )}

        {successMessage && (
          <div className="bg-green-50 border border-green-200 text-green-700 px-4 py-3 rounded">
            {successMessage}
          </div>
        )}

        {/* Setup Section */}
        <div className="card">
          <h2 className="text-lg font-semibold mb-4">Setup</h2>
          <div className="grid grid-cols-1 md:grid-cols-3 gap-4">
            {/* CSV Upload */}
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Upload Industry Weights CSV
              </label>
              <input
                type="file"
                accept=".csv"
                onChange={handleFileUpload}
                className="block w-full text-sm text-gray-500 file:mr-4 file:py-2 file:px-4 file:rounded file:border-0 file:text-sm file:font-semibold file:bg-blue-50 file:text-blue-700 hover:file:bg-blue-100"
              />
              <p className="mt-1 text-xs text-gray-500">
                CSV with columns: Industry, Weight
              </p>
            </div>

            {/* Account Selector */}
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Account
              </label>
              <select
                value={selectedAccount?.id || ''}
                onChange={(e) => {
                  const account = accounts.find(a => a.id === parseInt(e.target.value));
                  setSelectedAccount(account || null);
                }}
                className="w-full px-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500"
              >
                <option value="">Select account...</option>
                {accounts.map(account => (
                  <option key={account.id} value={account.id}>
                    {account.name} ({account.account_number})
                  </option>
                ))}
              </select>
            </div>

            {/* Total Amount */}
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">
                Total Amount to Allocate
              </label>
              <div className="relative">
                <span className="absolute left-3 top-2 text-gray-500">$</span>
                <input
                  type="text"
                  value={totalAmount ? totalAmount.toLocaleString() : ''}
                  onChange={(e) => handleTotalAmountChange(e.target.value)}
                  placeholder="1,000,000"
                  className="w-full pl-8 pr-3 py-2 border border-gray-300 rounded-md shadow-sm focus:outline-none focus:ring-blue-500 focus:border-blue-500"
                />
              </div>
            </div>
          </div>
        </div>

        {/* Summary Cards */}
        {totalAmount > 0 && (
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
            <div className="card bg-blue-50">
              <div className="text-sm text-blue-600">Total to Allocate</div>
              <div className="text-2xl font-bold text-blue-900">{formatCurrency(totalAmount)}</div>
            </div>
            <div className="card bg-green-50">
              <div className="text-sm text-green-600">Allocated</div>
              <div className="text-2xl font-bold text-green-900">{formatCurrency(totalAllocated)}</div>
            </div>
            <div className="card bg-yellow-50">
              <div className="text-sm text-yellow-600">Remaining</div>
              <div className="text-2xl font-bold text-yellow-900">{formatCurrency(remainingToAllocate)}</div>
            </div>
            <div className="card bg-purple-50">
              <div className="text-sm text-purple-600">% Allocated</div>
              <div className="text-2xl font-bold text-purple-900">
                {totalAmount > 0 ? ((totalAllocated / totalAmount) * 100).toFixed(1) : 0}%
              </div>
            </div>
          </div>
        )}

        {/* Industry Allocation Table */}
        {industries.length > 0 && (
          <div className="card">
            <h2 className="text-lg font-semibold mb-4">Industry Allocation</h2>
            <div className="overflow-x-auto">
              <table className="min-w-full divide-y divide-gray-200">
                <thead className="bg-gray-50">
                  <tr>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Industry</th>
                    <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">S&P Weight</th>
                    <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">CCM Weight</th>
                    <th className="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase">Adjust (bps)</th>
                    <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Pro-Forma</th>
                    <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Active</th>
                    <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">$ Allocation</th>
                    <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Remaining</th>
                    <th className="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase">Actions</th>
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-200">
                  {industries.map((industry) => {
                    const industryAllocations = getIndustryAllocations(industry.industry);
                    const industryRemaining = getIndustryRemaining(industry);
                    const isExpanded = expandedIndustry === industry.industry;

                    return (
                      <>
                        <tr key={industry.industry} className="hover:bg-gray-50">
                          <td className="px-4 py-3 text-sm font-medium text-gray-900">
                            <button
                              onClick={() => setExpandedIndustry(isExpanded ? null : industry.industry)}
                              className="flex items-center"
                            >
                              <span className="mr-2">{isExpanded ? '▼' : '▶'}</span>
                              {industry.industry}
                            </button>
                          </td>
                          <td className="px-4 py-3 text-sm text-right text-gray-600">
                            {formatPct(industry.sp500_weight)}
                          </td>
                          <td className="px-4 py-3 text-sm text-right text-gray-600">
                            {formatPct(industry.ccm_weight)}
                          </td>
                          <td className="px-4 py-3 text-center">
                            <input
                              type="number"
                              value={industry.adjustment_bps}
                              onChange={(e) => handleAdjustmentChange(industry.industry, parseInt(e.target.value) || 0)}
                              className="w-20 px-2 py-1 text-sm text-center border border-gray-300 rounded"
                            />
                          </td>
                          <td className="px-4 py-3 text-sm text-right font-medium text-gray-900">
                            {formatPct(industry.proforma_weight)}
                          </td>
                          <td className={`px-4 py-3 text-sm text-right font-medium ${
                            industry.active_weight > 0 ? 'text-green-600' :
                            industry.active_weight < 0 ? 'text-red-600' : 'text-gray-600'
                          }`}>
                            {industry.active_weight > 0 ? '+' : ''}{formatPct(industry.active_weight)}
                          </td>
                          <td className="px-4 py-3 text-sm text-right font-medium text-gray-900">
                            {formatCurrency(industry.dollar_allocation)}
                          </td>
                          <td className={`px-4 py-3 text-sm text-right font-medium ${
                            industryRemaining < 1 ? 'text-green-600' : 'text-yellow-600'
                          }`}>
                            {formatCurrency(industryRemaining)}
                          </td>
                          <td className="px-4 py-3 text-center">
                            <button
                              onClick={() => addTickerToIndustry(industry.industry)}
                              className="text-blue-600 hover:text-blue-800 text-sm"
                            >
                              + Add Ticker
                            </button>
                          </td>
                        </tr>

                        {/* Expanded ticker allocations */}
                        {isExpanded && (
                          <tr>
                            <td colSpan={9} className="px-4 py-3 bg-gray-50">
                              <div className="ml-6">
                                {industryAllocations.length === 0 ? (
                                  <p className="text-sm text-gray-500 italic">
                                    No tickers allocated. Click "Add Ticker" to begin.
                                  </p>
                                ) : (
                                  <table className="min-w-full">
                                    <thead>
                                      <tr className="text-xs text-gray-500">
                                        <th className="px-2 py-1 text-left">Ticker</th>
                                        <th className="px-2 py-1 text-left">Name</th>
                                        <th className="px-2 py-1 text-right">% of Industry</th>
                                        <th className="px-2 py-1 text-right">$ Amount</th>
                                        <th className="px-2 py-1 text-right">Price</th>
                                        <th className="px-2 py-1 text-right">Shares</th>
                                        <th className="px-2 py-1"></th>
                                      </tr>
                                    </thead>
                                    <tbody>
                                      {industryAllocations.map((alloc) => (
                                        <tr key={alloc.id} className="text-sm">
                                          <td className="px-2 py-1">
                                            <input
                                              type="text"
                                              value={alloc.ticker}
                                              onChange={(e) => updateTickerAllocation(alloc.id, 'ticker', e.target.value.toUpperCase())}
                                              onBlur={(e) => updateTickerAllocation(alloc.id, 'ticker', e.target.value.toUpperCase())}
                                              placeholder="AAPL"
                                              className="w-20 px-2 py-1 border border-gray-300 rounded text-sm"
                                            />
                                          </td>
                                          <td className="px-2 py-1 text-gray-600">
                                            {alloc.security_name || '-'}
                                          </td>
                                          <td className="px-2 py-1 text-right">
                                            <input
                                              type="number"
                                              value={alloc.pct_of_industry || ''}
                                              onChange={(e) => updateTickerAllocation(alloc.id, 'pct_of_industry', e.target.value)}
                                              placeholder="100"
                                              min="0"
                                              max="100"
                                              className="w-20 px-2 py-1 border border-gray-300 rounded text-sm text-right"
                                            />%
                                          </td>
                                          <td className="px-2 py-1 text-right font-medium">
                                            {formatCurrency(alloc.dollar_amount)}
                                          </td>
                                          <td className="px-2 py-1 text-right text-gray-600">
                                            {alloc.price > 0 ? `$${alloc.price.toFixed(2)}` : '-'}
                                          </td>
                                          <td className="px-2 py-1 text-right font-medium text-blue-600">
                                            {alloc.shares > 0 ? alloc.shares.toLocaleString() : '-'}
                                          </td>
                                          <td className="px-2 py-1">
                                            <button
                                              onClick={() => removeTickerAllocation(alloc.id)}
                                              className="text-red-500 hover:text-red-700"
                                            >
                                              ✕
                                            </button>
                                          </td>
                                        </tr>
                                      ))}
                                    </tbody>
                                  </table>
                                )}
                              </div>
                            </td>
                          </tr>
                        )}
                      </>
                    );
                  })}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {/* Execute Button */}
        {tickerAllocations.length > 0 && (
          <div className="card">
            <div className="flex items-center justify-between">
              <div>
                <h3 className="font-medium">Ready to Execute</h3>
                <p className="text-sm text-gray-500">
                  {tickerAllocations.filter(a => a.ticker && a.shares > 0).length} tickers with {' '}
                  {tickerAllocations.filter(a => a.shares > 0).reduce((sum, a) => sum + a.shares, 0).toLocaleString()} total shares
                </p>
              </div>
              <button
                onClick={handleExecute}
                disabled={loading || !selectedAccount}
                className={`px-6 py-3 rounded-lg font-semibold text-white ${
                  loading || !selectedAccount
                    ? 'bg-gray-400 cursor-not-allowed'
                    : 'bg-green-600 hover:bg-green-700'
                }`}
              >
                {loading ? 'Generating...' : 'Execute - Download Schwab CSV'}
              </button>
            </div>
          </div>
        )}

        {/* Help Section */}
        <div className="card bg-gray-50">
          <h3 className="font-medium mb-2">Instructions</h3>
          <ol className="list-decimal list-inside text-sm text-gray-600 space-y-1">
            <li>Upload a CSV file with S&P 500 industry weights (columns: Industry, Weight)</li>
            <li>Select the account to allocate new funds to</li>
            <li>Enter the total dollar amount to allocate</li>
            <li>Optionally adjust CCM weights using basis point adjustments</li>
            <li>Click on an industry row to expand and add tickers</li>
            <li>Enter ticker symbols and percentage allocation within each industry</li>
            <li>Click "Execute" to generate a Schwab-compatible CSV for bulk upload</li>
          </ol>
          <div className="mt-4 p-3 bg-blue-50 rounded">
            <p className="text-sm text-blue-800">
              <strong>Schwab CSV Format:</strong> Account Number, B (Buy), Shares, Ticker, M (Market)
            </p>
          </div>
        </div>
      </div>
    </Layout>
  );
}
