import { useState, useEffect, Fragment } from 'react';
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
  excluded: boolean;
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
        // Add excluded field to each industry
        const industriesWithExcluded = data.industries.map((ind: any) => ({
          ...ind,
          excluded: false
        }));
        setIndustries(industriesWithExcluded);
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

  // Handle portfolio CSV upload
  const handlePortfolioUpload = async (event: React.ChangeEvent<HTMLInputElement>) => {
    const file = event.target.files?.[0];
    if (!file) return;

    setLoading(true);
    setError(null);

    try {
      const data = await api.parsePortfolioCSV(file);
      if (data.success) {
        // Convert allocations to our format with IDs
        const newAllocations: TickerAllocation[] = data.allocations.map((alloc: any, idx: number) => ({
          id: `${alloc.industry}-${alloc.ticker}-${idx}`,
          ticker: alloc.ticker,
          industry: alloc.industry,
          pct_of_industry: alloc.pct_of_industry,
          dollar_amount: 0, // Will be recalculated
          shares: 0, // Will be recalculated
          price: alloc.price || 0,
          security_name: alloc.security_name
        }));

        // Calculate dollar amounts if we have industries and total amount
        if (industries.length > 0 && totalAmount > 0) {
          const updatedAllocations = newAllocations.map(alloc => {
            const industry = industries.find(i => i.industry === alloc.industry);
            if (industry) {
              const dollarAmount = industry.dollar_allocation * (alloc.pct_of_industry / 100);
              const shares = alloc.price > 0 ? Math.floor(dollarAmount / alloc.price) : 0;
              return { ...alloc, dollar_amount: dollarAmount, shares };
            }
            return alloc;
          });
          setTickerAllocations(updatedAllocations);
        } else {
          setTickerAllocations(newAllocations);
        }

        setSuccessMessage(`Loaded ${data.count} ticker allocations across ${data.industries_found.length} industries`);
        if (data.errors && data.errors.length > 0) {
          setError(`Warning: ${data.errors.length} rows had errors`);
        }
        setTimeout(() => setSuccessMessage(null), 5000);
      }
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to parse portfolio CSV');
    } finally {
      setLoading(false);
    }
  };

  // Handle total amount change
  const handleTotalAmountChange = (value: string) => {
    const amount = parseFloat(value.replace(/,/g, '')) || 0;
    setTotalAmount(amount);

    // Recalculate dollar allocations for industries (respecting exclusions)
    if (industries.length > 0) {
      const updated = industries.map(ind => {
        if (ind.excluded) {
          return { ...ind, dollar_allocation: 0 }; // Excluded industries get $0
        }
        return {
          ...ind,
          dollar_allocation: amount * ind.proforma_weight
        };
      });
      setIndustries(updated);

      // Also recalculate ticker allocations
      if (tickerAllocations.length > 0) {
        const updatedTickers = tickerAllocations.map(alloc => {
          const industry = updated.find(i => i.industry === alloc.industry);
          if (industry && !industry.excluded && alloc.pct_of_industry > 0) {
            const dollarAmount = industry.dollar_allocation * (alloc.pct_of_industry / 100);
            const shares = alloc.price > 0 ? Math.floor(dollarAmount / alloc.price) : 0;
            return { ...alloc, dollar_amount: dollarAmount, shares };
          } else if (industry?.excluded) {
            return { ...alloc, dollar_amount: 0, shares: 0 };
          }
          return alloc;
        });
        setTickerAllocations(updatedTickers);
      }
    }
  };

  // Handle adjustment change (basis points)
  const handleAdjustmentChange = (industry: string, bps: number) => {
    const targetIndustry = industries.find(i => i.industry === industry);
    if (!targetIndustry || targetIndustry.excluded) return; // Don't adjust excluded industries

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

    // Normalize if needed (only non-excluded industries)
    const nonExcluded = updated.filter(i => !i.excluded);
    const totalProforma = nonExcluded.reduce((sum, i) => sum + i.proforma_weight, 0);
    if (Math.abs(totalProforma - 1) > 0.0001 && totalProforma > 0) {
      const normalized = updated.map(ind => {
        if (ind.excluded) {
          return ind; // Keep excluded industries at 0
        }
        return {
          ...ind,
          proforma_weight: ind.proforma_weight / totalProforma,
          active_weight: (ind.proforma_weight / totalProforma) - ind.sp500_weight,
          dollar_allocation: totalAmount * (ind.proforma_weight / totalProforma)
        };
      });
      setIndustries(normalized);

      // Recalculate ALL ticker allocations based on new industry dollar allocations
      if (tickerAllocations.length > 0) {
        setTickerAllocations(tickerAllocations.map(alloc => {
          const ind = normalized.find(i => i.industry === alloc.industry);
          if (ind && !ind.excluded && alloc.pct_of_industry > 0) {
            const dollarAmount = ind.dollar_allocation * (alloc.pct_of_industry / 100);
            const shares = alloc.price > 0 ? Math.floor(dollarAmount / alloc.price) : 0;
            return { ...alloc, dollar_amount: dollarAmount, shares };
          } else if (ind?.excluded) {
            return { ...alloc, dollar_amount: 0, shares: 0 };
          }
          return alloc;
        }));
      }
    } else {
      setIndustries(updated);
    }
  };

  // Handle exclusion toggle (zero out allocation for an industry)
  const handleExclusionToggle = (industryName: string) => {
    const targetIndustry = industries.find(i => i.industry === industryName);
    if (!targetIndustry) return;

    const newExcluded = !targetIndustry.excluded;

    // Update the target industry
    let updated = industries.map(ind => {
      if (ind.industry === industryName) {
        if (newExcluded) {
          // Excluding: zero out the weights
          return {
            ...ind,
            excluded: true,
            proforma_weight: 0,
            active_weight: -ind.sp500_weight, // Full underweight vs S&P
            dollar_allocation: 0
          };
        } else {
          // Re-including: restore to ccm_weight + adjustment
          const baseWeight = ind.ccm_weight + (ind.adjustment_bps / 10000);
          return {
            ...ind,
            excluded: false,
            proforma_weight: baseWeight,
            active_weight: baseWeight - ind.sp500_weight,
            dollar_allocation: totalAmount * baseWeight
          };
        }
      }
      return ind;
    });

    // Renormalize non-excluded industries to sum to 1
    const nonExcluded = updated.filter(i => !i.excluded);
    const totalProforma = nonExcluded.reduce((sum, i) => sum + i.proforma_weight, 0);

    if (totalProforma > 0) {
      updated = updated.map(ind => {
        if (ind.excluded) {
          return ind; // Keep excluded at 0
        }
        return {
          ...ind,
          proforma_weight: ind.proforma_weight / totalProforma,
          active_weight: (ind.proforma_weight / totalProforma) - ind.sp500_weight,
          dollar_allocation: totalAmount * (ind.proforma_weight / totalProforma)
        };
      });
    }

    setIndustries(updated);

    // Recalculate ALL ticker allocations based on updated industry dollar allocations
    // (not just the excluded one - other industries' allocations have changed due to renormalization)
    setTickerAllocations(tickerAllocations.map(alloc => {
      const industry = updated.find(i => i.industry === alloc.industry);
      if (industry && !industry.excluded && alloc.pct_of_industry > 0) {
        const dollarAmount = industry.dollar_allocation * (alloc.pct_of_industry / 100);
        const shares = alloc.price > 0 ? Math.floor(dollarAmount / alloc.price) : 0;
        return { ...alloc, dollar_amount: dollarAmount, shares };
      } else if (industry?.excluded) {
        return { ...alloc, dollar_amount: 0, shares: 0 };
      }
      return alloc;
    }));
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
        {/* Page Header */}
        <div>
          <h1 className="text-2xl font-bold text-zinc-900">New Funds Allocation</h1>
          <p className="text-sm text-zinc-500 mt-1">
            Allocate new investments across industries and generate Schwab trade files
          </p>
        </div>

        {/* Error/Success Messages */}
        {error && (
          <div className="alert alert-danger flex justify-between items-center">
            <span>{error}</span>
            <button onClick={() => setError(null)} className="text-red-600 hover:text-red-800 font-medium">
              Dismiss
            </button>
          </div>
        )}

        {successMessage && (
          <div className="alert alert-success flex justify-between items-center">
            <span>{successMessage}</span>
            <button onClick={() => setSuccessMessage(null)} className="text-emerald-600 hover:text-emerald-800 font-medium">
              Dismiss
            </button>
          </div>
        )}

        {/* Setup Section */}
        <div className="card">
          <div className="card-header">
            <h2 className="card-title">Setup</h2>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 lg:grid-cols-4 gap-4">
            {/* Industry Weights CSV Upload */}
            <div>
              <label className="label">1. Upload Industry Weights CSV</label>
              <input
                type="file"
                accept=".csv"
                onChange={handleFileUpload}
                className="block w-full text-sm text-zinc-500
                  file:mr-4 file:py-2 file:px-4
                  file:rounded-lg file:border-0
                  file:text-sm file:font-medium
                  file:bg-blue-50 file:text-blue-700
                  hover:file:bg-blue-100
                  file:cursor-pointer cursor-pointer"
              />
              <p className="mt-1.5 text-xs text-zinc-500">
                CSV: Industry, Weight
              </p>
            </div>

            {/* Portfolio CSV Upload */}
            <div>
              <label className="label">2. Upload Portfolio CSV (Optional)</label>
              <input
                type="file"
                accept=".csv"
                onChange={handlePortfolioUpload}
                className="block w-full text-sm text-zinc-500
                  file:mr-4 file:py-2 file:px-4
                  file:rounded-lg file:border-0
                  file:text-sm file:font-medium
                  file:bg-emerald-50 file:text-emerald-700
                  hover:file:bg-emerald-100
                  file:cursor-pointer cursor-pointer"
              />
              <p className="mt-1.5 text-xs text-zinc-500">
                CSV: Ticker, Industry, % Allocation
              </p>
            </div>

            {/* Account Selector */}
            <div>
              <label className="label">3. Select Account</label>
              <select
                value={selectedAccount?.id || ''}
                onChange={(e) => {
                  const account = accounts.find(a => a.id === parseInt(e.target.value));
                  setSelectedAccount(account || null);
                }}
                className="select"
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
              <label className="label">4. Total Amount to Allocate</label>
              <div className="relative">
                <span className="absolute left-3 top-1/2 -translate-y-1/2 text-zinc-500">$</span>
                <input
                  type="text"
                  value={totalAmount ? totalAmount.toLocaleString() : ''}
                  onChange={(e) => handleTotalAmountChange(e.target.value)}
                  placeholder="1,000,000"
                  className="input pl-8"
                />
              </div>
            </div>
          </div>
        </div>

        {/* Summary Cards */}
        {totalAmount > 0 && (
          <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
            <div className="card bg-blue-50 border-blue-200">
              <div className="metric-card metric-card-blue">
                <div className="metric-label">Total to Allocate</div>
                <div className="metric-value-lg text-blue-900">{formatCurrency(totalAmount)}</div>
              </div>
            </div>
            <div className="card bg-emerald-50 border-emerald-200">
              <div className="metric-card metric-card-green">
                <div className="metric-label">Allocated</div>
                <div className="metric-value-lg text-emerald-900">{formatCurrency(totalAllocated)}</div>
              </div>
            </div>
            <div className="card bg-amber-50 border-amber-200">
              <div className="metric-card metric-card-orange">
                <div className="metric-label">Remaining</div>
                <div className="metric-value-lg text-amber-900">{formatCurrency(remainingToAllocate)}</div>
              </div>
            </div>
            <div className="card bg-violet-50 border-violet-200">
              <div className="metric-card metric-card-purple">
                <div className="metric-label">% Allocated</div>
                <div className="metric-value-lg text-violet-900">
                  {totalAmount > 0 ? ((totalAllocated / totalAmount) * 100).toFixed(1) : 0}%
                </div>
              </div>
            </div>
          </div>
        )}

        {/* Industry Allocation Table */}
        {industries.length > 0 && (
          <div className="card p-0 overflow-hidden">
            <div className="px-6 py-4 border-b border-zinc-100">
              <h2 className="text-base font-semibold text-zinc-900">Industry Allocation</h2>
            </div>
            <div className="table-container mx-0">
              <table className="table">
                <thead>
                  <tr>
                    <th className="text-center w-16" title="Include in allocation">Incl.</th>
                    <th>Industry</th>
                    <th className="text-right">S&P Weight</th>
                    <th className="text-right">CCM Weight</th>
                    <th className="text-center">Adjust (bps)</th>
                    <th className="text-right">Pro-Forma</th>
                    <th className="text-right">Active</th>
                    <th className="text-right">$ Allocation</th>
                    <th className="text-right">Remaining</th>
                    <th className="text-center">Actions</th>
                  </tr>
                </thead>
                <tbody className="tabular-nums">
                  {industries.map((industry) => {
                    const industryAllocations = getIndustryAllocations(industry.industry);
                    const industryRemaining = getIndustryRemaining(industry);
                    const isExpanded = expandedIndustry === industry.industry;

                    return (
                      <Fragment key={industry.industry}>
                        <tr className={`transition-colors ${industry.excluded ? 'bg-zinc-100/50 opacity-60' : ''}`}>
                          <td className="text-center">
                            <input
                              type="checkbox"
                              checked={!industry.excluded}
                              onChange={() => handleExclusionToggle(industry.industry)}
                              className="h-4 w-4 text-blue-600 border-zinc-300 rounded focus:ring-blue-500"
                              title={industry.excluded ? 'Include in allocation' : 'Exclude from allocation'}
                            />
                          </td>
                          <td>
                            <button
                              onClick={() => setExpandedIndustry(isExpanded ? null : industry.industry)}
                              className="flex items-center gap-2 font-medium text-zinc-900"
                            >
                              <svg className={`h-4 w-4 text-zinc-400 transition-transform ${isExpanded ? 'rotate-90' : ''}`} fill="none" viewBox="0 0 24 24" stroke="currentColor">
                                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M9 5l7 7-7 7" />
                              </svg>
                              {industry.industry}
                            </button>
                          </td>
                          <td className="text-right text-zinc-600">
                            {formatPct(industry.sp500_weight)}
                          </td>
                          <td className="text-right text-zinc-600">
                            {formatPct(industry.ccm_weight)}
                          </td>
                          <td className="text-center">
                            <input
                              type="number"
                              value={industry.adjustment_bps}
                              onChange={(e) => handleAdjustmentChange(industry.industry, parseInt(e.target.value) || 0)}
                              disabled={industry.excluded}
                              className={`w-20 px-2 py-1 text-sm text-center border border-zinc-300 rounded-lg disabled:bg-zinc-100 disabled:cursor-not-allowed`}
                            />
                          </td>
                          <td className="text-right font-medium text-zinc-900">
                            {formatPct(industry.proforma_weight)}
                          </td>
                          <td className={`text-right font-medium ${
                            industry.active_weight > 0 ? 'value-positive' :
                            industry.active_weight < 0 ? 'value-negative' : 'text-zinc-600'
                          }`}>
                            {industry.active_weight > 0 ? '+' : ''}{formatPct(industry.active_weight)}
                          </td>
                          <td className="text-right font-medium text-zinc-900">
                            {formatCurrency(industry.dollar_allocation)}
                          </td>
                          <td className={`text-right font-medium ${
                            industryRemaining < 1 ? 'value-positive' : 'text-amber-600'
                          }`}>
                            {formatCurrency(industryRemaining)}
                          </td>
                          <td className="text-center">
                            <button
                              onClick={() => addTickerToIndustry(industry.industry)}
                              disabled={industry.excluded}
                              className="btn btn-ghost btn-xs text-blue-600 disabled:text-zinc-400 disabled:cursor-not-allowed"
                            >
                              + Add Ticker
                            </button>
                          </td>
                        </tr>

                        {/* Expanded ticker allocations */}
                        {isExpanded && (
                          <tr>
                            <td colSpan={10} className="p-0 bg-zinc-50/50">
                              <div className="px-6 py-4 pl-12">
                                {industryAllocations.length === 0 ? (
                                  <p className="text-sm text-zinc-500 italic">
                                    No tickers allocated. Click "Add Ticker" to begin.
                                  </p>
                                ) : (
                                  <table className="w-full text-sm">
                                    <thead>
                                      <tr className="text-xs text-zinc-500 uppercase tracking-wider">
                                        <th className="px-2 py-2 text-left font-medium">Ticker</th>
                                        <th className="px-2 py-2 text-left font-medium">Name</th>
                                        <th className="px-2 py-2 text-right font-medium">% of Industry</th>
                                        <th className="px-2 py-2 text-right font-medium">$ Amount</th>
                                        <th className="px-2 py-2 text-right font-medium">Price</th>
                                        <th className="px-2 py-2 text-right font-medium">Shares</th>
                                        <th className="px-2 py-2"></th>
                                      </tr>
                                    </thead>
                                    <tbody className="tabular-nums">
                                      {industryAllocations.map((alloc) => (
                                        <tr key={alloc.id} className="border-t border-zinc-100">
                                          <td className="px-2 py-2">
                                            <input
                                              type="text"
                                              value={alloc.ticker}
                                              onChange={(e) => updateTickerAllocation(alloc.id, 'ticker', e.target.value.toUpperCase())}
                                              onBlur={(e) => updateTickerAllocation(alloc.id, 'ticker', e.target.value.toUpperCase())}
                                              placeholder="AAPL"
                                              className="w-20 px-2 py-1 border border-zinc-300 rounded text-sm"
                                            />
                                          </td>
                                          <td className="px-2 py-2 text-zinc-600">
                                            {alloc.security_name || '-'}
                                          </td>
                                          <td className="px-2 py-2 text-right">
                                            <div className="flex items-center justify-end">
                                              <input
                                                type="number"
                                                value={alloc.pct_of_industry || ''}
                                                onChange={(e) => updateTickerAllocation(alloc.id, 'pct_of_industry', e.target.value)}
                                                placeholder="100"
                                                min="0"
                                                max="100"
                                                className="w-20 px-2 py-1 border border-zinc-300 rounded text-sm text-right"
                                              />
                                              <span className="ml-1">%</span>
                                            </div>
                                          </td>
                                          <td className="px-2 py-2 text-right font-medium">
                                            {formatCurrency(alloc.dollar_amount)}
                                          </td>
                                          <td className="px-2 py-2 text-right text-zinc-600">
                                            {alloc.price > 0 ? `$${alloc.price.toFixed(2)}` : '-'}
                                          </td>
                                          <td className="px-2 py-2 text-right font-medium text-blue-600">
                                            {alloc.shares > 0 ? alloc.shares.toLocaleString() : '-'}
                                          </td>
                                          <td className="px-2 py-2 text-center">
                                            <button
                                              onClick={() => removeTickerAllocation(alloc.id)}
                                              className="text-red-500 hover:text-red-700 font-medium"
                                            >
                                              Remove
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
                      </Fragment>
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
                <h3 className="font-semibold text-zinc-900">Ready to Execute</h3>
                <p className="text-sm text-zinc-500">
                  {tickerAllocations.filter(a => a.ticker && a.shares > 0).length} tickers with{' '}
                  <span className="font-medium tabular-nums">
                    {tickerAllocations.filter(a => a.shares > 0).reduce((sum, a) => sum + a.shares, 0).toLocaleString()}
                  </span> total shares
                </p>
              </div>
              <button
                onClick={handleExecute}
                disabled={loading || !selectedAccount}
                className="btn btn-primary"
              >
                {loading ? (
                  <>
                    <svg className="animate-spin h-4 w-4" viewBox="0 0 24 24" fill="none">
                      <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                      <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
                    </svg>
                    Generating...
                  </>
                ) : 'Download Schwab CSV'}
              </button>
            </div>
          </div>
        )}

        {/* Help Section */}
        <div className="card bg-zinc-50 border-zinc-200">
          <div className="card-header">
            <h3 className="card-title">Instructions</h3>
          </div>
          <ol className="list-decimal list-inside text-sm text-zinc-600 space-y-1.5">
            <li>Upload a CSV file with S&P 500 industry weights (columns: Industry, Weight)</li>
            <li><strong>Optional:</strong> Upload a portfolio CSV to auto-populate tickers (columns: Ticker, Industry, % Allocation)</li>
            <li>Select the account to allocate new funds to</li>
            <li>Enter the total dollar amount to allocate</li>
            <li><strong>Optional:</strong> Uncheck industries to exclude them (zero allocation) - remaining weights auto-normalize</li>
            <li>Optionally adjust CCM weights using basis point adjustments</li>
            <li>Click on an industry row to expand and add/edit tickers manually</li>
            <li>Click "Download Schwab CSV" to generate a Schwab-compatible CSV for bulk upload</li>
          </ol>
          <div className="mt-5 grid grid-cols-1 md:grid-cols-3 gap-4">
            <div className="p-3 bg-blue-50 rounded-lg border border-blue-200">
              <p className="text-sm text-blue-800">
                <strong>Industry Weights CSV:</strong><br />
                <code className="text-xs">Industry, Weight</code><br />
                <code className="text-xs">Information Technology, 28.5</code><br />
                <code className="text-xs">Health Care, 13.5</code>
              </p>
            </div>
            <div className="p-3 bg-emerald-50 rounded-lg border border-emerald-200">
              <p className="text-sm text-emerald-800">
                <strong>Portfolio CSV:</strong><br />
                <code className="text-xs">Ticker, Industry, Allocation</code><br />
                <code className="text-xs">AAPL, Information Technology, 50</code><br />
                <code className="text-xs">MSFT, Information Technology, 50</code>
              </p>
            </div>
            <div className="p-3 bg-violet-50 rounded-lg border border-violet-200">
              <p className="text-sm text-violet-800">
                <strong>Schwab Output CSV:</strong><br />
                <code className="text-xs">Account Number, B (Buy), Shares, Ticker, M (Market)</code>
              </p>
            </div>
          </div>
        </div>
      </div>
    </Layout>
  );
}
