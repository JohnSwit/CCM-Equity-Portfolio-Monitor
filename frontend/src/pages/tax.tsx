import { useState, useEffect } from 'react';
import { useRouter } from 'next/router';
import Layout from '../components/Layout';
import { useAuth } from '../contexts/AuthContext';
import { api } from '../lib/api';

interface TaxSummary {
  tax_year: number;
  short_term_realized_gains: number;
  short_term_realized_losses: number;
  net_short_term: number;
  long_term_realized_gains: number;
  long_term_realized_losses: number;
  net_long_term: number;
  total_realized: number;
  wash_sale_disallowed: number;
  short_term_unrealized_gains: number;
  short_term_unrealized_losses: number;
  net_short_term_unrealized: number;
  long_term_unrealized_gains: number;
  long_term_unrealized_losses: number;
  net_long_term_unrealized: number;
  total_unrealized: number;
  estimated_tax_liability: number;
}

interface TaxLot {
  id: number;
  account_id: number;
  account_number: string | null;
  security_id: number;
  symbol: string;
  purchase_date: string;
  original_shares: number;
  remaining_shares: number;
  cost_basis_per_share: number;
  remaining_cost_basis: number;
  current_price: number | null;
  current_value: number | null;
  unrealized_gain_loss: number | null;
  unrealized_gain_loss_pct: number | null;
  holding_period_days: number;
  is_short_term: boolean;
}

interface HarvestCandidate {
  symbol: string;
  security_id: number;
  total_shares: number;
  total_cost_basis: number;
  current_value: number;
  unrealized_loss: number;
  unrealized_loss_pct: number;
  short_term_loss: number;
  long_term_loss: number;
  has_recent_purchase: boolean;
  has_pending_wash_sale: boolean;
  wash_sale_window_end: string | null;
}

interface RealizedGain {
  id: number;
  symbol: string;
  sale_date: string;
  purchase_date: string;
  shares_sold: number;
  proceeds: number;
  cost_basis: number;
  gain_loss: number;
  is_short_term: boolean;
  is_wash_sale: boolean;
  wash_sale_disallowed: number;
  adjusted_gain_loss: number;
}

interface TradeImpact {
  symbol: string;
  shares: number;
  estimated_proceeds: number;
  fifo_impact: { total_gain_loss: number; estimated_tax: number };
  lifo_impact: { total_gain_loss: number; estimated_tax: number };
  hifo_impact: { total_gain_loss: number; estimated_tax: number };
  lofo_impact: { total_gain_loss: number; estimated_tax: number };
  recommended_method: string;
  tax_savings_vs_fifo: number;
}

interface Account {
  id: number;
  account_number: string;
  name: string | null;
  lot_count: number;
}

type TabType = 'summary' | 'lots' | 'harvest' | 'realized' | 'simulator' | 'import';

interface TaxLotImport {
  id: number;
  file_name: string;
  status: string;
  rows_processed: number;
  rows_imported: number;
  rows_skipped: number;
  rows_error: number;
  created_at: string;
}

interface ImportPreview {
  status: string;
  file_name: string;
  total_rows: number;
  valid_rows: number;
  error_rows: number;
  errors: Array<{ row: number; error: string }>;
  warnings: Array<{ row: number; warning: string }>;
  preview_data: Array<{
    row_num: number;
    account_number: string;
    account_name: string;
    symbol: string;
    asset_name: string;
    open_date: string;
    unit_cost: number;
    units: number;
    cost_basis: number;
    market_value: number | null;
    short_term_gain_loss: number | null;
    long_term_gain_loss: number | null;
    total_gain_loss: number | null;
  }>;
}

export default function TaxPage() {
  const router = useRouter();
  const { user, loading: authLoading } = useAuth();
  const [activeTab, setActiveTab] = useState<TabType>('summary');
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Data
  const [accounts, setAccounts] = useState<Account[]>([]);
  const [selectedAccount, setSelectedAccount] = useState<number | null>(null);
  const [taxYear, setTaxYear] = useState<number>(new Date().getFullYear());
  const [summary, setSummary] = useState<TaxSummary | null>(null);
  const [lots, setLots] = useState<TaxLot[]>([]);
  const [harvestCandidates, setHarvestCandidates] = useState<HarvestCandidate[]>([]);
  const [realizedGains, setRealizedGains] = useState<RealizedGain[]>([]);

  // Trade simulator
  const [simSymbol, setSimSymbol] = useState('');
  const [simShares, setSimShares] = useState<number>(0);
  const [simAccountId, setSimAccountId] = useState<number | null>(null);
  const [tradeImpact, setTradeImpact] = useState<TradeImpact | null>(null);
  const [simulating, setSimulating] = useState(false);

  // Build lots
  const [building, setBuilding] = useState(false);

  // Import state
  const [importFile, setImportFile] = useState<File | null>(null);
  const [importPreview, setImportPreview] = useState<ImportPreview | null>(null);
  const [importHistory, setImportHistory] = useState<TaxLotImport[]>([]);
  const [importing, setImporting] = useState(false);
  const [previewLoading, setPreviewLoading] = useState(false);

  useEffect(() => {
    if (!authLoading && !user) {
      router.push('/login');
    }
  }, [authLoading, user, router]);

  useEffect(() => {
    if (user) {
      loadInitialData();
    }
  }, [user]);

  useEffect(() => {
    if (user && activeTab === 'summary') {
      loadSummary();
    } else if (user && activeTab === 'lots') {
      loadLots();
    } else if (user && activeTab === 'harvest') {
      loadHarvestCandidates();
    } else if (user && activeTab === 'realized') {
      loadRealizedGains();
    } else if (user && activeTab === 'import') {
      loadImportHistory();
    }
  }, [activeTab, selectedAccount, taxYear]);

  const loadInitialData = async () => {
    try {
      setLoading(true);
      const accountsData = await api.getTaxAccounts();
      setAccounts(accountsData);
      await loadSummary();
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to load data');
    } finally {
      setLoading(false);
    }
  };

  const loadSummary = async () => {
    try {
      const data = await api.getTaxSummary({
        account_id: selectedAccount || undefined,
        tax_year: taxYear,
      });
      setSummary(data);
    } catch (err: any) {
      console.error('Failed to load summary:', err);
    }
  };

  const loadLots = async () => {
    try {
      const data = await api.getTaxLots({
        account_id: selectedAccount || undefined,
        include_closed: false,
      });
      setLots(data.lots || []);
    } catch (err: any) {
      console.error('Failed to load lots:', err);
    }
  };

  const loadHarvestCandidates = async () => {
    try {
      const data = await api.getHarvestCandidates({
        account_id: selectedAccount || undefined,
        min_loss: 100,
      });
      setHarvestCandidates(data.candidates || []);
    } catch (err: any) {
      console.error('Failed to load harvest candidates:', err);
    }
  };

  const loadRealizedGains = async () => {
    try {
      const data = await api.getRealizedGains({
        account_id: selectedAccount || undefined,
        tax_year: taxYear,
      });
      setRealizedGains(data.gains || []);
    } catch (err: any) {
      console.error('Failed to load realized gains:', err);
    }
  };

  const handleBuildLots = async () => {
    try {
      setBuilding(true);
      setError(null);
      const result = await api.buildTaxLots(selectedAccount || undefined);
      alert(`Built ${result.total_lots_created} tax lots across ${result.accounts_processed} accounts`);
      await loadInitialData();
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to build tax lots');
    } finally {
      setBuilding(false);
    }
  };

  const handleSimulateTrade = async () => {
    if (!simSymbol || !simShares || !simAccountId) {
      setError('Please select account, enter symbol, and shares');
      return;
    }

    try {
      setSimulating(true);
      setError(null);
      const data = await api.getTradeImpact(simAccountId, simSymbol, simShares);
      setTradeImpact(data);
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to simulate trade');
    } finally {
      setSimulating(false);
    }
  };

  const loadImportHistory = async () => {
    try {
      const data = await api.getTaxLotImports(50);
      setImportHistory(data);
    } catch (err: any) {
      console.error('Failed to load import history:', err);
    }
  };

  const handleFileSelect = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (file) {
      setImportFile(file);
      setImportPreview(null);
      setError(null);
    }
  };

  const handlePreviewImport = async () => {
    if (!importFile) {
      setError('Please select a file first');
      return;
    }

    try {
      setPreviewLoading(true);
      setError(null);
      const data = await api.importTaxLots(importFile, 'preview');
      setImportPreview(data);
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to preview file');
    } finally {
      setPreviewLoading(false);
    }
  };

  const handleCommitImport = async () => {
    if (!importFile) {
      setError('Please select a file first');
      return;
    }

    try {
      setImporting(true);
      setError(null);
      const result = await api.importTaxLots(importFile, 'commit');
      alert(`Successfully imported ${result.imported} tax lots`);
      setImportFile(null);
      setImportPreview(null);
      await loadImportHistory();
      // Clear file input
      const fileInput = document.getElementById('tax-lot-file-input') as HTMLInputElement;
      if (fileInput) fileInput.value = '';
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to import file');
    } finally {
      setImporting(false);
    }
  };

  const handleDeleteImport = async (importId: number, fileName: string) => {
    if (!confirm(`Delete import "${fileName}" and all associated tax lots?`)) {
      return;
    }

    try {
      await api.deleteTaxLotImport(importId);
      await loadImportHistory();
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to delete import');
    }
  };

  const formatCurrency = (value: number | null | undefined) => {
    if (value == null) return '-';
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD',
      minimumFractionDigits: 0,
      maximumFractionDigits: 0,
    }).format(value);
  };

  const formatPercent = (value: number | null | undefined) => {
    if (value == null) return '-';
    return `${value >= 0 ? '+' : ''}${value.toFixed(2)}%`;
  };

  const formatDate = (dateStr: string) => {
    return new Date(dateStr).toLocaleDateString();
  };

  if (authLoading || loading) {
    return (
      <Layout>
        <div className="flex items-center justify-center h-64">
          <div className="text-gray-500">Loading...</div>
        </div>
      </Layout>
    );
  }

  return (
    <Layout>
      <div className="space-y-6">
        {/* Header */}
        <div className="flex justify-between items-center">
          <div>
            <h1 className="text-2xl font-bold text-gray-900">Tax Optimization</h1>
            <p className="text-sm text-gray-500 mt-1">
              Manage tax lots, harvest losses, and optimize trades
            </p>
          </div>
          <div className="flex gap-4 items-center">
            <div>
              <label className="block text-xs text-gray-500 mb-1">Tax Year</label>
              <select
                value={taxYear}
                onChange={(e) => setTaxYear(Number(e.target.value))}
                className="px-3 py-2 border rounded"
              >
                {[2024, 2025, 2026].map((y) => (
                  <option key={y} value={y}>{y}</option>
                ))}
              </select>
            </div>
            <div>
              <label className="block text-xs text-gray-500 mb-1">Account</label>
              <select
                value={selectedAccount || ''}
                onChange={(e) => setSelectedAccount(e.target.value ? Number(e.target.value) : null)}
                className="px-3 py-2 border rounded"
              >
                <option value="">All Accounts</option>
                {accounts.map((a) => (
                  <option key={a.id} value={a.id}>{a.account_number}</option>
                ))}
              </select>
            </div>
            <div className="flex items-end">
              <button
                onClick={handleBuildLots}
                disabled={building}
                className="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50"
              >
                {building ? 'Building...' : 'Rebuild Tax Lots'}
              </button>
            </div>
          </div>
        </div>

        {error && (
          <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded">
            {error}
            <button onClick={() => setError(null)} className="ml-4 text-red-500 hover:text-red-700">
              Dismiss
            </button>
          </div>
        )}

        {/* Tabs */}
        <div className="border-b">
          <nav className="flex space-x-8">
            {[
              { id: 'summary', label: 'Tax Summary' },
              { id: 'lots', label: 'Tax Lots' },
              { id: 'harvest', label: 'Loss Harvesting' },
              { id: 'realized', label: 'Realized Gains' },
              { id: 'simulator', label: 'Trade Simulator' },
              { id: 'import', label: 'Import Tax Lots' },
            ].map((tab) => (
              <button
                key={tab.id}
                onClick={() => setActiveTab(tab.id as TabType)}
                className={`py-4 px-1 border-b-2 font-medium text-sm ${
                  activeTab === tab.id
                    ? 'border-blue-500 text-blue-600'
                    : 'border-transparent text-gray-500 hover:text-gray-700'
                }`}
              >
                {tab.label}
              </button>
            ))}
          </nav>
        </div>

        {/* Tab Content */}
        {activeTab === 'summary' && summary && (
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {/* Realized Gains/Losses */}
            <div className="card p-6">
              <h3 className="text-lg font-semibold mb-4">Realized Gains/Losses ({taxYear})</h3>
              <div className="space-y-4">
                <div className="grid grid-cols-3 gap-4">
                  <div></div>
                  <div className="text-center text-sm text-gray-500">Short-Term</div>
                  <div className="text-center text-sm text-gray-500">Long-Term</div>
                </div>
                <div className="grid grid-cols-3 gap-4">
                  <div className="text-sm text-gray-600">Gains</div>
                  <div className="text-center text-green-600">{formatCurrency(summary.short_term_realized_gains)}</div>
                  <div className="text-center text-green-600">{formatCurrency(summary.long_term_realized_gains)}</div>
                </div>
                <div className="grid grid-cols-3 gap-4">
                  <div className="text-sm text-gray-600">Losses</div>
                  <div className="text-center text-red-600">({formatCurrency(summary.short_term_realized_losses)})</div>
                  <div className="text-center text-red-600">({formatCurrency(summary.long_term_realized_losses)})</div>
                </div>
                <div className="grid grid-cols-3 gap-4 border-t pt-2">
                  <div className="text-sm font-medium">Net</div>
                  <div className={`text-center font-medium ${summary.net_short_term >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                    {formatCurrency(summary.net_short_term)}
                  </div>
                  <div className={`text-center font-medium ${summary.net_long_term >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                    {formatCurrency(summary.net_long_term)}
                  </div>
                </div>
                {summary.wash_sale_disallowed > 0 && (
                  <div className="text-sm text-orange-600 mt-2">
                    Wash Sale Disallowed: {formatCurrency(summary.wash_sale_disallowed)}
                  </div>
                )}
              </div>
            </div>

            {/* Unrealized Gains/Losses */}
            <div className="card p-6">
              <h3 className="text-lg font-semibold mb-4">Unrealized Gains/Losses</h3>
              <div className="space-y-4">
                <div className="grid grid-cols-3 gap-4">
                  <div></div>
                  <div className="text-center text-sm text-gray-500">Short-Term</div>
                  <div className="text-center text-sm text-gray-500">Long-Term</div>
                </div>
                <div className="grid grid-cols-3 gap-4">
                  <div className="text-sm text-gray-600">Gains</div>
                  <div className="text-center text-green-600">{formatCurrency(summary.short_term_unrealized_gains)}</div>
                  <div className="text-center text-green-600">{formatCurrency(summary.long_term_unrealized_gains)}</div>
                </div>
                <div className="grid grid-cols-3 gap-4">
                  <div className="text-sm text-gray-600">Losses</div>
                  <div className="text-center text-red-600">({formatCurrency(summary.short_term_unrealized_losses)})</div>
                  <div className="text-center text-red-600">({formatCurrency(summary.long_term_unrealized_losses)})</div>
                </div>
                <div className="grid grid-cols-3 gap-4 border-t pt-2">
                  <div className="text-sm font-medium">Net</div>
                  <div className={`text-center font-medium ${summary.net_short_term_unrealized >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                    {formatCurrency(summary.net_short_term_unrealized)}
                  </div>
                  <div className={`text-center font-medium ${summary.net_long_term_unrealized >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                    {formatCurrency(summary.net_long_term_unrealized)}
                  </div>
                </div>
              </div>
            </div>

            {/* Tax Estimate */}
            <div className="card p-6 lg:col-span-2">
              <h3 className="text-lg font-semibold mb-4">Estimated Tax Impact</h3>
              <div className="grid grid-cols-4 gap-6">
                <div className="text-center">
                  <div className="text-sm text-gray-500">Total Realized</div>
                  <div className={`text-2xl font-bold ${summary.total_realized >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                    {formatCurrency(summary.total_realized)}
                  </div>
                </div>
                <div className="text-center">
                  <div className="text-sm text-gray-500">Total Unrealized</div>
                  <div className={`text-2xl font-bold ${summary.total_unrealized >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                    {formatCurrency(summary.total_unrealized)}
                  </div>
                </div>
                <div className="text-center">
                  <div className="text-sm text-gray-500">Est. Tax Liability</div>
                  <div className="text-2xl font-bold text-orange-600">
                    {formatCurrency(summary.estimated_tax_liability)}
                  </div>
                </div>
                <div className="text-center">
                  <div className="text-sm text-gray-500">Rates Used</div>
                  <div className="text-sm text-gray-700">
                    ST: 37% | LT: 20%
                  </div>
                </div>
              </div>
            </div>
          </div>
        )}

        {activeTab === 'lots' && (
          <div className="card overflow-hidden">
            <div className="overflow-x-auto">
              <table className="min-w-full divide-y divide-gray-200">
                <thead className="bg-gray-50">
                  <tr>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Symbol</th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Account</th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Purchase Date</th>
                    <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Shares</th>
                    <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Cost Basis</th>
                    <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Current Value</th>
                    <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Gain/Loss</th>
                    <th className="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase">Term</th>
                    <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Days Held</th>
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-200">
                  {lots.map((lot) => (
                    <tr key={lot.id} className="hover:bg-gray-50">
                      <td className="px-4 py-3 font-medium">{lot.symbol}</td>
                      <td className="px-4 py-3 text-gray-600">{lot.account_number || '-'}</td>
                      <td className="px-4 py-3">{formatDate(lot.purchase_date)}</td>
                      <td className="px-4 py-3 text-right">{lot.remaining_shares.toFixed(2)}</td>
                      <td className="px-4 py-3 text-right">{formatCurrency(lot.remaining_cost_basis)}</td>
                      <td className="px-4 py-3 text-right">{formatCurrency(lot.current_value)}</td>
                      <td className={`px-4 py-3 text-right ${(lot.unrealized_gain_loss || 0) >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                        {formatCurrency(lot.unrealized_gain_loss)} ({formatPercent(lot.unrealized_gain_loss_pct)})
                      </td>
                      <td className="px-4 py-3 text-center">
                        <span className={`px-2 py-1 rounded text-xs ${lot.is_short_term ? 'bg-yellow-100 text-yellow-800' : 'bg-green-100 text-green-800'}`}>
                          {lot.is_short_term ? 'Short' : 'Long'}
                        </span>
                      </td>
                      <td className="px-4 py-3 text-right">{lot.holding_period_days}</td>
                    </tr>
                  ))}
                  {lots.length === 0 && (
                    <tr>
                      <td colSpan={9} className="px-4 py-8 text-center text-gray-500">
                        No tax lots found. Click "Rebuild Tax Lots" to generate from transactions.
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {activeTab === 'harvest' && (
          <div className="space-y-6">
            <div className="card p-4 bg-blue-50 border-blue-200">
              <h3 className="font-semibold text-blue-900">Tax-Loss Harvesting</h3>
              <p className="text-sm text-blue-700 mt-1">
                Positions below have unrealized losses that could be harvested to offset gains.
                Watch for wash sale restrictions (30-day window).
              </p>
            </div>

            <div className="card overflow-hidden">
              <div className="overflow-x-auto">
                <table className="min-w-full divide-y divide-gray-200">
                  <thead className="bg-gray-50">
                    <tr>
                      <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Symbol</th>
                      <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Shares</th>
                      <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Cost Basis</th>
                      <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Current Value</th>
                      <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Unrealized Loss</th>
                      <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Short-Term</th>
                      <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Long-Term</th>
                      <th className="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase">Wash Sale Risk</th>
                    </tr>
                  </thead>
                  <tbody className="bg-white divide-y divide-gray-200">
                    {harvestCandidates.map((c) => (
                      <tr key={c.security_id} className="hover:bg-gray-50">
                        <td className="px-4 py-3 font-medium">{c.symbol}</td>
                        <td className="px-4 py-3 text-right">{c.total_shares.toFixed(2)}</td>
                        <td className="px-4 py-3 text-right">{formatCurrency(c.total_cost_basis)}</td>
                        <td className="px-4 py-3 text-right">{formatCurrency(c.current_value)}</td>
                        <td className="px-4 py-3 text-right text-red-600">
                          {formatCurrency(c.unrealized_loss)} ({formatPercent(c.unrealized_loss_pct)})
                        </td>
                        <td className="px-4 py-3 text-right text-red-600">{formatCurrency(c.short_term_loss)}</td>
                        <td className="px-4 py-3 text-right text-red-600">{formatCurrency(c.long_term_loss)}</td>
                        <td className="px-4 py-3 text-center">
                          {c.has_recent_purchase || c.has_pending_wash_sale ? (
                            <span className="px-2 py-1 rounded text-xs bg-orange-100 text-orange-800">
                              {c.wash_sale_window_end ? `Until ${formatDate(c.wash_sale_window_end)}` : 'At Risk'}
                            </span>
                          ) : (
                            <span className="px-2 py-1 rounded text-xs bg-green-100 text-green-800">Clear</span>
                          )}
                        </td>
                      </tr>
                    ))}
                    {harvestCandidates.length === 0 && (
                      <tr>
                        <td colSpan={8} className="px-4 py-8 text-center text-gray-500">
                          No tax-loss harvesting opportunities found.
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        )}

        {activeTab === 'realized' && (
          <div className="card overflow-hidden">
            <div className="overflow-x-auto">
              <table className="min-w-full divide-y divide-gray-200">
                <thead className="bg-gray-50">
                  <tr>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Symbol</th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Sale Date</th>
                    <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Purchase Date</th>
                    <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Shares</th>
                    <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Proceeds</th>
                    <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Cost Basis</th>
                    <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Gain/Loss</th>
                    <th className="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase">Term</th>
                    <th className="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase">Wash Sale</th>
                  </tr>
                </thead>
                <tbody className="bg-white divide-y divide-gray-200">
                  {realizedGains.map((g) => (
                    <tr key={g.id} className="hover:bg-gray-50">
                      <td className="px-4 py-3 font-medium">{g.symbol}</td>
                      <td className="px-4 py-3">{formatDate(g.sale_date)}</td>
                      <td className="px-4 py-3">{formatDate(g.purchase_date)}</td>
                      <td className="px-4 py-3 text-right">{g.shares_sold.toFixed(2)}</td>
                      <td className="px-4 py-3 text-right">{formatCurrency(g.proceeds)}</td>
                      <td className="px-4 py-3 text-right">{formatCurrency(g.cost_basis)}</td>
                      <td className={`px-4 py-3 text-right ${g.adjusted_gain_loss >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                        {formatCurrency(g.adjusted_gain_loss)}
                      </td>
                      <td className="px-4 py-3 text-center">
                        <span className={`px-2 py-1 rounded text-xs ${g.is_short_term ? 'bg-yellow-100 text-yellow-800' : 'bg-green-100 text-green-800'}`}>
                          {g.is_short_term ? 'Short' : 'Long'}
                        </span>
                      </td>
                      <td className="px-4 py-3 text-center">
                        {g.is_wash_sale && (
                          <span className="px-2 py-1 rounded text-xs bg-red-100 text-red-800">
                            {formatCurrency(g.wash_sale_disallowed)}
                          </span>
                        )}
                      </td>
                    </tr>
                  ))}
                  {realizedGains.length === 0 && (
                    <tr>
                      <td colSpan={9} className="px-4 py-8 text-center text-gray-500">
                        No realized gains/losses for {taxYear}.
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {activeTab === 'simulator' && (
          <div className="space-y-6">
            <div className="card p-6">
              <h3 className="text-lg font-semibold mb-4">Trade Impact Simulator</h3>
              <p className="text-sm text-gray-600 mb-4">
                Analyze the tax impact of selling shares using different lot selection methods.
              </p>
              <div className="grid grid-cols-1 md:grid-cols-4 gap-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Account</label>
                  <select
                    value={simAccountId || ''}
                    onChange={(e) => setSimAccountId(e.target.value ? Number(e.target.value) : null)}
                    className="w-full px-3 py-2 border rounded"
                  >
                    <option value="">Select Account</option>
                    {accounts.map((a) => (
                      <option key={a.id} value={a.id}>{a.account_number}</option>
                    ))}
                  </select>
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Symbol</label>
                  <input
                    type="text"
                    value={simSymbol}
                    onChange={(e) => setSimSymbol(e.target.value.toUpperCase())}
                    placeholder="AAPL"
                    className="w-full px-3 py-2 border rounded"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Shares to Sell</label>
                  <input
                    type="number"
                    value={simShares || ''}
                    onChange={(e) => setSimShares(Number(e.target.value))}
                    placeholder="100"
                    className="w-full px-3 py-2 border rounded"
                  />
                </div>
                <div className="flex items-end">
                  <button
                    onClick={handleSimulateTrade}
                    disabled={simulating || !simSymbol || !simShares || !simAccountId}
                    className="w-full px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50"
                  >
                    {simulating ? 'Analyzing...' : 'Analyze Impact'}
                  </button>
                </div>
              </div>
            </div>

            {tradeImpact && (
              <div className="card p-6">
                <h3 className="text-lg font-semibold mb-4">
                  Results: Sell {tradeImpact.shares} shares of {tradeImpact.symbol}
                </h3>
                <p className="text-sm text-gray-600 mb-4">
                  Estimated Proceeds: {formatCurrency(tradeImpact.estimated_proceeds)}
                </p>

                <div className="grid grid-cols-2 lg:grid-cols-4 gap-4 mb-6">
                  {[
                    { method: 'FIFO', data: tradeImpact.fifo_impact, label: 'First In, First Out' },
                    { method: 'LIFO', data: tradeImpact.lifo_impact, label: 'Last In, First Out' },
                    { method: 'HIFO', data: tradeImpact.hifo_impact, label: 'Highest In, First Out' },
                    { method: 'LOFO', data: tradeImpact.lofo_impact, label: 'Lowest In, First Out' },
                  ].map(({ method, data, label }) => (
                    <div
                      key={method}
                      className={`p-4 rounded border ${
                        tradeImpact.recommended_method.toUpperCase() === method
                          ? 'border-green-500 bg-green-50'
                          : 'border-gray-200'
                      }`}
                    >
                      <div className="flex justify-between items-center mb-2">
                        <span className="font-semibold">{method}</span>
                        {tradeImpact.recommended_method.toUpperCase() === method && (
                          <span className="text-xs bg-green-500 text-white px-2 py-1 rounded">Best</span>
                        )}
                      </div>
                      <div className="text-xs text-gray-500 mb-2">{label}</div>
                      <div className={`text-lg font-bold ${data.total_gain_loss >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                        {formatCurrency(data.total_gain_loss)}
                      </div>
                      <div className="text-sm text-gray-600">
                        Est. Tax: {formatCurrency(data.estimated_tax)}
                      </div>
                    </div>
                  ))}
                </div>

                <div className="bg-blue-50 p-4 rounded">
                  <h4 className="font-semibold text-blue-900">Recommendation</h4>
                  <p className="text-sm text-blue-700 mt-1">
                    Use <strong>{tradeImpact.recommended_method.toUpperCase()}</strong> method to save{' '}
                    <strong>{formatCurrency(tradeImpact.tax_savings_vs_fifo)}</strong> in taxes vs FIFO.
                  </p>
                </div>
              </div>
            )}
          </div>
        )}

        {activeTab === 'import' && (
          <div className="space-y-6">
            {/* Upload Section */}
            <div className="card p-6">
              <h3 className="text-lg font-semibold mb-4">Import Tax Lots from CSV</h3>
              <p className="text-sm text-gray-600 mb-4">
                Upload a CSV file with tax lot data. Required columns: Account Number, Symbol, Open Date, Units, Unit Cost.
                Optional columns: Account Display Name, Class, Asset Name, Cost Basis, Market Value, Short-Term Gain/Loss, Long-Term Gain/Loss, Total Gain Loss.
              </p>

              <div className="flex flex-wrap gap-4 items-end">
                <div className="flex-1 min-w-[200px]">
                  <label className="block text-sm font-medium text-gray-700 mb-1">Select CSV File</label>
                  <input
                    id="tax-lot-file-input"
                    type="file"
                    accept=".csv"
                    onChange={handleFileSelect}
                    className="block w-full text-sm text-gray-500
                      file:mr-4 file:py-2 file:px-4
                      file:rounded file:border-0
                      file:text-sm file:font-semibold
                      file:bg-blue-50 file:text-blue-700
                      hover:file:bg-blue-100"
                  />
                </div>
                <button
                  onClick={handlePreviewImport}
                  disabled={!importFile || previewLoading}
                  className="px-4 py-2 bg-gray-600 text-white rounded hover:bg-gray-700 disabled:opacity-50"
                >
                  {previewLoading ? 'Previewing...' : 'Preview'}
                </button>
                <button
                  onClick={handleCommitImport}
                  disabled={!importPreview || importPreview.valid_rows === 0 || importing}
                  className="px-4 py-2 bg-green-600 text-white rounded hover:bg-green-700 disabled:opacity-50"
                >
                  {importing ? 'Importing...' : 'Import'}
                </button>
              </div>
            </div>

            {/* Preview Section */}
            {importPreview && (
              <div className="card p-6">
                <h3 className="text-lg font-semibold mb-4">
                  Preview: {importPreview.file_name}
                </h3>

                <div className="grid grid-cols-4 gap-4 mb-6">
                  <div className="text-center p-3 bg-gray-50 rounded">
                    <div className="text-2xl font-bold text-gray-800">{importPreview.total_rows}</div>
                    <div className="text-sm text-gray-500">Total Rows</div>
                  </div>
                  <div className="text-center p-3 bg-green-50 rounded">
                    <div className="text-2xl font-bold text-green-600">{importPreview.valid_rows}</div>
                    <div className="text-sm text-gray-500">Valid Rows</div>
                  </div>
                  <div className="text-center p-3 bg-red-50 rounded">
                    <div className="text-2xl font-bold text-red-600">{importPreview.error_rows}</div>
                    <div className="text-sm text-gray-500">Errors</div>
                  </div>
                  <div className="text-center p-3 bg-yellow-50 rounded">
                    <div className="text-2xl font-bold text-yellow-600">{importPreview.warnings?.length || 0}</div>
                    <div className="text-sm text-gray-500">Warnings</div>
                  </div>
                </div>

                {importPreview.errors && importPreview.errors.length > 0 && (
                  <div className="mb-4">
                    <h4 className="font-medium text-red-700 mb-2">Errors</h4>
                    <div className="bg-red-50 rounded p-3 max-h-40 overflow-y-auto">
                      {importPreview.errors.map((e, i) => (
                        <div key={i} className="text-sm text-red-600">
                          Row {e.row}: {e.error}
                        </div>
                      ))}
                    </div>
                  </div>
                )}

                {importPreview.preview_data && importPreview.preview_data.length > 0 && (
                  <div>
                    <h4 className="font-medium text-gray-700 mb-2">Preview Data (first {importPreview.preview_data.length} rows)</h4>
                    <div className="overflow-x-auto">
                      <table className="min-w-full divide-y divide-gray-200 text-sm">
                        <thead className="bg-gray-50">
                          <tr>
                            <th className="px-3 py-2 text-left text-xs font-medium text-gray-500">Account</th>
                            <th className="px-3 py-2 text-left text-xs font-medium text-gray-500">Symbol</th>
                            <th className="px-3 py-2 text-left text-xs font-medium text-gray-500">Open Date</th>
                            <th className="px-3 py-2 text-right text-xs font-medium text-gray-500">Units</th>
                            <th className="px-3 py-2 text-right text-xs font-medium text-gray-500">Unit Cost</th>
                            <th className="px-3 py-2 text-right text-xs font-medium text-gray-500">Cost Basis</th>
                            <th className="px-3 py-2 text-right text-xs font-medium text-gray-500">Market Value</th>
                            <th className="px-3 py-2 text-right text-xs font-medium text-gray-500">Gain/Loss</th>
                          </tr>
                        </thead>
                        <tbody className="bg-white divide-y divide-gray-200">
                          {importPreview.preview_data.map((row) => (
                            <tr key={row.row_num} className="hover:bg-gray-50">
                              <td className="px-3 py-2">{row.account_number}</td>
                              <td className="px-3 py-2 font-medium">{row.symbol}</td>
                              <td className="px-3 py-2">{row.open_date}</td>
                              <td className="px-3 py-2 text-right">{row.units?.toFixed(2)}</td>
                              <td className="px-3 py-2 text-right">{formatCurrency(row.unit_cost)}</td>
                              <td className="px-3 py-2 text-right">{formatCurrency(row.cost_basis)}</td>
                              <td className="px-3 py-2 text-right">{row.market_value ? formatCurrency(row.market_value) : '-'}</td>
                              <td className={`px-3 py-2 text-right ${(row.total_gain_loss || 0) >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                                {row.total_gain_loss ? formatCurrency(row.total_gain_loss) : '-'}
                              </td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                )}
              </div>
            )}

            {/* Import History */}
            <div className="card p-6">
              <h3 className="text-lg font-semibold mb-4">Import History</h3>
              {importHistory.length === 0 ? (
                <p className="text-gray-500 text-center py-4">No imports yet</p>
              ) : (
                <div className="overflow-x-auto">
                  <table className="min-w-full divide-y divide-gray-200">
                    <thead className="bg-gray-50">
                      <tr>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">File Name</th>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Status</th>
                        <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Processed</th>
                        <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Imported</th>
                        <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Skipped</th>
                        <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Errors</th>
                        <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Date</th>
                        <th className="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase">Actions</th>
                      </tr>
                    </thead>
                    <tbody className="bg-white divide-y divide-gray-200">
                      {importHistory.map((imp) => (
                        <tr key={imp.id} className="hover:bg-gray-50">
                          <td className="px-4 py-3 font-medium">{imp.file_name}</td>
                          <td className="px-4 py-3">
                            <span className={`px-2 py-1 rounded text-xs ${
                              imp.status === 'completed' ? 'bg-green-100 text-green-800' :
                              imp.status === 'completed_with_errors' ? 'bg-yellow-100 text-yellow-800' :
                              'bg-gray-100 text-gray-800'
                            }`}>
                              {imp.status}
                            </span>
                          </td>
                          <td className="px-4 py-3 text-right">{imp.rows_processed}</td>
                          <td className="px-4 py-3 text-right text-green-600">{imp.rows_imported}</td>
                          <td className="px-4 py-3 text-right text-yellow-600">{imp.rows_skipped}</td>
                          <td className="px-4 py-3 text-right text-red-600">{imp.rows_error}</td>
                          <td className="px-4 py-3">{formatDate(imp.created_at)}</td>
                          <td className="px-4 py-3 text-center">
                            <button
                              onClick={() => handleDeleteImport(imp.id, imp.file_name)}
                              className="text-red-600 hover:text-red-800"
                            >
                              Delete
                            </button>
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              )}
            </div>
          </div>
        )}
      </div>
    </Layout>
  );
}
