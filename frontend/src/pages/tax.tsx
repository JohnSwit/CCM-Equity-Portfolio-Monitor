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

interface Account {
  id: number;
  account_number: string;
  name: string | null;
  lot_count: number;
}

interface SimLotResult {
  lot_id: number;
  account_id: number;
  account_number: string | null;
  security_id: number;
  symbol: string | null;
  purchase_date: string;
  remaining_shares: number;
  cost_basis_per_share: number;
  current_price: number | null;
  proceeds: number | null;
  cost_basis: number;
  gain_loss: number | null;
  gain_loss_pct?: number;
  holding_period_days: number;
  is_short_term: boolean;
  estimated_tax: number | null;
  error?: string;
}

interface SimulationResult {
  lots: SimLotResult[];
  totals: {
    total_proceeds: number;
    total_cost_basis: number;
    total_gain_loss: number;
    short_term_gain_loss: number;
    long_term_gain_loss: number;
    estimated_tax: number;
    total_shares: number;
    lot_count: number;
    gain_loss_pct: number;
  };
  missing_lot_ids: number[];
}

type TabType = 'summary' | 'lots' | 'harvest' | 'realized' | 'import';

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


  // Import state
  const [importFile, setImportFile] = useState<File | null>(null);
  const [importPreview, setImportPreview] = useState<ImportPreview | null>(null);
  const [importHistory, setImportHistory] = useState<TaxLotImport[]>([]);
  const [importing, setImporting] = useState(false);
  const [previewLoading, setPreviewLoading] = useState(false);

  // Tax Lots filters
  const [lotsFilterSymbol, setLotsFilterSymbol] = useState<string>('');
  const [lotsFilterAccount, setLotsFilterAccount] = useState<number | null>(null);
  const [lotsFilterGainLoss, setLotsFilterGainLoss] = useState<'all' | 'gains' | 'losses'>('all');
  const [lotsSortBy, setLotsSortBy] = useState<'none' | 'gain_high' | 'gain_low' | 'loss_high' | 'loss_low'>('none');

  // Lot selection & multi-lot simulation
  const [selectedLotIds, setSelectedLotIds] = useState<Set<number>>(new Set());
  const [lotSimResult, setLotSimResult] = useState<SimulationResult | null>(null);
  const [lotSimulating, setLotSimulating] = useState(false);

  // Harvest filters
  const [harvestFilterSymbol, setHarvestFilterSymbol] = useState<string>('');
  const [harvestFilterType, setHarvestFilterType] = useState<'all' | 'short_term' | 'long_term'>('all');
  const [harvestMinLoss, setHarvestMinLoss] = useState<number>(100);
  const [harvestSortBy, setHarvestSortBy] = useState<'none' | 'loss_high' | 'loss_low' | 'st_high' | 'lt_high'>('loss_high');


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

  // ---- Lot selection helpers ----
  const toggleLotSelection = (lotId: number) => {
    setSelectedLotIds(prev => {
      const next = new Set(prev);
      if (next.has(lotId)) {
        next.delete(lotId);
      } else {
        next.add(lotId);
      }
      return next;
    });
  };

  const toggleSelectAllFiltered = () => {
    // We reference filteredLots below after it's computed, so use lots directly here
    // with same filter logic. We'll call this after filteredLots is available.
    const filteredIds = filteredLots.map(l => l.id);
    const allSelected = filteredIds.length > 0 && filteredIds.every(id => selectedLotIds.has(id));
    if (allSelected) {
      setSelectedLotIds(prev => {
        const next = new Set(prev);
        filteredIds.forEach(id => next.delete(id));
        return next;
      });
    } else {
      setSelectedLotIds(prev => {
        const next = new Set(prev);
        filteredIds.forEach(id => next.add(id));
        return next;
      });
    }
  };

  const handleSimulateSelectedLots = async () => {
    if (selectedLotIds.size === 0) return;
    try {
      setLotSimulating(true);
      setError(null);
      const data = await api.simulateSelectedLots(Array.from(selectedLotIds));
      setLotSimResult(data);
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to simulate selected lots');
    } finally {
      setLotSimulating(false);
    }
  };

  const downloadSchwabCSV = () => {
    if (!lotSimResult) return;

    // Group selected lots by account+symbol and sum shares
    const grouped: Record<string, { account_number: string; symbol: string; shares: number }> = {};
    for (const lot of lotSimResult.lots) {
      if (lot.error) continue;
      const key = `${lot.account_number}|${lot.symbol}`;
      if (!grouped[key]) {
        grouped[key] = {
          account_number: lot.account_number || '',
          symbol: lot.symbol || '',
          shares: 0,
        };
      }
      grouped[key].shares += lot.remaining_shares;
    }

    // Build CSV: Col A=Account, Col B=S, Col C=Shares, Col D=Ticker, Col E=M, Col F=VSP
    const rows = Object.values(grouped).map(g =>
      `${g.account_number},S,${g.shares},${g.symbol},M,VSP`
    );
    const csv = rows.join('\n');

    const blob = new Blob([csv], { type: 'text/csv;charset=utf-8;' });
    const url = URL.createObjectURL(blob);
    const link = document.createElement('a');
    link.href = url;
    link.download = `schwab_trades_${new Date().toISOString().slice(0, 10)}.csv`;
    document.body.appendChild(link);
    link.click();
    document.body.removeChild(link);
    URL.revokeObjectURL(url);
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

  const getValueClass = (value: number | null | undefined) => {
    if (value == null) return 'text-zinc-400';
    return value >= 0 ? 'value-positive' : 'value-negative';
  };

  // Get unique symbols from lots for filter dropdown
  const uniqueSymbols = [...new Set(lots.map(lot => lot.symbol))].sort();
  const uniqueAccountsInLots = [...new Set(lots.map(lot => lot.account_id))];

  // Filtered and sorted lots
  const filteredLots = lots
    .filter(lot => {
      if (lotsFilterSymbol && lot.symbol !== lotsFilterSymbol) return false;
      if (lotsFilterAccount && lot.account_id !== lotsFilterAccount) return false;
      if (lotsFilterGainLoss === 'gains' && (lot.unrealized_gain_loss || 0) < 0) return false;
      if (lotsFilterGainLoss === 'losses' && (lot.unrealized_gain_loss || 0) >= 0) return false;
      return true;
    })
    .sort((a, b) => {
      const aVal = a.unrealized_gain_loss || 0;
      const bVal = b.unrealized_gain_loss || 0;
      switch (lotsSortBy) {
        case 'gain_high': return bVal - aVal;
        case 'gain_low': return aVal - bVal;
        case 'loss_high': return aVal - bVal;
        case 'loss_low': return bVal - aVal;
        default: return 0;
      }
    });

  // Get unique symbols from harvest candidates
  const harvestUniqueSymbols = [...new Set(harvestCandidates.map(c => c.symbol))].sort();

  // Filtered and sorted harvest candidates
  const filteredHarvestCandidates = harvestCandidates
    .filter(c => {
      if (harvestFilterSymbol && c.symbol !== harvestFilterSymbol) return false;
      if (harvestFilterType === 'short_term' && c.short_term_loss >= 0) return false;
      if (harvestFilterType === 'long_term' && c.long_term_loss >= 0) return false;
      if (Math.abs(c.unrealized_loss) < harvestMinLoss) return false;
      return true;
    })
    .sort((a, b) => {
      switch (harvestSortBy) {
        case 'loss_high': return a.unrealized_loss - b.unrealized_loss;
        case 'loss_low': return b.unrealized_loss - a.unrealized_loss;
        case 'st_high': return a.short_term_loss - b.short_term_loss;
        case 'lt_high': return a.long_term_loss - b.long_term_loss;
        default: return 0;
      }
    });

  const tabs = [
    { id: 'summary', label: 'Tax Summary' },
    { id: 'lots', label: 'Tax Lots' },
    { id: 'harvest', label: 'Loss Harvesting' },
    { id: 'realized', label: 'Realized Gains' },
    { id: 'import', label: 'Import Tax Lots' },
  ];

  if (authLoading || loading) {
    return (
      <Layout>
        <div className="flex items-center justify-center h-64">
          <div className="flex items-center gap-3 text-zinc-500">
            <svg className="loading-spinner" viewBox="0 0 24 24" fill="none">
              <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
              <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
            </svg>
            <span>Loading tax data...</span>
          </div>
        </div>
      </Layout>
    );
  }

  return (
    <Layout>
      <div className="space-y-6">
        {/* Page Header */}
        <div className="flex justify-between items-start">
          <div>
            <h1 className="text-2xl font-bold text-zinc-900">Tax Optimization</h1>
            <p className="text-sm text-zinc-500 mt-1">
              Manage tax lots, harvest losses, and optimize trades
            </p>
          </div>
          <div className="flex gap-4 items-end">
            <div>
              <label className="label">Tax Year</label>
              <select
                value={taxYear}
                onChange={(e) => setTaxYear(Number(e.target.value))}
                className="select"
              >
                {[2024, 2025, 2026].map((y) => (
                  <option key={y} value={y}>{y}</option>
                ))}
              </select>
            </div>
            <div>
              <label className="label">Account</label>
              <select
                value={selectedAccount || ''}
                onChange={(e) => setSelectedAccount(e.target.value ? Number(e.target.value) : null)}
                className="select"
              >
                <option value="">All Accounts</option>
                {accounts.map((a) => (
                  <option key={a.id} value={a.id}>{a.account_number}</option>
                ))}
              </select>
            </div>
          </div>
        </div>

        {/* Error Alert */}
        {error && (
          <div className="alert alert-danger flex justify-between items-center">
            <span>{error}</span>
            <button onClick={() => setError(null)} className="text-red-600 hover:text-red-800 font-medium">
              Dismiss
            </button>
          </div>
        )}

        {/* Tabs */}
        <div className="pill-tabs">
          {tabs.map((tab) => (
            <button
              key={tab.id}
              onClick={() => setActiveTab(tab.id as TabType)}
              className={`pill-tab ${activeTab === tab.id ? 'pill-tab-active' : ''}`}
            >
              {tab.label}
            </button>
          ))}
        </div>

        {/* Tab Content */}
        {activeTab === 'summary' && summary && (
          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
            {/* Realized Gains/Losses */}
            <div className="card">
              <div className="card-header">
                <h3 className="card-title">Realized Gains/Losses ({taxYear})</h3>
              </div>
              <div className="space-y-4">
                <div className="grid grid-cols-3 gap-4 text-sm">
                  <div></div>
                  <div className="text-center font-medium text-zinc-500">Short-Term</div>
                  <div className="text-center font-medium text-zinc-500">Long-Term</div>
                </div>
                <div className="grid grid-cols-3 gap-4 text-sm tabular-nums">
                  <div className="text-zinc-600 font-medium">Gains</div>
                  <div className="text-center value-positive">{formatCurrency(summary.short_term_realized_gains)}</div>
                  <div className="text-center value-positive">{formatCurrency(summary.long_term_realized_gains)}</div>
                </div>
                <div className="grid grid-cols-3 gap-4 text-sm tabular-nums">
                  <div className="text-zinc-600 font-medium">Losses</div>
                  <div className="text-center value-negative">({formatCurrency(summary.short_term_realized_losses)})</div>
                  <div className="text-center value-negative">({formatCurrency(summary.long_term_realized_losses)})</div>
                </div>
                <div className="divider" />
                <div className="grid grid-cols-3 gap-4 text-sm tabular-nums">
                  <div className="font-semibold text-zinc-900">Net</div>
                  <div className={`text-center font-semibold ${getValueClass(summary.net_short_term)}`}>
                    {formatCurrency(summary.net_short_term)}
                  </div>
                  <div className={`text-center font-semibold ${getValueClass(summary.net_long_term)}`}>
                    {formatCurrency(summary.net_long_term)}
                  </div>
                </div>
                {summary.wash_sale_disallowed > 0 && (
                  <div className="text-sm text-amber-600 bg-amber-50 p-3 rounded-lg mt-4">
                    Wash Sale Disallowed: {formatCurrency(summary.wash_sale_disallowed)}
                  </div>
                )}
              </div>
            </div>

            {/* Unrealized Gains/Losses */}
            <div className="card">
              <div className="card-header">
                <h3 className="card-title">Unrealized Gains/Losses</h3>
              </div>
              <div className="space-y-4">
                <div className="grid grid-cols-3 gap-4 text-sm">
                  <div></div>
                  <div className="text-center font-medium text-zinc-500">Short-Term</div>
                  <div className="text-center font-medium text-zinc-500">Long-Term</div>
                </div>
                <div className="grid grid-cols-3 gap-4 text-sm tabular-nums">
                  <div className="text-zinc-600 font-medium">Gains</div>
                  <div className="text-center value-positive">{formatCurrency(summary.short_term_unrealized_gains)}</div>
                  <div className="text-center value-positive">{formatCurrency(summary.long_term_unrealized_gains)}</div>
                </div>
                <div className="grid grid-cols-3 gap-4 text-sm tabular-nums">
                  <div className="text-zinc-600 font-medium">Losses</div>
                  <div className="text-center value-negative">({formatCurrency(summary.short_term_unrealized_losses)})</div>
                  <div className="text-center value-negative">({formatCurrency(summary.long_term_unrealized_losses)})</div>
                </div>
                <div className="divider" />
                <div className="grid grid-cols-3 gap-4 text-sm tabular-nums">
                  <div className="font-semibold text-zinc-900">Net</div>
                  <div className={`text-center font-semibold ${getValueClass(summary.net_short_term_unrealized)}`}>
                    {formatCurrency(summary.net_short_term_unrealized)}
                  </div>
                  <div className={`text-center font-semibold ${getValueClass(summary.net_long_term_unrealized)}`}>
                    {formatCurrency(summary.net_long_term_unrealized)}
                  </div>
                </div>
              </div>
            </div>

            {/* Tax Estimate */}
            <div className="card lg:col-span-2">
              <div className="card-header">
                <h3 className="card-title">Estimated Tax Impact</h3>
              </div>
              <div className="grid grid-cols-4 gap-6">
                <div className="metric-card metric-card-blue">
                  <div className="metric-label">Total Realized</div>
                  <div className={`metric-value-lg ${getValueClass(summary.total_realized)}`}>
                    {formatCurrency(summary.total_realized)}
                  </div>
                </div>
                <div className="metric-card metric-card-purple">
                  <div className="metric-label">Total Unrealized</div>
                  <div className={`metric-value-lg ${getValueClass(summary.total_unrealized)}`}>
                    {formatCurrency(summary.total_unrealized)}
                  </div>
                </div>
                <div className="metric-card metric-card-orange">
                  <div className="metric-label">Est. Tax Liability</div>
                  <div className="metric-value-lg text-amber-600">
                    {formatCurrency(summary.estimated_tax_liability)}
                  </div>
                </div>
                <div className="pl-4 py-1">
                  <div className="metric-label">Rates Used</div>
                  <div className="text-sm text-zinc-700 mt-1">
                    <div>Short-Term: 37%</div>
                    <div>Long-Term: 20%</div>
                  </div>
                </div>
              </div>
            </div>
          </div>
        )}

        {activeTab === 'lots' && (
          <div className="space-y-4">
            {/* Simulation Results (shown at top when available) */}
            {lotSimResult && (
              <div className="space-y-4">
                {/* Totals Summary */}
                <div className="card">
                  <div className="card-header">
                    <div className="flex justify-between items-center w-full">
                      <div>
                        <h3 className="card-title">Trade Simulation Results</h3>
                        <p className="card-subtitle">
                          {lotSimResult.totals.lot_count} lot{lotSimResult.totals.lot_count !== 1 ? 's' : ''} across{' '}
                          {[...new Set(lotSimResult.lots.filter(l => !l.error).map(l => l.symbol))].length} securities,{' '}
                          {lotSimResult.totals.total_shares.toFixed(2)} total shares
                        </p>
                      </div>
                      <div className="flex gap-2">
                        <button
                          onClick={downloadSchwabCSV}
                          className="btn btn-primary"
                        >
                          <svg className="h-4 w-4 mr-1.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 16v1a3 3 0 003 3h10a3 3 0 003-3v-1m-4-4l-4 4m0 0l-4-4m4 4V4" />
                          </svg>
                          Download Schwab CSV
                        </button>
                        <button
                          onClick={() => setLotSimResult(null)}
                          className="btn btn-ghost text-zinc-500"
                        >
                          Dismiss
                        </button>
                      </div>
                    </div>
                  </div>

                  <div className="grid grid-cols-2 lg:grid-cols-5 gap-4 mb-4">
                    <div className="metric-card metric-card-blue">
                      <div className="metric-label">Total Proceeds</div>
                      <div className="metric-value-lg tabular-nums">
                        {formatCurrency(lotSimResult.totals.total_proceeds)}
                      </div>
                    </div>
                    <div className="metric-card metric-card-purple">
                      <div className="metric-label">Total Cost Basis</div>
                      <div className="metric-value-lg tabular-nums">
                        {formatCurrency(lotSimResult.totals.total_cost_basis)}
                      </div>
                    </div>
                    <div className={`metric-card ${lotSimResult.totals.total_gain_loss >= 0 ? 'metric-card-green' : 'metric-card-red'}`}>
                      <div className="metric-label">Total Gain/Loss</div>
                      <div className={`metric-value-lg tabular-nums ${getValueClass(lotSimResult.totals.total_gain_loss)}`}>
                        {formatCurrency(lotSimResult.totals.total_gain_loss)}
                        <span className="text-sm ml-1">({formatPercent(lotSimResult.totals.gain_loss_pct)})</span>
                      </div>
                    </div>
                    <div className="metric-card metric-card-orange">
                      <div className="metric-label">Est. Tax</div>
                      <div className="metric-value-lg tabular-nums text-amber-600">
                        {formatCurrency(lotSimResult.totals.estimated_tax)}
                      </div>
                    </div>
                    <div className="p-3">
                      <div className="metric-label mb-1">Breakdown</div>
                      <div className="text-sm tabular-nums space-y-1">
                        <div className="flex justify-between">
                          <span className="text-zinc-500">Short-term:</span>
                          <span className={getValueClass(lotSimResult.totals.short_term_gain_loss)}>
                            {formatCurrency(lotSimResult.totals.short_term_gain_loss)}
                          </span>
                        </div>
                        <div className="flex justify-between">
                          <span className="text-zinc-500">Long-term:</span>
                          <span className={getValueClass(lotSimResult.totals.long_term_gain_loss)}>
                            {formatCurrency(lotSimResult.totals.long_term_gain_loss)}
                          </span>
                        </div>
                      </div>
                    </div>
                  </div>
                </div>

                {/* Per-lot detail table */}
                <div className="card p-0 overflow-hidden">
                  <div className="table-container mx-0">
                    <table className="table">
                      <thead>
                        <tr>
                          <th>Symbol</th>
                          <th>Account</th>
                          <th>Purchase Date</th>
                          <th className="text-right">Shares</th>
                          <th className="text-right">Cost Basis</th>
                          <th className="text-right">Proceeds</th>
                          <th className="text-right">Gain/Loss</th>
                          <th className="text-center">Term</th>
                          <th className="text-right">Est. Tax</th>
                        </tr>
                      </thead>
                      <tbody className="tabular-nums">
                        {lotSimResult.lots.map((lot) => (
                          <tr key={lot.lot_id} className={lot.error ? 'bg-red-50' : ''}>
                            <td className="font-semibold text-zinc-900">{lot.symbol}</td>
                            <td className="text-zinc-600">{lot.account_number || '-'}</td>
                            <td>{formatDate(lot.purchase_date)}</td>
                            <td className="text-right">{lot.remaining_shares.toFixed(2)}</td>
                            <td className="text-right">{formatCurrency(lot.cost_basis)}</td>
                            <td className="text-right">{lot.error ? <span className="text-red-500 text-xs">{lot.error}</span> : formatCurrency(lot.proceeds)}</td>
                            <td className={`text-right ${getValueClass(lot.gain_loss)}`}>
                              {lot.gain_loss != null ? (
                                <>{formatCurrency(lot.gain_loss)} ({formatPercent(lot.gain_loss_pct)})</>
                              ) : '-'}
                            </td>
                            <td className="text-center">
                              <span className={`badge ${lot.is_short_term ? 'badge-warning' : 'badge-success'}`}>
                                {lot.is_short_term ? 'Short' : 'Long'}
                              </span>
                            </td>
                            <td className="text-right">{lot.estimated_tax != null ? formatCurrency(lot.estimated_tax) : '-'}</td>
                          </tr>
                        ))}
                      </tbody>
                    </table>
                  </div>
                </div>
              </div>
            )}

            {/* Filters */}
            <div className="card">
              <div className="flex flex-wrap gap-4 items-end">
                <div>
                  <label className="label">Symbol</label>
                  <select
                    value={lotsFilterSymbol}
                    onChange={(e) => setLotsFilterSymbol(e.target.value)}
                    className="select min-w-[140px]"
                  >
                    <option value="">All Symbols</option>
                    {uniqueSymbols.map(symbol => (
                      <option key={symbol} value={symbol}>{symbol}</option>
                    ))}
                  </select>
                </div>
                <div>
                  <label className="label">Account</label>
                  <select
                    value={lotsFilterAccount || ''}
                    onChange={(e) => setLotsFilterAccount(e.target.value ? Number(e.target.value) : null)}
                    className="select min-w-[160px]"
                  >
                    <option value="">All Accounts</option>
                    {accounts.filter(a => uniqueAccountsInLots.includes(a.id)).map(a => (
                      <option key={a.id} value={a.id}>{a.account_number}</option>
                    ))}
                  </select>
                </div>
                <div>
                  <label className="label">Gain/Loss</label>
                  <select
                    value={lotsFilterGainLoss}
                    onChange={(e) => setLotsFilterGainLoss(e.target.value as 'all' | 'gains' | 'losses')}
                    className="select min-w-[140px]"
                  >
                    <option value="all">All</option>
                    <option value="gains">Gains Only</option>
                    <option value="losses">Losses Only</option>
                  </select>
                </div>
                <div>
                  <label className="label">Sort By</label>
                  <select
                    value={lotsSortBy}
                    onChange={(e) => setLotsSortBy(e.target.value as 'none' | 'gain_high' | 'gain_low' | 'loss_high' | 'loss_low')}
                    className="select min-w-[180px]"
                  >
                    <option value="none">Default</option>
                    <option value="gain_high">Gains: High to Low</option>
                    <option value="gain_low">Gains: Low to High</option>
                    <option value="loss_high">Losses: High to Low</option>
                    <option value="loss_low">Losses: Low to High</option>
                  </select>
                </div>
                <div className="text-sm text-zinc-500 self-center">
                  Showing {filteredLots.length} of {lots.length} lots
                </div>
              </div>
            </div>

            {/* Selection action bar */}
            {selectedLotIds.size > 0 && (
              <div className="card bg-blue-50 border-blue-200">
                <div className="flex items-center justify-between">
                  <div className="flex items-center gap-3">
                    <span className="text-sm font-medium text-blue-900">
                      {selectedLotIds.size} lot{selectedLotIds.size !== 1 ? 's' : ''} selected
                    </span>
                    <button
                      onClick={() => setSelectedLotIds(new Set())}
                      className="text-sm text-blue-600 hover:text-blue-800 underline"
                    >
                      Clear selection
                    </button>
                  </div>
                  <button
                    onClick={handleSimulateSelectedLots}
                    disabled={lotSimulating}
                    className="btn btn-primary"
                  >
                    {lotSimulating ? (
                      <>
                        <svg className="animate-spin h-4 w-4 mr-2" viewBox="0 0 24 24" fill="none">
                          <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                          <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
                        </svg>
                        Simulating...
                      </>
                    ) : (
                      `Simulate Sale of ${selectedLotIds.size} Lot${selectedLotIds.size !== 1 ? 's' : ''}`
                    )}
                  </button>
                </div>
              </div>
            )}

            <div className="card p-0 overflow-hidden">
              <div className="table-container mx-0">
                <table className="table">
                  <thead>
                    <tr>
                      <th className="w-10">
                        <input
                          type="checkbox"
                          checked={filteredLots.length > 0 && filteredLots.every(l => selectedLotIds.has(l.id))}
                          onChange={toggleSelectAllFiltered}
                          className="h-4 w-4 rounded border-zinc-300 text-blue-600 focus:ring-blue-500"
                        />
                      </th>
                      <th>Symbol</th>
                      <th>Account</th>
                      <th>Purchase Date</th>
                      <th className="text-right">Shares</th>
                      <th className="text-right">Cost Basis</th>
                      <th className="text-right">Current Value</th>
                      <th className="text-right">Gain/Loss</th>
                      <th className="text-center">Term</th>
                      <th className="text-right">Days Held</th>
                    </tr>
                  </thead>
                  <tbody className="tabular-nums">
                    {filteredLots.map((lot) => (
                      <tr
                        key={lot.id}
                        className={`cursor-pointer ${selectedLotIds.has(lot.id) ? 'bg-blue-50' : 'hover:bg-zinc-50'}`}
                        onClick={() => toggleLotSelection(lot.id)}
                      >
                        <td onClick={(e) => e.stopPropagation()}>
                          <input
                            type="checkbox"
                            checked={selectedLotIds.has(lot.id)}
                            onChange={() => toggleLotSelection(lot.id)}
                            className="h-4 w-4 rounded border-zinc-300 text-blue-600 focus:ring-blue-500"
                          />
                        </td>
                        <td className="font-semibold text-zinc-900">{lot.symbol}</td>
                        <td className="text-zinc-600">{lot.account_number || '-'}</td>
                        <td>{formatDate(lot.purchase_date)}</td>
                        <td className="text-right">{lot.remaining_shares.toFixed(2)}</td>
                        <td className="text-right">{formatCurrency(lot.remaining_cost_basis)}</td>
                        <td className="text-right">{formatCurrency(lot.current_value)}</td>
                        <td className={`text-right ${getValueClass(lot.unrealized_gain_loss)}`}>
                          {formatCurrency(lot.unrealized_gain_loss)} ({formatPercent(lot.unrealized_gain_loss_pct)})
                        </td>
                        <td className="text-center">
                          <span className={`badge ${lot.is_short_term ? 'badge-warning' : 'badge-success'}`}>
                            {lot.is_short_term ? 'Short' : 'Long'}
                          </span>
                        </td>
                        <td className="text-right">{lot.holding_period_days}</td>
                      </tr>
                    ))}
                    {filteredLots.length === 0 && (
                      <tr>
                        <td colSpan={10}>
                          <div className="empty-state">
                            <svg className="empty-state-icon" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 12h6m-6 4h6m2 5H7a2 2 0 01-2-2V5a2 2 0 012-2h5.586a1 1 0 01.707.293l5.414 5.414a1 1 0 01.293.707V19a2 2 0 01-2 2z" />
                            </svg>
                            <p className="empty-state-title">No tax lots found</p>
                            <p className="empty-state-description">
                              {lots.length === 0 ? 'Import tax lots from the Import tab.' : 'No lots match the current filters.'}
                            </p>
                          </div>
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
            </div>

          </div>
        )}

        {activeTab === 'harvest' && (
          <div className="space-y-6">
            <div className="alert alert-info">
              <div className="flex items-start gap-3">
                <svg className="h-5 w-5 text-blue-600 mt-0.5" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 16h-1v-4h-1m1-4h.01M21 12a9 9 0 11-18 0 9 9 0 0118 0z" />
                </svg>
                <div>
                  <h3 className="font-semibold text-blue-900">Tax-Loss Harvesting</h3>
                  <p className="text-sm text-blue-700 mt-1">
                    Positions below have unrealized losses that could be harvested to offset gains.
                    Watch for wash sale restrictions (30-day window).
                  </p>
                </div>
              </div>
            </div>

            {/* Filters */}
            <div className="card">
              <div className="flex flex-wrap gap-4 items-end">
                <div>
                  <label className="label">Symbol</label>
                  <select
                    value={harvestFilterSymbol}
                    onChange={(e) => setHarvestFilterSymbol(e.target.value)}
                    className="select min-w-[140px]"
                  >
                    <option value="">All Symbols</option>
                    {harvestUniqueSymbols.map(symbol => (
                      <option key={symbol} value={symbol}>{symbol}</option>
                    ))}
                  </select>
                </div>
                <div>
                  <label className="label">Loss Type</label>
                  <select
                    value={harvestFilterType}
                    onChange={(e) => setHarvestFilterType(e.target.value as 'all' | 'short_term' | 'long_term')}
                    className="select min-w-[160px]"
                  >
                    <option value="all">All Losses</option>
                    <option value="short_term">Short-Term Losses</option>
                    <option value="long_term">Long-Term Losses</option>
                  </select>
                </div>
                <div>
                  <label className="label">Min Loss Amount</label>
                  <input
                    type="number"
                    value={harvestMinLoss}
                    onChange={(e) => setHarvestMinLoss(Number(e.target.value))}
                    className="input w-[120px]"
                    min={0}
                    step={100}
                  />
                </div>
                <div>
                  <label className="label">Sort By</label>
                  <select
                    value={harvestSortBy}
                    onChange={(e) => setHarvestSortBy(e.target.value as 'none' | 'loss_high' | 'loss_low' | 'st_high' | 'lt_high')}
                    className="select min-w-[200px]"
                  >
                    <option value="loss_high">Total Loss: High to Low</option>
                    <option value="loss_low">Total Loss: Low to High</option>
                    <option value="st_high">Short-Term Loss: High to Low</option>
                    <option value="lt_high">Long-Term Loss: High to Low</option>
                    <option value="none">Default</option>
                  </select>
                </div>
                <div className="text-sm text-zinc-500 self-center">
                  Showing {filteredHarvestCandidates.length} of {harvestCandidates.length} candidates
                </div>
              </div>
            </div>

            <div className="card p-0 overflow-hidden">
              <div className="table-container mx-0">
                <table className="table">
                  <thead>
                    <tr>
                      <th>Symbol</th>
                      <th className="text-right">Shares</th>
                      <th className="text-right">Cost Basis</th>
                      <th className="text-right">Current Value</th>
                      <th className="text-right">Unrealized Loss</th>
                      <th className="text-right">Short-Term</th>
                      <th className="text-right">Long-Term</th>
                      <th className="text-center">Wash Sale Risk</th>
                    </tr>
                  </thead>
                  <tbody className="tabular-nums">
                    {filteredHarvestCandidates.map((c) => (
                      <tr key={c.security_id}>
                        <td className="font-semibold text-zinc-900">{c.symbol}</td>
                        <td className="text-right">{c.total_shares.toFixed(2)}</td>
                        <td className="text-right">{formatCurrency(c.total_cost_basis)}</td>
                        <td className="text-right">{formatCurrency(c.current_value)}</td>
                        <td className="text-right value-negative">
                          {formatCurrency(c.unrealized_loss)} ({formatPercent(c.unrealized_loss_pct)})
                        </td>
                        <td className="text-right value-negative">{formatCurrency(c.short_term_loss)}</td>
                        <td className="text-right value-negative">{formatCurrency(c.long_term_loss)}</td>
                        <td className="text-center">
                          {c.has_recent_purchase || c.has_pending_wash_sale ? (
                            <span className="badge badge-warning">
                              {c.wash_sale_window_end ? `Until ${formatDate(c.wash_sale_window_end)}` : 'At Risk'}
                            </span>
                          ) : (
                            <span className="badge badge-success">Clear</span>
                          )}
                        </td>
                      </tr>
                    ))}
                    {filteredHarvestCandidates.length === 0 && (
                      <tr>
                        <td colSpan={8}>
                          <div className="empty-state">
                            <svg className="empty-state-icon" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                              <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 12l2 2 4-4m6 2a9 9 0 11-18 0 9 9 0 0118 0z" />
                            </svg>
                            <p className="empty-state-title">No harvesting opportunities</p>
                            <p className="empty-state-description">
                              {harvestCandidates.length === 0 ? 'No tax-loss harvesting opportunities found.' : 'No candidates match the current filters.'}
                            </p>
                          </div>
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
          <div className="card p-0 overflow-hidden">
            <div className="table-container mx-0">
              <table className="table">
                <thead>
                  <tr>
                    <th>Symbol</th>
                    <th>Sale Date</th>
                    <th>Purchase Date</th>
                    <th className="text-right">Shares</th>
                    <th className="text-right">Proceeds</th>
                    <th className="text-right">Cost Basis</th>
                    <th className="text-right">Gain/Loss</th>
                    <th className="text-center">Term</th>
                    <th className="text-center">Wash Sale</th>
                  </tr>
                </thead>
                <tbody className="tabular-nums">
                  {realizedGains.map((g) => (
                    <tr key={g.id}>
                      <td className="font-semibold text-zinc-900">{g.symbol}</td>
                      <td>{formatDate(g.sale_date)}</td>
                      <td>{formatDate(g.purchase_date)}</td>
                      <td className="text-right">{g.shares_sold.toFixed(2)}</td>
                      <td className="text-right">{formatCurrency(g.proceeds)}</td>
                      <td className="text-right">{formatCurrency(g.cost_basis)}</td>
                      <td className={`text-right ${getValueClass(g.adjusted_gain_loss)}`}>
                        {formatCurrency(g.adjusted_gain_loss)}
                      </td>
                      <td className="text-center">
                        <span className={`badge ${g.is_short_term ? 'badge-warning' : 'badge-success'}`}>
                          {g.is_short_term ? 'Short' : 'Long'}
                        </span>
                      </td>
                      <td className="text-center">
                        {g.is_wash_sale && (
                          <span className="badge badge-danger">
                            {formatCurrency(g.wash_sale_disallowed)}
                          </span>
                        )}
                      </td>
                    </tr>
                  ))}
                  {realizedGains.length === 0 && (
                    <tr>
                      <td colSpan={9}>
                        <div className="empty-state">
                          <svg className="empty-state-icon" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2" />
                          </svg>
                          <p className="empty-state-title">No realized gains</p>
                          <p className="empty-state-description">No realized gains/losses for {taxYear}.</p>
                        </div>
                      </td>
                    </tr>
                  )}
                </tbody>
              </table>
            </div>
          </div>
        )}

        {activeTab === 'import' && (
          <div className="space-y-6">
            {/* Upload Section */}
            <div className="card">
              <div className="card-header">
                <div>
                  <h3 className="card-title">Import Tax Lots from CSV</h3>
                  <p className="card-subtitle">
                    Required columns: Account Number, Symbol, Open Date, Units, Unit Cost.
                    Optional: Account Display Name, Class, Asset Name, Cost Basis, Market Value, Gain/Loss columns.
                  </p>
                </div>
              </div>

              <div className="flex flex-wrap gap-4 items-end">
                <div className="flex-1 min-w-[200px]">
                  <label className="label">Select CSV File</label>
                  <input
                    id="tax-lot-file-input"
                    type="file"
                    accept=".csv"
                    onChange={handleFileSelect}
                    className="block w-full text-sm text-zinc-500
                      file:mr-4 file:py-2 file:px-4
                      file:rounded-lg file:border-0
                      file:text-sm file:font-medium
                      file:bg-blue-50 file:text-blue-700
                      hover:file:bg-blue-100
                      file:cursor-pointer cursor-pointer"
                  />
                </div>
                <button
                  onClick={handlePreviewImport}
                  disabled={!importFile || previewLoading}
                  className="btn btn-secondary"
                >
                  {previewLoading ? 'Previewing...' : 'Preview'}
                </button>
                <button
                  onClick={handleCommitImport}
                  disabled={!importPreview || importPreview.valid_rows === 0 || importing}
                  className="btn btn-primary"
                >
                  {importing ? 'Importing...' : 'Import'}
                </button>
              </div>
            </div>

            {/* Preview Section */}
            {importPreview && (
              <div className="card">
                <div className="card-header">
                  <h3 className="card-title">Preview: {importPreview.file_name}</h3>
                </div>

                <div className="grid grid-cols-4 gap-4 mb-6">
                  <div className="text-center p-4 bg-zinc-50 rounded-lg">
                    <div className="text-2xl font-bold text-zinc-800 tabular-nums">{importPreview.total_rows}</div>
                    <div className="text-sm text-zinc-500">Total Rows</div>
                  </div>
                  <div className="text-center p-4 bg-emerald-50 rounded-lg">
                    <div className="text-2xl font-bold text-emerald-600 tabular-nums">{importPreview.valid_rows}</div>
                    <div className="text-sm text-zinc-500">Valid Rows</div>
                  </div>
                  <div className="text-center p-4 bg-red-50 rounded-lg">
                    <div className="text-2xl font-bold text-red-600 tabular-nums">{importPreview.error_rows}</div>
                    <div className="text-sm text-zinc-500">Errors</div>
                  </div>
                  <div className="text-center p-4 bg-amber-50 rounded-lg">
                    <div className="text-2xl font-bold text-amber-600 tabular-nums">{importPreview.warnings?.length || 0}</div>
                    <div className="text-sm text-zinc-500">Warnings</div>
                  </div>
                </div>

                {importPreview.errors && importPreview.errors.length > 0 && (
                  <div className="mb-4">
                    <h4 className="font-medium text-red-700 mb-2">Errors</h4>
                    <div className="bg-red-50 rounded-lg p-3 max-h-40 overflow-y-auto border border-red-200">
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
                    <h4 className="font-medium text-zinc-700 mb-2">Preview Data (first {importPreview.preview_data.length} rows)</h4>
                    <div className="overflow-x-auto border border-zinc-200 rounded-lg">
                      <table className="min-w-full text-sm">
                        <thead className="bg-zinc-50 border-b border-zinc-200">
                          <tr>
                            <th className="px-3 py-2 text-left text-xs font-semibold text-zinc-500 uppercase">Account</th>
                            <th className="px-3 py-2 text-left text-xs font-semibold text-zinc-500 uppercase">Symbol</th>
                            <th className="px-3 py-2 text-left text-xs font-semibold text-zinc-500 uppercase">Open Date</th>
                            <th className="px-3 py-2 text-right text-xs font-semibold text-zinc-500 uppercase">Units</th>
                            <th className="px-3 py-2 text-right text-xs font-semibold text-zinc-500 uppercase">Unit Cost</th>
                            <th className="px-3 py-2 text-right text-xs font-semibold text-zinc-500 uppercase">Cost Basis</th>
                            <th className="px-3 py-2 text-right text-xs font-semibold text-zinc-500 uppercase">Market Value</th>
                            <th className="px-3 py-2 text-right text-xs font-semibold text-zinc-500 uppercase">Gain/Loss</th>
                          </tr>
                        </thead>
                        <tbody className="tabular-nums">
                          {importPreview.preview_data.map((row) => (
                            <tr key={row.row_num} className="border-b border-zinc-100 hover:bg-zinc-50">
                              <td className="px-3 py-2">{row.account_number}</td>
                              <td className="px-3 py-2 font-medium text-zinc-900">{row.symbol}</td>
                              <td className="px-3 py-2">{row.open_date}</td>
                              <td className="px-3 py-2 text-right">{row.units?.toFixed(2)}</td>
                              <td className="px-3 py-2 text-right">{formatCurrency(row.unit_cost)}</td>
                              <td className="px-3 py-2 text-right">{formatCurrency(row.cost_basis)}</td>
                              <td className="px-3 py-2 text-right">{row.market_value ? formatCurrency(row.market_value) : '-'}</td>
                              <td className={`px-3 py-2 text-right ${getValueClass(row.total_gain_loss)}`}>
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
            <div className="card">
              <div className="card-header">
                <h3 className="card-title">Import History</h3>
              </div>
              {importHistory.length === 0 ? (
                <div className="empty-state">
                  <svg className="empty-state-icon" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M7 16a4 4 0 01-.88-7.903A5 5 0 1115.9 6L16 6a5 5 0 011 9.9M15 13l-3-3m0 0l-3 3m3-3v12" />
                  </svg>
                  <p className="empty-state-title">No imports yet</p>
                  <p className="empty-state-description">Upload a CSV file to import tax lots.</p>
                </div>
              ) : (
                <div className="table-container mx-0 -mb-6">
                  <table className="table">
                    <thead>
                      <tr>
                        <th>File Name</th>
                        <th>Status</th>
                        <th className="text-right">Processed</th>
                        <th className="text-right">Imported</th>
                        <th className="text-right">Skipped</th>
                        <th className="text-right">Errors</th>
                        <th>Date</th>
                        <th className="text-center">Actions</th>
                      </tr>
                    </thead>
                    <tbody className="tabular-nums">
                      {importHistory.map((imp) => (
                        <tr key={imp.id}>
                          <td className="font-medium text-zinc-900">{imp.file_name}</td>
                          <td>
                            <span className={`badge ${
                              imp.status === 'completed' ? 'badge-success' :
                              imp.status === 'completed_with_errors' ? 'badge-warning' :
                              'badge-neutral'
                            }`}>
                              {imp.status}
                            </span>
                          </td>
                          <td className="text-right">{imp.rows_processed}</td>
                          <td className="text-right text-emerald-600">{imp.rows_imported}</td>
                          <td className="text-right text-amber-600">{imp.rows_skipped}</td>
                          <td className="text-right text-red-600">{imp.rows_error}</td>
                          <td>{formatDate(imp.created_at)}</td>
                          <td className="text-center">
                            <button
                              onClick={() => handleDeleteImport(imp.id, imp.file_name)}
                              className="btn btn-ghost btn-xs text-red-600 hover:text-red-700"
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
