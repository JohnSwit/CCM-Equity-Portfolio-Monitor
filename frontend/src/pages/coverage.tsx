import { useState, useEffect, Fragment, useRef } from 'react';
import { useRouter } from 'next/router';
import Layout from '../components/Layout';
import { useAuth } from '../contexts/AuthContext';
import { api } from '../lib/api';

interface Analyst {
  id: number;
  name: string;
  is_active: boolean;
}

interface MetricEstimates {
  ccm_minus1yr: number | null;
  ccm_1yr: number | null;
  ccm_2yr: number | null;
  ccm_3yr: number | null;
  street_minus1yr: number | null;
  street_1yr: number | null;
  street_2yr: number | null;
  street_3yr: number | null;
  growth_ccm_1yr: number | null;
  growth_ccm_2yr: number | null;
  growth_ccm_3yr: number | null;
  growth_street_1yr: number | null;
  growth_street_2yr: number | null;
  growth_street_3yr: number | null;
  diff_1yr_pct: number | null;
  diff_2yr_pct: number | null;
  diff_3yr_pct: number | null;
}

interface MarginEstimates {
  ccm_minus1yr: number | null;
  ccm_1yr: number | null;
  ccm_2yr: number | null;
  ccm_3yr: number | null;
  street_minus1yr: number | null;
  street_1yr: number | null;
  street_2yr: number | null;
  street_3yr: number | null;
}

interface ModelData {
  irr_3yr: number | null;
  ccm_fair_value: number | null;
  street_price_target: number | null;
  current_price: number | null;
  ccm_upside_pct: number | null;
  street_upside_pct: number | null;
  ccm_vs_street_diff_pct: number | null;
  revenue: MetricEstimates | null;
  ebitda: MetricEstimates | null;
  eps: MetricEstimates | null;
  fcf: MetricEstimates | null;
  ebitda_margin: MarginEstimates | null;
  fcf_margin: MarginEstimates | null;
  data_as_of: string | null;
  last_refreshed: string | null;
}

interface CoverageSnapshot {
  id: number;
  coverage_id: number;
  snapshot_name: string | null;
  created_at: string;
  ccm_fair_value: number | null;
  irr_3yr: number | null;
  revenue_ccm_1yr: number | null;
  ebitda_ccm_1yr: number | null;
}

interface Coverage {
  id: number;
  ticker: string;
  primary_analyst: Analyst | null;
  secondary_analyst: Analyst | null;
  model_path: string | null;
  model_share_link: string | null;
  notes: string | null;
  is_active: boolean;
  model_updated: boolean;
  thesis: string | null;
  bull_case: string | null;
  bear_case: string | null;
  alert: string | null;
  has_alert: boolean;
  market_value: number | null;
  weight_pct: number | null;
  current_price: number | null;
  model_data: ModelData | null;
  documents: any[];
  snapshots: CoverageSnapshot[];
  created_at: string;
  updated_at: string;
}

type SortField = 'none' | 'market_value' | 'upside' | 'irr';

export default function CoveragePage() {
  const router = useRouter();
  const { user, loading: authLoading } = useAuth();
  const [coverages, setCoverages] = useState<Coverage[]>([]);
  const [analysts, setAnalysts] = useState<Analyst[]>([]);
  const [totalFirmValue, setTotalFirmValue] = useState<number>(0);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);

  // Add ticker form
  const [newTicker, setNewTicker] = useState('');
  const [newPrimaryAnalyst, setNewPrimaryAnalyst] = useState<number | null>(null);
  const [newSecondaryAnalyst, setNewSecondaryAnalyst] = useState<number | null>(null);
  const [newModelPath, setNewModelPath] = useState('');
  const [newModelShareLink, setNewModelShareLink] = useState('');
  const [adding, setAdding] = useState(false);

  // Edit modal
  const [editingCoverage, setEditingCoverage] = useState<Coverage | null>(null);
  const [editPrimaryAnalyst, setEditPrimaryAnalyst] = useState<number | null>(null);
  const [editSecondaryAnalyst, setEditSecondaryAnalyst] = useState<number | null>(null);
  const [editModelPath, setEditModelPath] = useState('');
  const [editModelShareLink, setEditModelShareLink] = useState('');
  const [editThesis, setEditThesis] = useState('');
  const [editBullCase, setEditBullCase] = useState('');
  const [editBearCase, setEditBearCase] = useState('');
  const [editAlert, setEditAlert] = useState('');

  // Snapshot diff view
  const [viewingSnapshotDiff, setViewingSnapshotDiff] = useState<{ coverageId: number; snapshotId: number } | null>(null);
  const [snapshotDiff, setSnapshotDiff] = useState<any>(null);

  // Expanded detail view
  const [expandedTicker, setExpandedTicker] = useState<number | null>(null);

  // Refresh status
  const [refreshing, setRefreshing] = useState<number | null>(null);
  const [refreshingAll, setRefreshingAll] = useState(false);

  // Inline editing for thesis/bull/bear
  const [inlineThesis, setInlineThesis] = useState<Record<number, string>>({});
  const [inlineBull, setInlineBull] = useState<Record<number, string>>({});
  const [inlineBear, setInlineBear] = useState<Record<number, string>>({});
  const [savingInline, setSavingInline] = useState<number | null>(null);

  // Column filters
  const [filterPrimaryAnalyst, setFilterPrimaryAnalyst] = useState<string>('');
  const [filterSecondaryAnalyst, setFilterSecondaryAnalyst] = useState<string>('');
  const [sortField, setSortField] = useState<SortField>('none');

  useEffect(() => {
    if (!authLoading && !user) {
      router.push('/login');
    }
  }, [authLoading, user, router]);

  useEffect(() => {
    if (user) {
      loadData();
    }
  }, [user]);

  const loadData = async () => {
    try {
      setLoading(true);
      setError(null);

      // Initialize analysts if needed
      await api.initAnalysts();

      const [analystData, coverageData] = await Promise.all([
        api.getAnalysts(),
        api.getCoverageList(true),
      ]);

      setAnalysts(analystData);
      setCoverages(coverageData.coverages || []);
      setTotalFirmValue(coverageData.total_firm_value || 0);
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to load data');
    } finally {
      setLoading(false);
    }
  };

  const handleAddTicker = async () => {
    if (!newTicker.trim()) return;

    try {
      setAdding(true);
      await api.createCoverage({
        ticker: newTicker.toUpperCase(),
        primary_analyst_id: newPrimaryAnalyst || undefined,
        secondary_analyst_id: newSecondaryAnalyst || undefined,
        model_path: newModelPath || undefined,
        model_share_link: newModelShareLink || undefined,
      });

      setNewTicker('');
      setNewPrimaryAnalyst(null);
      setNewSecondaryAnalyst(null);
      setNewModelPath('');
      setNewModelShareLink('');

      await loadData();
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to add ticker');
    } finally {
      setAdding(false);
    }
  };

  const handleUpdateCoverage = async () => {
    if (!editingCoverage) return;

    try {
      await api.updateCoverage(editingCoverage.id, {
        primary_analyst_id: editPrimaryAnalyst || undefined,
        secondary_analyst_id: editSecondaryAnalyst || undefined,
        model_path: editModelPath || undefined,
        model_share_link: editModelShareLink || undefined,
        thesis: editThesis || undefined,
        bull_case: editBullCase || undefined,
        bear_case: editBearCase || undefined,
        alert: editAlert || undefined,
      });

      setEditingCoverage(null);
      await loadData();
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to update coverage');
    }
  };

  const handleDeleteCoverage = async (id: number) => {
    if (!confirm('Are you sure you want to remove this ticker from coverage?')) return;

    try {
      await api.deleteCoverage(id);
      await loadData();
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to delete coverage');
    }
  };

  const handleRefreshModel = async (id: number) => {
    try {
      setRefreshing(id);
      await api.refreshModelData(id);
      await loadData();
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to refresh model data');
    } finally {
      setRefreshing(null);
    }
  };

  const handleRefreshAllModels = async () => {
    try {
      setRefreshingAll(true);
      const result = await api.refreshAllModels();
      if (result.failed > 0) {
        setError(`Refreshed ${result.success} models, ${result.failed} failed`);
      }
      await loadData();
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to refresh models');
    } finally {
      setRefreshingAll(false);
    }
  };

  const openEditModal = (coverage: Coverage) => {
    setEditingCoverage(coverage);
    setEditPrimaryAnalyst(coverage.primary_analyst?.id || null);
    setEditSecondaryAnalyst(coverage.secondary_analyst?.id || null);
    setEditModelPath(coverage.model_path || '');
    setEditModelShareLink(coverage.model_share_link || '');
    setEditThesis(coverage.thesis || '');
    setEditBullCase(coverage.bull_case || '');
    setEditBearCase(coverage.bear_case || '');
    setEditAlert(coverage.alert || '');
  };

  const handleToggleModelUpdated = async (coverageId: number, currentValue: boolean) => {
    try {
      await api.updateCoverage(coverageId, { model_updated: !currentValue });
      await loadData();
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to update model status');
    }
  };

  // Inline save for thesis/bull/bear - saves on blur
  const handleInlineSave = async (coverageId: number, field: 'thesis' | 'bull_case' | 'bear_case', value: string) => {
    const coverage = coverages.find(c => c.id === coverageId);
    if (!coverage) return;

    // Only save if value actually changed
    const original = coverage[field] || '';
    if (value === original) return;

    try {
      setSavingInline(coverageId);
      await api.updateCoverage(coverageId, { [field]: value || undefined });
      await loadData();
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to save');
    } finally {
      setSavingInline(null);
    }
  };

  // Initialize inline values when expanding a ticker
  const handleExpandTicker = (coverageId: number) => {
    if (expandedTicker === coverageId) {
      setExpandedTicker(null);
      return;
    }
    const coverage = coverages.find(c => c.id === coverageId);
    if (coverage) {
      setInlineThesis(prev => ({ ...prev, [coverageId]: coverage.thesis || '' }));
      setInlineBull(prev => ({ ...prev, [coverageId]: coverage.bull_case || '' }));
      setInlineBear(prev => ({ ...prev, [coverageId]: coverage.bear_case || '' }));
    }
    setExpandedTicker(coverageId);
  };

  const handleViewSnapshotDiff = async (coverageId: number, snapshotId: number) => {
    try {
      const diff = await api.getSnapshotDiff(coverageId, snapshotId);
      setSnapshotDiff(diff);
      setViewingSnapshotDiff({ coverageId, snapshotId });
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to load snapshot diff');
    }
  };

  const handleDeleteSnapshot = async (coverageId: number, snapshotId: number) => {
    if (!confirm('Delete this snapshot?')) return;
    try {
      await api.deleteCoverageSnapshot(coverageId, snapshotId);
      await loadData();
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to delete snapshot');
    }
  };

  // --- Formatting helpers ---
  const formatDollar = (value: number | null | undefined) => {
    if (value == null) return '-';
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD',
      minimumFractionDigits: 0,
      maximumFractionDigits: 0,
    }).format(value);
  };

  // Price-like values: fair value, street target, EPS - show 2 decimals
  const formatPrice = (value: number | null | undefined) => {
    if (value == null) return '-';
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD',
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    }).format(value);
  };

  const formatEstimate = (value: number | null | undefined) => {
    if (value == null) return '-';
    return '$' + new Intl.NumberFormat('en-US', {
      minimumFractionDigits: 0,
      maximumFractionDigits: 0,
    }).format(Math.round(value));
  };

  // EPS-like values: smaller numbers with 2 decimals
  const formatEps = (value: number | null | undefined) => {
    if (value == null) return '-';
    return '$' + new Intl.NumberFormat('en-US', {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    }).format(value);
  };

  const formatPercent = (value: number | null | undefined) => {
    if (value == null) return '-';
    return `${value >= 0 ? '+' : ''}${value.toFixed(2)}%`;
  };

  const formatWeight = (value: number | null | undefined) => {
    if (value == null) return '-';
    return `${value.toFixed(2)}%`;
  };

  const formatBps = (current: number | null | undefined, prior: number | null | undefined) => {
    if (current == null || prior == null) return '-';
    const bps = (current - prior) * 100;
    return `${bps >= 0 ? '+' : ''}${bps.toFixed(0)} bps`;
  };

  const getBpsClass = (current: number | null | undefined, prior: number | null | undefined) => {
    if (current == null || prior == null) return 'text-zinc-400';
    const diff = current - prior;
    if (diff > 0) return 'value-positive';
    if (diff < 0) return 'value-negative';
    return 'text-zinc-400';
  };

  const getValueClass = (value: number | null | undefined) => {
    if (value == null) return 'text-zinc-400';
    return value >= 0 ? 'value-positive' : 'value-negative';
  };

  // --- Filtering and sorting ---
  const filteredAndSortedCoverages = coverages
    .filter(c => {
      if (filterPrimaryAnalyst && (c.primary_analyst?.name || '') !== filterPrimaryAnalyst) return false;
      if (filterSecondaryAnalyst && (c.secondary_analyst?.name || '') !== filterSecondaryAnalyst) return false;
      return true;
    })
    .sort((a, b) => {
      switch (sortField) {
        case 'market_value':
          return (b.market_value || 0) - (a.market_value || 0);
        case 'upside':
          return (b.model_data?.ccm_upside_pct || -Infinity) - (a.model_data?.ccm_upside_pct || -Infinity);
        case 'irr':
          return (b.model_data?.irr_3yr || -Infinity) - (a.model_data?.irr_3yr || -Infinity);
        default:
          return 0;
      }
    });

  const analystNames = [...new Set(
    coverages.flatMap(c => [c.primary_analyst?.name, c.secondary_analyst?.name]).filter(Boolean)
  )].sort() as string[];

  if (authLoading || loading) {
    return (
      <Layout>
        <div className="flex items-center justify-center h-64">
          <div className="flex items-center gap-3 text-zinc-500">
            <svg className="loading-spinner" viewBox="0 0 24 24" fill="none">
              <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
              <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
            </svg>
            <span>Loading coverage data...</span>
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
            <h1 className="text-2xl font-bold text-zinc-900">Active Coverage</h1>
            <p className="text-sm text-zinc-500 mt-1">
              Track analyst coverage with linked Excel models and valuation metrics
            </p>
          </div>
          <button
            onClick={handleRefreshAllModels}
            disabled={refreshingAll}
            className="btn btn-secondary"
          >
            {refreshingAll ? (
              <>
                <svg className="animate-spin h-4 w-4" viewBox="0 0 24 24" fill="none">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
                </svg>
                Refreshing...
              </>
            ) : (
              <>
                <svg className="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
                </svg>
                Refresh All Models
              </>
            )}
          </button>
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

        {/* Add Ticker Form */}
        <div className="card">
          <div className="card-header">
            <h2 className="card-title">Add Ticker to Coverage</h2>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-6 gap-4">
            <div>
              <label className="label">Ticker</label>
              <input
                type="text"
                value={newTicker}
                onChange={(e) => setNewTicker(e.target.value.toUpperCase())}
                placeholder="AAPL"
                className="input"
              />
            </div>
            <div>
              <label className="label">Primary Analyst</label>
              <select
                value={newPrimaryAnalyst || ''}
                onChange={(e) => setNewPrimaryAnalyst(e.target.value ? Number(e.target.value) : null)}
                className="select"
              >
                <option value="">Select...</option>
                {analysts.map((a) => (
                  <option key={a.id} value={a.id}>{a.name}</option>
                ))}
              </select>
            </div>
            <div>
              <label className="label">Secondary Analyst</label>
              <select
                value={newSecondaryAnalyst || ''}
                onChange={(e) => setNewSecondaryAnalyst(e.target.value ? Number(e.target.value) : null)}
                className="select"
              >
                <option value="">Select...</option>
                {analysts.map((a) => (
                  <option key={a.id} value={a.id}>{a.name}</option>
                ))}
              </select>
            </div>
            <div className="md:col-span-2">
              <label className="label">Model Path</label>
              <input
                type="text"
                value={newModelPath}
                onChange={(e) => setNewModelPath(e.target.value)}
                placeholder="/models/AAPL/AAPL Model.xlsx"
                className="input"
              />
            </div>
            <div className="flex items-end">
              <button
                onClick={handleAddTicker}
                disabled={adding || !newTicker.trim()}
                className="btn btn-primary w-full"
              >
                {adding ? 'Adding...' : 'Add Ticker'}
              </button>
            </div>
          </div>
        </div>

        {/* Coverage Table */}
        <div className="card p-0 overflow-hidden">
          <div className="table-container mx-0">
            <table className="table">
              <thead>
                <tr>
                  <th>Ticker</th>
                  <th className="text-center">Status</th>
                  <th>
                    <div className="space-y-1">
                      <div>Primary</div>
                      <select
                        value={filterPrimaryAnalyst}
                        onChange={(e) => setFilterPrimaryAnalyst(e.target.value)}
                        className="text-xs font-normal bg-white border border-zinc-200 rounded px-1.5 py-0.5 w-full"
                        onClick={(e) => e.stopPropagation()}
                      >
                        <option value="">All</option>
                        {analystNames.map(name => (
                          <option key={name} value={name}>{name}</option>
                        ))}
                      </select>
                    </div>
                  </th>
                  <th>
                    <div className="space-y-1">
                      <div>Secondary</div>
                      <select
                        value={filterSecondaryAnalyst}
                        onChange={(e) => setFilterSecondaryAnalyst(e.target.value)}
                        className="text-xs font-normal bg-white border border-zinc-200 rounded px-1.5 py-0.5 w-full"
                        onClick={(e) => e.stopPropagation()}
                      >
                        <option value="">All</option>
                        {analystNames.map(name => (
                          <option key={name} value={name}>{name}</option>
                        ))}
                      </select>
                    </div>
                  </th>
                  <th className="text-right">Price</th>
                  <th className="text-right">
                    <div className="space-y-1">
                      <div
                        className={`cursor-pointer hover:text-blue-600 ${sortField === 'market_value' ? 'text-blue-600' : ''}`}
                        onClick={() => setSortField(sortField === 'market_value' ? 'none' : 'market_value')}
                      >
                        Mkt Value {sortField === 'market_value' ? ' \u2193' : ''}
                      </div>
                    </div>
                  </th>
                  <th className="text-right">Weight</th>
                  <th className="text-right">CCM FV</th>
                  <th className="text-right">
                    <div
                      className={`cursor-pointer hover:text-blue-600 ${sortField === 'upside' ? 'text-blue-600' : ''}`}
                      onClick={() => setSortField(sortField === 'upside' ? 'none' : 'upside')}
                    >
                      Upside {sortField === 'upside' ? ' \u2193' : ''}
                    </div>
                  </th>
                  <th className="text-right">
                    <div
                      className={`cursor-pointer hover:text-blue-600 ${sortField === 'irr' ? 'text-blue-600' : ''}`}
                      onClick={() => setSortField(sortField === 'irr' ? 'none' : 'irr')}
                    >
                      3Y IRR {sortField === 'irr' ? ' \u2193' : ''}
                    </div>
                  </th>
                  <th className="text-center">Model</th>
                  <th className="text-center">Actions</th>
                </tr>
              </thead>
              <tbody>
                {filteredAndSortedCoverages.map((coverage) => (
                  <Fragment key={coverage.id}>
                    <tr
                      className={`cursor-pointer transition-colors ${expandedTicker === coverage.id ? 'bg-blue-50/50' : ''}`}
                      onClick={() => handleExpandTicker(coverage.id)}
                    >
                      <td className="font-semibold text-zinc-900">
                        <div className="flex items-center gap-2">
                          {coverage.has_alert && (
                            <span className="inline-flex items-center justify-center w-5 h-5 rounded-full bg-amber-100 text-amber-600 text-xs" title={coverage.alert || 'Action required'}>
                              !
                            </span>
                          )}
                          <span>{coverage.ticker}</span>
                          <svg className={`h-4 w-4 text-zinc-400 transition-transform ${expandedTicker === coverage.id ? 'rotate-180' : ''}`} fill="none" viewBox="0 0 24 24" stroke="currentColor">
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                          </svg>
                        </div>
                      </td>
                      <td className="text-center">
                        <button
                          onClick={(e) => { e.stopPropagation(); handleToggleModelUpdated(coverage.id, coverage.model_updated); }}
                          className={`badge ${coverage.model_updated ? 'badge-success' : 'badge-danger'} cursor-pointer hover:opacity-80 transition-opacity`}
                        >
                          {coverage.model_updated ? 'Updated' : 'Needs Update'}
                        </button>
                      </td>
                      <td className="text-zinc-600">{coverage.primary_analyst?.name || '-'}</td>
                      <td className="text-zinc-600">{coverage.secondary_analyst?.name || '-'}</td>
                      <td className="text-right tabular-nums font-medium">
                        {coverage.current_price ? formatPrice(coverage.current_price) : '-'}
                      </td>
                      <td className="text-right tabular-nums">{formatDollar(coverage.market_value)}</td>
                      <td className="text-right tabular-nums">{formatWeight(coverage.weight_pct)}</td>
                      <td className="text-right tabular-nums font-medium">
                        {coverage.model_data?.ccm_fair_value ? formatPrice(coverage.model_data.ccm_fair_value) : '-'}
                      </td>
                      <td className={`text-right tabular-nums font-medium ${getValueClass(coverage.model_data?.ccm_upside_pct)}`}>
                        {formatPercent(coverage.model_data?.ccm_upside_pct)}
                      </td>
                      <td className="text-right tabular-nums font-medium">
                        {coverage.model_data?.irr_3yr != null ? `${(coverage.model_data.irr_3yr * 100).toFixed(1)}%` : '-'}
                      </td>
                      <td className="text-center">
                        {coverage.model_share_link ? (
                          <a
                            href={coverage.model_share_link}
                            target="_blank"
                            rel="noopener noreferrer"
                            onClick={(e) => e.stopPropagation()}
                            className="text-blue-600 hover:text-blue-800 font-medium text-sm"
                          >
                            Open
                          </a>
                        ) : coverage.model_path ? (
                          <span className="badge badge-neutral">Local</span>
                        ) : (
                          <span className="text-zinc-400">-</span>
                        )}
                      </td>
                      <td className="text-center">
                        <div className="flex items-center justify-center gap-1">
                          <button
                            onClick={(e) => { e.stopPropagation(); openEditModal(coverage); }}
                            className="btn btn-ghost btn-xs"
                          >
                            Edit
                          </button>
                          {coverage.model_path && (
                            <button
                              onClick={(e) => { e.stopPropagation(); handleRefreshModel(coverage.id); }}
                              disabled={refreshing === coverage.id}
                              className="btn btn-ghost btn-xs text-emerald-600 hover:text-emerald-700 disabled:opacity-50"
                            >
                              {refreshing === coverage.id ? '...' : 'Refresh'}
                            </button>
                          )}
                          <button
                            onClick={(e) => { e.stopPropagation(); handleDeleteCoverage(coverage.id); }}
                            className="btn btn-ghost btn-xs text-red-600 hover:text-red-700"
                          >
                            Remove
                          </button>
                        </div>
                      </td>
                    </tr>
                    {/* Expanded Detail Row */}
                    {expandedTicker === coverage.id && (
                      <tr>
                        <td colSpan={12} className="p-0">
                          <div className="bg-zinc-50/50 border-t border-zinc-100 p-6">
                            <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                              {/* Valuation Summary */}
                              <div className="bg-white rounded-lg border border-zinc-200 p-5">
                                <h3 className="text-sm font-semibold text-zinc-900 mb-4">Valuation</h3>
                                <div className="grid grid-cols-3 gap-6">
                                  <div className="metric-card metric-card-blue">
                                    <div className="metric-label">CCM Fair Value</div>
                                    <div className="metric-value">
                                      {coverage.model_data?.ccm_fair_value ? formatPrice(coverage.model_data.ccm_fair_value) : '-'}
                                    </div>
                                    <div className={`text-sm ${getValueClass(coverage.model_data?.ccm_upside_pct)}`}>
                                      {formatPercent(coverage.model_data?.ccm_upside_pct)} upside
                                    </div>
                                  </div>
                                  <div className="metric-card metric-card-purple">
                                    <div className="metric-label">Street Target</div>
                                    <div className="metric-value">
                                      {coverage.model_data?.street_price_target ? formatPrice(coverage.model_data.street_price_target) : '-'}
                                    </div>
                                    <div className={`text-sm ${getValueClass(coverage.model_data?.street_upside_pct)}`}>
                                      {formatPercent(coverage.model_data?.street_upside_pct)} upside
                                    </div>
                                  </div>
                                  <div className="metric-card metric-card-teal">
                                    <div className="metric-label">CCM vs Street</div>
                                    <div className={`metric-value ${getValueClass(coverage.model_data?.ccm_vs_street_diff_pct)}`}>
                                      {formatPercent(coverage.model_data?.ccm_vs_street_diff_pct)}
                                    </div>
                                  </div>
                                </div>
                              </div>

                              {/* IRR */}
                              <div className="bg-white rounded-lg border border-zinc-200 p-5">
                                <h3 className="text-sm font-semibold text-zinc-900 mb-4">Return Metrics</h3>
                                <div className="grid grid-cols-2 gap-6">
                                  <div className="metric-card metric-card-green">
                                    <div className="metric-label">3-Year IRR</div>
                                    <div className="metric-value-lg text-emerald-600">
                                      {coverage.model_data?.irr_3yr != null ? `${(coverage.model_data.irr_3yr * 100).toFixed(1)}%` : '-'}
                                    </div>
                                  </div>
                                  <div>
                                    <div className="text-xs font-medium text-zinc-500 uppercase tracking-wide">Last Refreshed</div>
                                    <div className="text-sm text-zinc-700 mt-1">
                                      {coverage.model_data?.last_refreshed
                                        ? new Date(coverage.model_data.last_refreshed).toLocaleString()
                                        : 'Never'}
                                    </div>
                                  </div>
                                </div>
                              </div>

                              {/* Estimates Tables */}
                              {['revenue', 'ebitda', 'eps', 'fcf'].map((metric) => {
                                const data = coverage.model_data?.[metric as keyof ModelData] as MetricEstimates | null;
                                if (!data) return null;
                                const marginData = metric === 'ebitda' ? coverage.model_data?.ebitda_margin :
                                                   metric === 'fcf' ? coverage.model_data?.fcf_margin : null;
                                const fmt = metric === 'eps' ? formatEps : formatEstimate;

                                return (
                                  <div key={metric} className="bg-white rounded-lg border border-zinc-200 p-5">
                                    <h3 className="text-sm font-semibold text-zinc-900 mb-4 uppercase">{metric}</h3>
                                    <div className="overflow-x-auto">
                                      <table className="w-full text-sm">
                                        <thead>
                                          <tr className="text-xs text-zinc-500 uppercase tracking-wider">
                                            <th className="pb-2 text-left font-medium"></th>
                                            <th className="pb-2 text-right font-medium">-1Y</th>
                                            <th className="pb-2 text-right font-medium">1Y</th>
                                            <th className="pb-2 text-right font-medium">2Y</th>
                                            <th className="pb-2 text-right font-medium">3Y</th>
                                          </tr>
                                        </thead>
                                        <tbody className="tabular-nums">
                                          <tr className="border-t border-zinc-100">
                                            <td className="py-2 font-medium text-zinc-700">CCM</td>
                                            <td className="py-2 text-right">{fmt(data.ccm_minus1yr)}</td>
                                            <td className="py-2 text-right">{fmt(data.ccm_1yr)}</td>
                                            <td className="py-2 text-right">{fmt(data.ccm_2yr)}</td>
                                            <td className="py-2 text-right">{fmt(data.ccm_3yr)}</td>
                                          </tr>
                                          <tr className="border-t border-zinc-100">
                                            <td className="py-2 font-medium text-zinc-700">Street</td>
                                            <td className="py-2 text-right">{fmt(data.street_minus1yr)}</td>
                                            <td className="py-2 text-right">{fmt(data.street_1yr)}</td>
                                            <td className="py-2 text-right">{fmt(data.street_2yr)}</td>
                                            <td className="py-2 text-right">{fmt(data.street_3yr)}</td>
                                          </tr>
                                          <tr className="border-t border-zinc-100">
                                            <td className="py-2 font-medium text-zinc-700">Growth (CCM)</td>
                                            <td className="py-2 text-right text-zinc-400">-</td>
                                            <td className={`py-2 text-right ${getValueClass(data.growth_ccm_1yr)}`}>{formatPercent(data.growth_ccm_1yr)}</td>
                                            <td className={`py-2 text-right ${getValueClass(data.growth_ccm_2yr)}`}>{formatPercent(data.growth_ccm_2yr)}</td>
                                            <td className={`py-2 text-right ${getValueClass(data.growth_ccm_3yr)}`}>{formatPercent(data.growth_ccm_3yr)}</td>
                                          </tr>
                                          <tr className="border-t border-zinc-100">
                                            <td className="py-2 font-medium text-blue-600">CCM vs Street</td>
                                            <td className="py-2 text-right text-zinc-400">-</td>
                                            <td className={`py-2 text-right ${getValueClass(data.diff_1yr_pct)}`}>{formatPercent(data.diff_1yr_pct)}</td>
                                            <td className={`py-2 text-right ${getValueClass(data.diff_2yr_pct)}`}>{formatPercent(data.diff_2yr_pct)}</td>
                                            <td className={`py-2 text-right ${getValueClass(data.diff_3yr_pct)}`}>{formatPercent(data.diff_3yr_pct)}</td>
                                          </tr>
                                          {marginData && (
                                            <>
                                              <tr className="border-t border-zinc-100">
                                                <td className="py-2 font-medium text-violet-600">Margin (CCM)</td>
                                                <td className="py-2 text-right text-violet-600">{formatPercent(marginData.ccm_minus1yr)}</td>
                                                <td className="py-2 text-right text-violet-600">{formatPercent(marginData.ccm_1yr)}</td>
                                                <td className="py-2 text-right text-violet-600">{formatPercent(marginData.ccm_2yr)}</td>
                                                <td className="py-2 text-right text-violet-600">{formatPercent(marginData.ccm_3yr)}</td>
                                              </tr>
                                              <tr className="border-t border-dashed border-zinc-100">
                                                <td className="py-1 text-xs text-zinc-500 italic">YoY (bps)</td>
                                                <td className="py-1 text-right text-xs text-zinc-400">-</td>
                                                <td className={`py-1 text-right text-xs ${getBpsClass(marginData.ccm_1yr, marginData.ccm_minus1yr)}`}>
                                                  {formatBps(marginData.ccm_1yr, marginData.ccm_minus1yr)}
                                                </td>
                                                <td className={`py-1 text-right text-xs ${getBpsClass(marginData.ccm_2yr, marginData.ccm_1yr)}`}>
                                                  {formatBps(marginData.ccm_2yr, marginData.ccm_1yr)}
                                                </td>
                                                <td className={`py-1 text-right text-xs ${getBpsClass(marginData.ccm_3yr, marginData.ccm_2yr)}`}>
                                                  {formatBps(marginData.ccm_3yr, marginData.ccm_2yr)}
                                                </td>
                                              </tr>
                                            </>
                                          )}
                                        </tbody>
                                      </table>
                                    </div>
                                  </div>
                                );
                              })}

                              {/* Alert Section */}
                              {coverage.alert && (
                                <div className="alert alert-warning lg:col-span-2">
                                  <div className="flex items-start gap-3">
                                    <div className="flex-shrink-0 w-8 h-8 rounded-full bg-amber-200 flex items-center justify-center">
                                      <span className="text-amber-700 font-bold">!</span>
                                    </div>
                                    <div>
                                      <h3 className="font-semibold text-amber-900 mb-1">Action Required</h3>
                                      <p className="text-amber-800 whitespace-pre-wrap">{coverage.alert}</p>
                                    </div>
                                  </div>
                                </div>
                              )}

                              {/* Thesis Section - always visible, inline-editable */}
                              <div className="bg-white rounded-lg border border-zinc-200 p-5 lg:col-span-2">
                                <div className="flex justify-between items-center mb-3">
                                  <h3 className="text-sm font-semibold text-zinc-900">Investment Thesis</h3>
                                  {savingInline === coverage.id && (
                                    <span className="text-xs text-zinc-400">Saving...</span>
                                  )}
                                </div>
                                <textarea
                                  value={inlineThesis[coverage.id] ?? coverage.thesis ?? ''}
                                  onChange={(e) => setInlineThesis(prev => ({ ...prev, [coverage.id]: e.target.value }))}
                                  onBlur={() => handleInlineSave(coverage.id, 'thesis', inlineThesis[coverage.id] ?? '')}
                                  rows={3}
                                  placeholder="Write your investment thesis here..."
                                  className="w-full text-sm text-zinc-700 bg-zinc-50 border border-zinc-200 rounded-lg px-3 py-2 resize-none focus:outline-none focus:ring-2 focus:ring-blue-500/20 focus:border-blue-400 leading-relaxed"
                                  onClick={(e) => e.stopPropagation()}
                                />
                              </div>

                              {/* Bull/Bear Case - always visible, inline-editable */}
                              <div className="bg-emerald-50 rounded-lg border border-emerald-200 p-5">
                                <h3 className="text-sm font-semibold text-emerald-800 mb-3 flex items-center gap-2">
                                  <svg className="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 7h8m0 0v8m0-8l-8 8-4-4-6 6" />
                                  </svg>
                                  Bull Case
                                </h3>
                                <textarea
                                  value={inlineBull[coverage.id] ?? coverage.bull_case ?? ''}
                                  onChange={(e) => setInlineBull(prev => ({ ...prev, [coverage.id]: e.target.value }))}
                                  onBlur={() => handleInlineSave(coverage.id, 'bull_case', inlineBull[coverage.id] ?? '')}
                                  rows={3}
                                  placeholder="Key bull arguments..."
                                  className="w-full text-sm text-emerald-900 bg-white/60 border border-emerald-200 rounded-lg px-3 py-2 resize-none focus:outline-none focus:ring-2 focus:ring-emerald-500/20 focus:border-emerald-400 leading-relaxed"
                                  onClick={(e) => e.stopPropagation()}
                                />
                              </div>
                              <div className="bg-red-50 rounded-lg border border-red-200 p-5">
                                <h3 className="text-sm font-semibold text-red-800 mb-3 flex items-center gap-2">
                                  <svg className="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                                    <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M13 17h8m0 0V9m0 8l-8-8-4 4-6-6" />
                                  </svg>
                                  Bear Case
                                </h3>
                                <textarea
                                  value={inlineBear[coverage.id] ?? coverage.bear_case ?? ''}
                                  onChange={(e) => setInlineBear(prev => ({ ...prev, [coverage.id]: e.target.value }))}
                                  onBlur={() => handleInlineSave(coverage.id, 'bear_case', inlineBear[coverage.id] ?? '')}
                                  rows={3}
                                  placeholder="Key bear arguments..."
                                  className="w-full text-sm text-red-900 bg-white/60 border border-red-200 rounded-lg px-3 py-2 resize-none focus:outline-none focus:ring-2 focus:ring-red-500/20 focus:border-red-400 leading-relaxed"
                                  onClick={(e) => e.stopPropagation()}
                                />
                              </div>

                              {/* Snapshots/Version History Section */}
                              <div className="bg-white rounded-lg border border-zinc-200 p-5 lg:col-span-2">
                                <h3 className="text-sm font-semibold text-zinc-900 mb-4">Model Version History</h3>
                                {coverage.snapshots && coverage.snapshots.length > 0 ? (
                                  <ul className="space-y-2">
                                    {coverage.snapshots.slice(0, 5).map((snapshot) => (
                                      <li key={snapshot.id} className="flex justify-between items-center p-2 rounded-lg border border-zinc-100 hover:border-zinc-200 transition-colors">
                                        <div>
                                          <span className="text-sm text-zinc-700">
                                            {new Date(snapshot.created_at).toLocaleDateString()} {new Date(snapshot.created_at).toLocaleTimeString()}
                                          </span>
                                          {snapshot.ccm_fair_value && (
                                            <span className="text-zinc-500 text-sm ml-2 tabular-nums">FV: {formatDollar(snapshot.ccm_fair_value)}</span>
                                          )}
                                        </div>
                                        <div className="flex items-center gap-1">
                                          <button
                                            onClick={(e) => { e.stopPropagation(); handleViewSnapshotDiff(coverage.id, snapshot.id); }}
                                            className="btn btn-ghost btn-xs text-blue-600"
                                          >
                                            View Diff
                                          </button>
                                          <button
                                            onClick={(e) => { e.stopPropagation(); handleDeleteSnapshot(coverage.id, snapshot.id); }}
                                            className="btn btn-ghost btn-xs text-red-500"
                                          >
                                            Delete
                                          </button>
                                        </div>
                                      </li>
                                    ))}
                                  </ul>
                                ) : (
                                  <div className="empty-state py-6">
                                    <svg className="empty-state-icon h-8 w-8" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                                      <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M12 8v4l3 3m6-3a9 9 0 11-18 0 9 9 0 0118 0z" />
                                    </svg>
                                    <p className="empty-state-description mt-2">No snapshots yet. Refresh model to create a snapshot.</p>
                                  </div>
                                )}
                              </div>
                            </div>
                          </div>
                        </td>
                      </tr>
                    )}
                  </Fragment>
                ))}
                {filteredAndSortedCoverages.length === 0 && (
                  <tr>
                    <td colSpan={12}>
                      <div className="empty-state">
                        <svg className="empty-state-icon" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 19v-6a2 2 0 00-2-2H5a2 2 0 00-2 2v6a2 2 0 002 2h2a2 2 0 002-2zm0 0V9a2 2 0 012-2h2a2 2 0 012 2v10m-6 0a2 2 0 002 2h2a2 2 0 002-2m0 0V5a2 2 0 012-2h2a2 2 0 012 2v14a2 2 0 01-2 2h-2a2 2 0 01-2-2z" />
                        </svg>
                        <p className="empty-state-title">
                          {coverages.length === 0 ? 'No tickers in coverage' : 'No tickers match filters'}
                        </p>
                        <p className="empty-state-description">
                          {coverages.length === 0 ? 'Add a ticker above to get started.' : 'Try adjusting your analyst filters.'}
                        </p>
                      </div>
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>

        {/* Firm Summary */}
        <div className="card">
          <div className="flex justify-between items-center">
            <span className="text-zinc-600 font-medium">Total Firm Portfolio Value</span>
            <span className="text-2xl font-bold text-zinc-900 tabular-nums">{formatDollar(totalFirmValue)}</span>
          </div>
        </div>

        {/* Edit Modal */}
        {editingCoverage && (
          <div className="modal-backdrop" onClick={() => setEditingCoverage(null)}>
            <div className="modal w-full max-w-2xl max-h-[90vh]" onClick={(e) => e.stopPropagation()}>
              <div className="modal-header">
                <h2 className="text-lg font-semibold text-zinc-900">Edit {editingCoverage.ticker}</h2>
              </div>
              <div className="modal-body overflow-y-auto space-y-5">
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="label">Primary Analyst</label>
                    <select
                      value={editPrimaryAnalyst || ''}
                      onChange={(e) => setEditPrimaryAnalyst(e.target.value ? Number(e.target.value) : null)}
                      className="select"
                    >
                      <option value="">Select...</option>
                      {analysts.map((a) => (
                        <option key={a.id} value={a.id}>{a.name}</option>
                      ))}
                    </select>
                  </div>
                  <div>
                    <label className="label">Secondary Analyst</label>
                    <select
                      value={editSecondaryAnalyst || ''}
                      onChange={(e) => setEditSecondaryAnalyst(e.target.value ? Number(e.target.value) : null)}
                      className="select"
                    >
                      <option value="">Select...</option>
                      {analysts.map((a) => (
                        <option key={a.id} value={a.id}>{a.name}</option>
                      ))}
                    </select>
                  </div>
                </div>
                <div>
                  <label className="label">Model Path</label>
                  <input
                    type="text"
                    value={editModelPath}
                    onChange={(e) => setEditModelPath(e.target.value)}
                    placeholder="/models/TICKER/TICKER Model.xlsx"
                    className="input"
                  />
                  <p className="text-xs text-zinc-500 mt-1.5">
                    Path to the Excel model file (e.g., /models/LLY/LLY Model.xlsx)
                  </p>
                </div>
                <div>
                  <label className="label">Share Link (Optional)</label>
                  <input
                    type="text"
                    value={editModelShareLink}
                    onChange={(e) => setEditModelShareLink(e.target.value)}
                    placeholder="https://..."
                    className="input"
                  />
                  <p className="text-xs text-zinc-500 mt-1.5">
                    OneDrive share link for opening the model in browser
                  </p>
                </div>
                <div>
                  <label className="label">Investment Thesis</label>
                  <textarea
                    value={editThesis}
                    onChange={(e) => setEditThesis(e.target.value)}
                    rows={3}
                    placeholder="Investment thesis..."
                    className="input resize-none"
                  />
                </div>
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="label text-emerald-700">Bull Case</label>
                    <textarea
                      value={editBullCase}
                      onChange={(e) => setEditBullCase(e.target.value)}
                      rows={3}
                      placeholder="Key bull arguments..."
                      className="input resize-none border-emerald-200 focus:border-emerald-500 focus:ring-emerald-500/20"
                    />
                  </div>
                  <div>
                    <label className="label text-red-700">Bear Case</label>
                    <textarea
                      value={editBearCase}
                      onChange={(e) => setEditBearCase(e.target.value)}
                      rows={3}
                      placeholder="Key bear arguments..."
                      className="input resize-none border-red-200 focus:border-red-500 focus:ring-red-500/20"
                    />
                  </div>
                </div>
                <div>
                  <label className="label text-amber-700">Alert / Action Item</label>
                  <textarea
                    value={editAlert}
                    onChange={(e) => setEditAlert(e.target.value)}
                    rows={2}
                    placeholder="Action items or alerts (will show warning icon if populated)..."
                    className="input resize-none border-amber-200 focus:border-amber-500 focus:ring-amber-500/20"
                  />
                </div>
              </div>
              <div className="modal-footer">
                <button
                  onClick={() => setEditingCoverage(null)}
                  className="btn btn-secondary"
                >
                  Cancel
                </button>
                <button
                  onClick={handleUpdateCoverage}
                  className="btn btn-primary"
                >
                  Save Changes
                </button>
              </div>
            </div>
          </div>
        )}

        {/* Snapshot Diff Modal */}
        {viewingSnapshotDiff && snapshotDiff && (
          <div className="modal-backdrop" onClick={() => { setViewingSnapshotDiff(null); setSnapshotDiff(null); }}>
            <div className="modal w-full max-w-2xl max-h-[80vh]" onClick={(e) => e.stopPropagation()}>
              <div className="modal-header">
                <h2 className="text-lg font-semibold text-zinc-900">
                  Model Changes Since {new Date(snapshotDiff.snapshot_date).toLocaleDateString()}
                </h2>
              </div>
              <div className="modal-body overflow-y-auto">
                <table className="table">
                  <thead>
                    <tr>
                      <th>Metric</th>
                      <th className="text-right">Previous</th>
                      <th className="text-right">Current</th>
                      <th className="text-right">Change</th>
                      <th className="text-right">% Change</th>
                    </tr>
                  </thead>
                  <tbody className="tabular-nums">
                    {snapshotDiff.diffs.map((diff: any, index: number) => (
                      <tr key={index} className={diff.change ? (diff.change > 0 ? 'bg-emerald-50/50' : 'bg-red-50/50') : ''}>
                        <td className="font-medium text-zinc-900">{diff.field}</td>
                        <td className="text-right">{diff.old_value?.toFixed(2) ?? '-'}</td>
                        <td className="text-right">{diff.new_value?.toFixed(2) ?? '-'}</td>
                        <td className={`text-right ${diff.change > 0 ? 'value-positive' : diff.change < 0 ? 'value-negative' : ''}`}>
                          {diff.change != null ? (diff.change > 0 ? '+' : '') + diff.change.toFixed(2) : '-'}
                        </td>
                        <td className={`text-right ${diff.change_pct > 0 ? 'value-positive' : diff.change_pct < 0 ? 'value-negative' : ''}`}>
                          {diff.change_pct != null ? (diff.change_pct > 0 ? '+' : '') + diff.change_pct.toFixed(1) + '%' : '-'}
                        </td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              </div>
              <div className="modal-footer">
                <button
                  onClick={() => { setViewingSnapshotDiff(null); setSnapshotDiff(null); }}
                  className="btn btn-secondary"
                >
                  Close
                </button>
              </div>
            </div>
          </div>
        )}
      </div>
    </Layout>
  );
}
