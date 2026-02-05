import { useState, useEffect, Fragment } from 'react';
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

interface CoverageDocument {
  id: number;
  coverage_id: number;
  file_name: string;
  file_path: string;
  file_type: string | null;
  file_size: number | null;
  description: string | null;
  uploaded_at: string;
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
  documents: CoverageDocument[];
  snapshots: CoverageSnapshot[];
  created_at: string;
  updated_at: string;
}

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

  // Document modal
  const [addingDocument, setAddingDocument] = useState<number | null>(null);
  const [newDocFileName, setNewDocFileName] = useState('');
  const [newDocFilePath, setNewDocFilePath] = useState('');
  const [newDocDescription, setNewDocDescription] = useState('');

  // Snapshot diff view
  const [viewingSnapshotDiff, setViewingSnapshotDiff] = useState<{ coverageId: number; snapshotId: number } | null>(null);
  const [snapshotDiff, setSnapshotDiff] = useState<any>(null);

  // Expanded detail view
  const [expandedTicker, setExpandedTicker] = useState<number | null>(null);

  // Refresh status
  const [refreshing, setRefreshing] = useState<number | null>(null);
  const [refreshingAll, setRefreshingAll] = useState(false);

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

      // Reset form
      setNewTicker('');
      setNewPrimaryAnalyst(null);
      setNewSecondaryAnalyst(null);
      setNewModelPath('');
      setNewModelShareLink('');

      // Reload data
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

  const handleAddDocument = async (coverageId: number) => {
    if (!newDocFileName.trim() || !newDocFilePath.trim()) {
      setError('File name and path are required');
      return;
    }
    try {
      await api.addCoverageDocument(coverageId, {
        file_name: newDocFileName,
        file_path: newDocFilePath,
        description: newDocDescription || undefined,
      });
      setAddingDocument(null);
      setNewDocFileName('');
      setNewDocFilePath('');
      setNewDocDescription('');
      await loadData();
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to add document');
    }
  };

  const handleDeleteDocument = async (coverageId: number, documentId: number) => {
    if (!confirm('Delete this document?')) return;
    try {
      await api.deleteCoverageDocument(coverageId, documentId);
      await loadData();
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to delete document');
    }
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

  const formatWeight = (value: number | null | undefined) => {
    if (value == null) return '-';
    return `${value.toFixed(2)}%`;
  };

  const formatNumber = (value: number | null | undefined, decimals: number = 2) => {
    if (value == null) return '-';
    return value.toFixed(decimals);
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
            <h1 className="text-2xl font-bold text-gray-900">Active Coverage</h1>
            <p className="text-sm text-gray-500 mt-1">
              Track analyst coverage with linked Excel models
            </p>
          </div>
          <div className="flex gap-2">
            <button
              onClick={handleRefreshAllModels}
              disabled={refreshingAll}
              className="px-4 py-2 bg-gray-100 text-gray-700 rounded hover:bg-gray-200 disabled:opacity-50"
            >
              {refreshingAll ? 'Refreshing...' : 'Refresh All Models'}
            </button>
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

        {/* Add Ticker Form */}
        <div className="card p-4">
          <h2 className="text-lg font-semibold mb-4">Add Ticker to Coverage</h2>
          <div className="grid grid-cols-1 md:grid-cols-6 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Ticker</label>
              <input
                type="text"
                value={newTicker}
                onChange={(e) => setNewTicker(e.target.value.toUpperCase())}
                placeholder="AAPL"
                className="w-full px-3 py-2 border rounded focus:ring-2 focus:ring-blue-500"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Primary Analyst</label>
              <select
                value={newPrimaryAnalyst || ''}
                onChange={(e) => setNewPrimaryAnalyst(e.target.value ? Number(e.target.value) : null)}
                className="w-full px-3 py-2 border rounded focus:ring-2 focus:ring-blue-500"
              >
                <option value="">Select...</option>
                {analysts.map((a) => (
                  <option key={a.id} value={a.id}>{a.name}</option>
                ))}
              </select>
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-1">Secondary Analyst</label>
              <select
                value={newSecondaryAnalyst || ''}
                onChange={(e) => setNewSecondaryAnalyst(e.target.value ? Number(e.target.value) : null)}
                className="w-full px-3 py-2 border rounded focus:ring-2 focus:ring-blue-500"
              >
                <option value="">Select...</option>
                {analysts.map((a) => (
                  <option key={a.id} value={a.id}>{a.name}</option>
                ))}
              </select>
            </div>
            <div className="md:col-span-2">
              <label className="block text-sm font-medium text-gray-700 mb-1">Model Path</label>
              <input
                type="text"
                value={newModelPath}
                onChange={(e) => setNewModelPath(e.target.value)}
                placeholder="/models/AAPL/AAPL Model.xlsx"
                className="w-full px-3 py-2 border rounded focus:ring-2 focus:ring-blue-500"
              />
            </div>
            <div className="flex items-end">
              <button
                onClick={handleAddTicker}
                disabled={adding || !newTicker.trim()}
                className="w-full px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50"
              >
                {adding ? 'Adding...' : 'Add Ticker'}
              </button>
            </div>
          </div>
        </div>

        {/* Coverage Table */}
        <div className="card overflow-hidden">
          <div className="overflow-x-auto">
            <table className="min-w-full divide-y divide-gray-200">
              <thead className="bg-gray-50">
                <tr>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Ticker</th>
                  <th className="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase">Updated</th>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Primary</th>
                  <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Secondary</th>
                  <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Price</th>
                  <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Mkt Value</th>
                  <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Weight</th>
                  <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">CCM FV</th>
                  <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Upside</th>
                  <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">3Y IRR</th>
                  <th className="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase">Model</th>
                  <th className="px-4 py-3 text-center text-xs font-medium text-gray-500 uppercase">Actions</th>
                </tr>
              </thead>
              <tbody className="bg-white divide-y divide-gray-200">
                {coverages.map((coverage) => (
                  <Fragment key={coverage.id}>
                    <tr
                      className={`hover:bg-gray-50 cursor-pointer ${expandedTicker === coverage.id ? 'bg-blue-50' : ''}`}
                      onClick={() => setExpandedTicker(expandedTicker === coverage.id ? null : coverage.id)}
                    >
                      <td className="px-4 py-3 font-medium text-gray-900">
                        <div className="flex items-center gap-2">
                          {coverage.has_alert && (
                            <span className="text-orange-500" title={coverage.alert || 'Action required'}>
                              ⚠️
                            </span>
                          )}
                          {coverage.ticker}
                        </div>
                      </td>
                      <td className="px-4 py-3 text-center">
                        <button
                          onClick={(e) => { e.stopPropagation(); handleToggleModelUpdated(coverage.id, coverage.model_updated); }}
                          className={`px-2 py-1 rounded text-xs font-medium ${
                            coverage.model_updated
                              ? 'bg-green-100 text-green-800 hover:bg-green-200'
                              : 'bg-red-100 text-red-800 hover:bg-red-200'
                          }`}
                        >
                          {coverage.model_updated ? 'Updated' : 'Not Updated'}
                        </button>
                      </td>
                      <td className="px-4 py-3 text-gray-600">{coverage.primary_analyst?.name || '-'}</td>
                      <td className="px-4 py-3 text-gray-600">{coverage.secondary_analyst?.name || '-'}</td>
                      <td className="px-4 py-3 text-right text-gray-900">
                        {coverage.current_price ? `$${coverage.current_price.toFixed(2)}` : '-'}
                      </td>
                      <td className="px-4 py-3 text-right text-gray-900">{formatCurrency(coverage.market_value)}</td>
                      <td className="px-4 py-3 text-right text-gray-900">{formatWeight(coverage.weight_pct)}</td>
                      <td className="px-4 py-3 text-right text-gray-900">
                        {coverage.model_data?.ccm_fair_value ? `$${coverage.model_data.ccm_fair_value.toFixed(2)}` : '-'}
                      </td>
                      <td className={`px-4 py-3 text-right ${
                        coverage.model_data?.ccm_upside_pct != null
                          ? coverage.model_data.ccm_upside_pct >= 0 ? 'text-green-600' : 'text-red-600'
                          : 'text-gray-400'
                      }`}>
                        {formatPercent(coverage.model_data?.ccm_upside_pct)}
                      </td>
                      <td className="px-4 py-3 text-right text-gray-900">
                        {coverage.model_data?.irr_3yr != null ? `${(coverage.model_data.irr_3yr * 100).toFixed(1)}%` : '-'}
                      </td>
                      <td className="px-4 py-3 text-center">
                        {coverage.model_share_link ? (
                          <a
                            href={coverage.model_share_link}
                            target="_blank"
                            rel="noopener noreferrer"
                            onClick={(e) => e.stopPropagation()}
                            className="text-blue-600 hover:text-blue-800"
                          >
                            Open
                          </a>
                        ) : coverage.model_path ? (
                          <span className="text-gray-400 text-sm">Local</span>
                        ) : (
                          <span className="text-gray-400">-</span>
                        )}
                      </td>
                      <td className="px-4 py-3 text-center space-x-2">
                        <button
                          onClick={(e) => { e.stopPropagation(); openEditModal(coverage); }}
                          className="text-blue-600 hover:text-blue-800"
                        >
                          Edit
                        </button>
                        {coverage.model_path && (
                          <button
                            onClick={(e) => { e.stopPropagation(); handleRefreshModel(coverage.id); }}
                            disabled={refreshing === coverage.id}
                            className="text-green-600 hover:text-green-800 disabled:opacity-50"
                          >
                            {refreshing === coverage.id ? '...' : 'Refresh'}
                          </button>
                        )}
                        <button
                          onClick={(e) => { e.stopPropagation(); handleDeleteCoverage(coverage.id); }}
                          className="text-red-600 hover:text-red-800"
                        >
                          Remove
                        </button>
                      </td>
                    </tr>
                    {/* Expanded Detail Row */}
                    {expandedTicker === coverage.id && (
                      <tr>
                        <td colSpan={12} className="px-4 py-4 bg-gray-50">
                          <div className="grid grid-cols-1 lg:grid-cols-2 gap-6">
                            {/* Valuation Summary */}
                            <div className="bg-white p-4 rounded border">
                              <h3 className="font-semibold text-gray-900 mb-3">Valuation</h3>
                              <div className="grid grid-cols-3 gap-4 text-sm">
                                <div>
                                  <div className="text-gray-500">CCM Fair Value</div>
                                  <div className="font-medium">
                                    {coverage.model_data?.ccm_fair_value ? `$${coverage.model_data.ccm_fair_value.toFixed(2)}` : '-'}
                                  </div>
                                  <div className={coverage.model_data?.ccm_upside_pct != null && coverage.model_data.ccm_upside_pct >= 0 ? 'text-green-600' : 'text-red-600'}>
                                    {formatPercent(coverage.model_data?.ccm_upside_pct)} upside
                                  </div>
                                </div>
                                <div>
                                  <div className="text-gray-500">Street Target</div>
                                  <div className="font-medium">
                                    {coverage.model_data?.street_price_target ? `$${coverage.model_data.street_price_target.toFixed(2)}` : '-'}
                                  </div>
                                  <div className={coverage.model_data?.street_upside_pct != null && coverage.model_data.street_upside_pct >= 0 ? 'text-green-600' : 'text-red-600'}>
                                    {formatPercent(coverage.model_data?.street_upside_pct)} upside
                                  </div>
                                </div>
                                <div>
                                  <div className="text-gray-500">CCM vs Street</div>
                                  <div className={`font-medium ${coverage.model_data?.ccm_vs_street_diff_pct != null && coverage.model_data.ccm_vs_street_diff_pct >= 0 ? 'text-green-600' : 'text-red-600'}`}>
                                    {formatPercent(coverage.model_data?.ccm_vs_street_diff_pct)}
                                  </div>
                                </div>
                              </div>
                            </div>

                            {/* IRR */}
                            <div className="bg-white p-4 rounded border">
                              <h3 className="font-semibold text-gray-900 mb-3">Return Metrics</h3>
                              <div className="grid grid-cols-2 gap-4 text-sm">
                                <div>
                                  <div className="text-gray-500">3-Year IRR</div>
                                  <div className="text-2xl font-bold text-blue-600">
                                    {coverage.model_data?.irr_3yr != null ? `${(coverage.model_data.irr_3yr * 100).toFixed(1)}%` : '-'}
                                  </div>
                                </div>
                                <div>
                                  <div className="text-gray-500">Last Refreshed</div>
                                  <div className="font-medium">
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

                              return (
                                <div key={metric} className="bg-white p-4 rounded border">
                                  <h3 className="font-semibold text-gray-900 mb-3 capitalize">{metric}</h3>
                                  <table className="w-full text-sm">
                                    <thead>
                                      <tr className="text-gray-500 text-left">
                                        <th className="pb-2"></th>
                                        <th className="pb-2 text-right">-1Y</th>
                                        <th className="pb-2 text-right">1Y</th>
                                        <th className="pb-2 text-right">2Y</th>
                                        <th className="pb-2 text-right">3Y</th>
                                      </tr>
                                    </thead>
                                    <tbody>
                                      <tr>
                                        <td className="py-1 font-medium">CCM</td>
                                        <td className="py-1 text-right">{formatNumber(data.ccm_minus1yr)}</td>
                                        <td className="py-1 text-right">{formatNumber(data.ccm_1yr)}</td>
                                        <td className="py-1 text-right">{formatNumber(data.ccm_2yr)}</td>
                                        <td className="py-1 text-right">{formatNumber(data.ccm_3yr)}</td>
                                      </tr>
                                      <tr>
                                        <td className="py-1 font-medium">Street</td>
                                        <td className="py-1 text-right">{formatNumber(data.street_minus1yr)}</td>
                                        <td className="py-1 text-right">{formatNumber(data.street_1yr)}</td>
                                        <td className="py-1 text-right">{formatNumber(data.street_2yr)}</td>
                                        <td className="py-1 text-right">{formatNumber(data.street_3yr)}</td>
                                      </tr>
                                      <tr className="text-green-600">
                                        <td className="py-1 font-medium">Growth (CCM)</td>
                                        <td className="py-1 text-right">-</td>
                                        <td className="py-1 text-right">{formatPercent(data.growth_ccm_1yr)}</td>
                                        <td className="py-1 text-right">{formatPercent(data.growth_ccm_2yr)}</td>
                                        <td className="py-1 text-right">{formatPercent(data.growth_ccm_3yr)}</td>
                                      </tr>
                                      <tr className="text-blue-600">
                                        <td className="py-1 font-medium">CCM vs Street</td>
                                        <td className="py-1 text-right">-</td>
                                        <td className="py-1 text-right">{formatPercent(data.diff_1yr_pct)}</td>
                                        <td className="py-1 text-right">{formatPercent(data.diff_2yr_pct)}</td>
                                        <td className="py-1 text-right">{formatPercent(data.diff_3yr_pct)}</td>
                                      </tr>
                                      {marginData && (
                                        <tr className="text-purple-600">
                                          <td className="py-1 font-medium">Margin (CCM)</td>
                                          <td className="py-1 text-right">{formatPercent(marginData.ccm_minus1yr)}</td>
                                          <td className="py-1 text-right">{formatPercent(marginData.ccm_1yr)}</td>
                                          <td className="py-1 text-right">{formatPercent(marginData.ccm_2yr)}</td>
                                          <td className="py-1 text-right">{formatPercent(marginData.ccm_3yr)}</td>
                                        </tr>
                                      )}
                                    </tbody>
                                  </table>
                                </div>
                              );
                            })}

                            {/* Alert Section */}
                            {coverage.alert && (
                              <div className="bg-orange-50 p-4 rounded border border-orange-200 lg:col-span-2">
                                <h3 className="font-semibold text-orange-900 mb-2 flex items-center gap-2">
                                  <span>⚠️</span> Action Required
                                </h3>
                                <p className="text-orange-800 whitespace-pre-wrap">{coverage.alert}</p>
                              </div>
                            )}

                            {/* Thesis Section */}
                            {coverage.thesis && (
                              <div className="bg-white p-4 rounded border lg:col-span-2">
                                <h3 className="font-semibold text-gray-900 mb-2">Investment Thesis</h3>
                                <p className="text-gray-700 whitespace-pre-wrap">{coverage.thesis}</p>
                              </div>
                            )}

                            {/* Bull/Bear Case */}
                            {(coverage.bull_case || coverage.bear_case) && (
                              <div className="bg-white p-4 rounded border">
                                <h3 className="font-semibold text-green-700 mb-2">Bull Case</h3>
                                <p className="text-gray-700 whitespace-pre-wrap">{coverage.bull_case || 'Not specified'}</p>
                              </div>
                            )}
                            {(coverage.bull_case || coverage.bear_case) && (
                              <div className="bg-white p-4 rounded border">
                                <h3 className="font-semibold text-red-700 mb-2">Bear Case</h3>
                                <p className="text-gray-700 whitespace-pre-wrap">{coverage.bear_case || 'Not specified'}</p>
                              </div>
                            )}

                            {/* Documents Section */}
                            <div className="bg-white p-4 rounded border">
                              <div className="flex justify-between items-center mb-3">
                                <h3 className="font-semibold text-gray-900">Documents</h3>
                                <button
                                  onClick={(e) => { e.stopPropagation(); setAddingDocument(coverage.id); }}
                                  className="text-blue-600 hover:text-blue-800 text-sm"
                                >
                                  + Add Document
                                </button>
                              </div>
                              {coverage.documents && coverage.documents.length > 0 ? (
                                <ul className="space-y-2">
                                  {coverage.documents.map((doc) => (
                                    <li key={doc.id} className="flex justify-between items-center text-sm">
                                      <div>
                                        <span className="font-medium">{doc.file_name}</span>
                                        {doc.description && <span className="text-gray-500 ml-2">- {doc.description}</span>}
                                      </div>
                                      <button
                                        onClick={(e) => { e.stopPropagation(); handleDeleteDocument(coverage.id, doc.id); }}
                                        className="text-red-500 hover:text-red-700"
                                      >
                                        Delete
                                      </button>
                                    </li>
                                  ))}
                                </ul>
                              ) : (
                                <p className="text-gray-500 text-sm">No documents uploaded</p>
                              )}
                            </div>

                            {/* Snapshots/Version History Section */}
                            <div className="bg-white p-4 rounded border">
                              <h3 className="font-semibold text-gray-900 mb-3">Model Version History</h3>
                              {coverage.snapshots && coverage.snapshots.length > 0 ? (
                                <ul className="space-y-2">
                                  {coverage.snapshots.slice(0, 5).map((snapshot) => (
                                    <li key={snapshot.id} className="flex justify-between items-center text-sm border-b pb-2">
                                      <div>
                                        <span className="text-gray-600">
                                          {new Date(snapshot.created_at).toLocaleDateString()} {new Date(snapshot.created_at).toLocaleTimeString()}
                                        </span>
                                        {snapshot.ccm_fair_value && (
                                          <span className="text-gray-500 ml-2">FV: ${snapshot.ccm_fair_value.toFixed(2)}</span>
                                        )}
                                      </div>
                                      <div className="space-x-2">
                                        <button
                                          onClick={(e) => { e.stopPropagation(); handleViewSnapshotDiff(coverage.id, snapshot.id); }}
                                          className="text-blue-600 hover:text-blue-800"
                                        >
                                          View Diff
                                        </button>
                                        <button
                                          onClick={(e) => { e.stopPropagation(); handleDeleteSnapshot(coverage.id, snapshot.id); }}
                                          className="text-red-500 hover:text-red-700"
                                        >
                                          Delete
                                        </button>
                                      </div>
                                    </li>
                                  ))}
                                </ul>
                              ) : (
                                <p className="text-gray-500 text-sm">No snapshots yet. Refresh model to create a snapshot.</p>
                              )}
                            </div>
                          </div>
                        </td>
                      </tr>
                    )}
                  </Fragment>
                ))}
                {coverages.length === 0 && (
                  <tr>
                    <td colSpan={12} className="px-4 py-8 text-center text-gray-500">
                      No tickers in coverage. Add a ticker above to get started.
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>

        {/* Firm Summary */}
        <div className="card p-4">
          <div className="flex justify-between items-center">
            <span className="text-gray-600">Total Firm Portfolio Value:</span>
            <span className="text-xl font-bold">{formatCurrency(totalFirmValue)}</span>
          </div>
        </div>

        {/* Edit Modal */}
        {editingCoverage && (
          <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
            <div className="bg-white rounded-lg p-6 w-full max-w-2xl max-h-[90vh] overflow-y-auto">
              <h2 className="text-xl font-bold mb-4">Edit {editingCoverage.ticker}</h2>
              <div className="space-y-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Primary Analyst</label>
                  <select
                    value={editPrimaryAnalyst || ''}
                    onChange={(e) => setEditPrimaryAnalyst(e.target.value ? Number(e.target.value) : null)}
                    className="w-full px-3 py-2 border rounded focus:ring-2 focus:ring-blue-500"
                  >
                    <option value="">Select...</option>
                    {analysts.map((a) => (
                      <option key={a.id} value={a.id}>{a.name}</option>
                    ))}
                  </select>
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Secondary Analyst</label>
                  <select
                    value={editSecondaryAnalyst || ''}
                    onChange={(e) => setEditSecondaryAnalyst(e.target.value ? Number(e.target.value) : null)}
                    className="w-full px-3 py-2 border rounded focus:ring-2 focus:ring-blue-500"
                  >
                    <option value="">Select...</option>
                    {analysts.map((a) => (
                      <option key={a.id} value={a.id}>{a.name}</option>
                    ))}
                  </select>
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Model Path</label>
                  <input
                    type="text"
                    value={editModelPath}
                    onChange={(e) => setEditModelPath(e.target.value)}
                    placeholder="/models/TICKER/TICKER Model.xlsx"
                    className="w-full px-3 py-2 border rounded focus:ring-2 focus:ring-blue-500"
                  />
                  <p className="text-xs text-gray-500 mt-1">
                    Path to the Excel model file (e.g., /models/LLY/LLY Model.xlsx)
                  </p>
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Share Link (Optional)</label>
                  <input
                    type="text"
                    value={editModelShareLink}
                    onChange={(e) => setEditModelShareLink(e.target.value)}
                    placeholder="https://..."
                    className="w-full px-3 py-2 border rounded focus:ring-2 focus:ring-blue-500"
                  />
                  <p className="text-xs text-gray-500 mt-1">
                    OneDrive share link for opening the model in browser
                  </p>
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Investment Thesis</label>
                  <textarea
                    value={editThesis}
                    onChange={(e) => setEditThesis(e.target.value)}
                    rows={3}
                    placeholder="Investment thesis..."
                    className="w-full px-3 py-2 border rounded focus:ring-2 focus:ring-blue-500"
                  />
                </div>
                <div className="grid grid-cols-2 gap-4">
                  <div>
                    <label className="block text-sm font-medium text-green-700 mb-1">Bull Case</label>
                    <textarea
                      value={editBullCase}
                      onChange={(e) => setEditBullCase(e.target.value)}
                      rows={3}
                      placeholder="Key bull arguments..."
                      className="w-full px-3 py-2 border rounded focus:ring-2 focus:ring-green-500"
                    />
                  </div>
                  <div>
                    <label className="block text-sm font-medium text-red-700 mb-1">Bear Case</label>
                    <textarea
                      value={editBearCase}
                      onChange={(e) => setEditBearCase(e.target.value)}
                      rows={3}
                      placeholder="Key bear arguments..."
                      className="w-full px-3 py-2 border rounded focus:ring-2 focus:ring-red-500"
                    />
                  </div>
                </div>
                <div>
                  <label className="block text-sm font-medium text-orange-700 mb-1">Alert / Action Item</label>
                  <textarea
                    value={editAlert}
                    onChange={(e) => setEditAlert(e.target.value)}
                    rows={2}
                    placeholder="Action items or alerts (will show warning icon if populated)..."
                    className="w-full px-3 py-2 border border-orange-200 rounded focus:ring-2 focus:ring-orange-500"
                  />
                </div>
              </div>
              <div className="flex justify-end gap-3 mt-6">
                <button
                  onClick={() => setEditingCoverage(null)}
                  className="px-4 py-2 text-gray-600 hover:text-gray-800"
                >
                  Cancel
                </button>
                <button
                  onClick={handleUpdateCoverage}
                  className="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700"
                >
                  Save Changes
                </button>
              </div>
            </div>
          </div>
        )}

        {/* Add Document Modal */}
        {addingDocument && (
          <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
            <div className="bg-white rounded-lg p-6 w-full max-w-md">
              <h2 className="text-xl font-bold mb-4">Add Document</h2>
              <div className="space-y-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">File Name</label>
                  <input
                    type="text"
                    value={newDocFileName}
                    onChange={(e) => setNewDocFileName(e.target.value)}
                    placeholder="Earnings Report Q1 2024.pdf"
                    className="w-full px-3 py-2 border rounded focus:ring-2 focus:ring-blue-500"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">File Path / URL</label>
                  <input
                    type="text"
                    value={newDocFilePath}
                    onChange={(e) => setNewDocFilePath(e.target.value)}
                    placeholder="/documents/AAPL/report.pdf or https://..."
                    className="w-full px-3 py-2 border rounded focus:ring-2 focus:ring-blue-500"
                  />
                </div>
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Description (Optional)</label>
                  <input
                    type="text"
                    value={newDocDescription}
                    onChange={(e) => setNewDocDescription(e.target.value)}
                    placeholder="Brief description..."
                    className="w-full px-3 py-2 border rounded focus:ring-2 focus:ring-blue-500"
                  />
                </div>
              </div>
              <div className="flex justify-end gap-3 mt-6">
                <button
                  onClick={() => { setAddingDocument(null); setNewDocFileName(''); setNewDocFilePath(''); setNewDocDescription(''); }}
                  className="px-4 py-2 text-gray-600 hover:text-gray-800"
                >
                  Cancel
                </button>
                <button
                  onClick={() => handleAddDocument(addingDocument)}
                  className="px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700"
                >
                  Add Document
                </button>
              </div>
            </div>
          </div>
        )}

        {/* Snapshot Diff Modal */}
        {viewingSnapshotDiff && snapshotDiff && (
          <div className="fixed inset-0 bg-black bg-opacity-50 flex items-center justify-center z-50">
            <div className="bg-white rounded-lg p-6 w-full max-w-2xl max-h-[80vh] overflow-y-auto">
              <h2 className="text-xl font-bold mb-4">Model Changes Since {new Date(snapshotDiff.snapshot_date).toLocaleDateString()}</h2>
              <table className="w-full text-sm">
                <thead className="bg-gray-50">
                  <tr>
                    <th className="px-4 py-2 text-left">Metric</th>
                    <th className="px-4 py-2 text-right">Previous</th>
                    <th className="px-4 py-2 text-right">Current</th>
                    <th className="px-4 py-2 text-right">Change</th>
                    <th className="px-4 py-2 text-right">% Change</th>
                  </tr>
                </thead>
                <tbody>
                  {snapshotDiff.diffs.map((diff: any, index: number) => (
                    <tr key={index} className={diff.change ? (diff.change > 0 ? 'bg-green-50' : 'bg-red-50') : ''}>
                      <td className="px-4 py-2 font-medium">{diff.field}</td>
                      <td className="px-4 py-2 text-right">{diff.old_value?.toFixed(2) ?? '-'}</td>
                      <td className="px-4 py-2 text-right">{diff.new_value?.toFixed(2) ?? '-'}</td>
                      <td className={`px-4 py-2 text-right ${diff.change > 0 ? 'text-green-600' : diff.change < 0 ? 'text-red-600' : ''}`}>
                        {diff.change != null ? (diff.change > 0 ? '+' : '') + diff.change.toFixed(2) : '-'}
                      </td>
                      <td className={`px-4 py-2 text-right ${diff.change_pct > 0 ? 'text-green-600' : diff.change_pct < 0 ? 'text-red-600' : ''}`}>
                        {diff.change_pct != null ? (diff.change_pct > 0 ? '+' : '') + diff.change_pct.toFixed(1) + '%' : '-'}
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
              <div className="flex justify-end mt-6">
                <button
                  onClick={() => { setViewingSnapshotDiff(null); setSnapshotDiff(null); }}
                  className="px-4 py-2 bg-gray-200 text-gray-700 rounded hover:bg-gray-300"
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
