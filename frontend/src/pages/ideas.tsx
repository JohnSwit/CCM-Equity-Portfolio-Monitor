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

interface IdeaDocument {
  id: number;
  idea_id: number;
  filename: string;
  original_filename: string;
  file_type: string | null;
  file_size: number | null;
  uploaded_at: string;
}

interface Idea {
  id: number;
  ticker: string;
  primary_analyst: Analyst | null;
  secondary_analyst: Analyst | null;
  model_path: string | null;
  model_share_link: string | null;
  initial_review_complete: boolean;
  deep_dive_complete: boolean;
  model_complete: boolean;
  writeup_complete: boolean;
  thesis: string | null;
  next_steps: string | null;
  notes: string | null;
  is_active: boolean;
  model_data: ModelData | null;
  documents: IdeaDocument[];
  created_at: string;
  updated_at: string;
}

export default function IdeasPage() {
  const router = useRouter();
  const { user, loading: authLoading } = useAuth();
  const [ideas, setIdeas] = useState<Idea[]>([]);
  const [analysts, setAnalysts] = useState<Analyst[]>([]);
  const [loading, setLoading] = useState(true);
  const [error, setError] = useState<string | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);

  // Add idea form
  const [newTicker, setNewTicker] = useState('');
  const [newPrimaryAnalyst, setNewPrimaryAnalyst] = useState<number | null>(null);
  const [newSecondaryAnalyst, setNewSecondaryAnalyst] = useState<number | null>(null);
  const [newModelPath, setNewModelPath] = useState('');
  const [adding, setAdding] = useState(false);

  // Edit modal
  const [editingIdea, setEditingIdea] = useState<Idea | null>(null);
  const [editPrimaryAnalyst, setEditPrimaryAnalyst] = useState<number | null>(null);
  const [editSecondaryAnalyst, setEditSecondaryAnalyst] = useState<number | null>(null);
  const [editModelPath, setEditModelPath] = useState('');
  const [editModelShareLink, setEditModelShareLink] = useState('');
  const [editThesis, setEditThesis] = useState('');
  const [editNextSteps, setEditNextSteps] = useState('');
  const [editNotes, setEditNotes] = useState('');
  const [editInitialReview, setEditInitialReview] = useState(false);
  const [editDeepDive, setEditDeepDive] = useState(false);
  const [editModel, setEditModel] = useState(false);
  const [editWriteup, setEditWriteup] = useState(false);

  // Expanded detail view
  const [expandedTicker, setExpandedTicker] = useState<number | null>(null);

  // Refresh and upload status
  const [refreshing, setRefreshing] = useState<number | null>(null);
  const [uploading, setUploading] = useState<number | null>(null);

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

      const [analystData, ideasData] = await Promise.all([
        api.getAnalysts(),
        api.getIdeas(true),
      ]);

      setAnalysts(analystData);
      setIdeas(ideasData.ideas || []);
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to load data');
    } finally {
      setLoading(false);
    }
  };

  const handleAddIdea = async () => {
    if (!newTicker.trim()) return;

    try {
      setAdding(true);
      await api.createIdea({
        ticker: newTicker.toUpperCase(),
        primary_analyst_id: newPrimaryAnalyst || undefined,
        secondary_analyst_id: newSecondaryAnalyst || undefined,
        model_path: newModelPath || undefined,
      });

      // Reset form
      setNewTicker('');
      setNewPrimaryAnalyst(null);
      setNewSecondaryAnalyst(null);
      setNewModelPath('');

      // Reload data
      await loadData();
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to add idea');
    } finally {
      setAdding(false);
    }
  };

  const handleUpdateIdea = async () => {
    if (!editingIdea) return;

    try {
      await api.updateIdea(editingIdea.id, {
        primary_analyst_id: editPrimaryAnalyst || undefined,
        secondary_analyst_id: editSecondaryAnalyst || undefined,
        model_path: editModelPath || undefined,
        model_share_link: editModelShareLink || undefined,
        thesis: editThesis || undefined,
        next_steps: editNextSteps || undefined,
        notes: editNotes || undefined,
        initial_review_complete: editInitialReview,
        deep_dive_complete: editDeepDive,
        model_complete: editModel,
        writeup_complete: editWriteup,
      });

      setEditingIdea(null);
      await loadData();
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to update idea');
    }
  };

  const handleDeleteIdea = async (id: number) => {
    if (!confirm('Are you sure you want to remove this idea from the pipeline?')) return;

    try {
      await api.deleteIdea(id);
      await loadData();
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to delete idea');
    }
  };

  const handleRefreshModel = async (id: number) => {
    try {
      setRefreshing(id);
      await api.refreshIdeaModelData(id);
      await loadData();
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to refresh model data');
    } finally {
      setRefreshing(null);
    }
  };

  const handleUpdatePipelineStatus = async (idea: Idea, field: string, value: boolean) => {
    try {
      await api.updateIdea(idea.id, { [field]: value });
      await loadData();
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to update status');
    }
  };

  const handleFileUpload = async (ideaId: number, files: FileList | null) => {
    if (!files || files.length === 0) return;

    try {
      setUploading(ideaId);
      for (const file of Array.from(files)) {
        await api.uploadIdeaDocument(ideaId, file);
      }
      await loadData();
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to upload file');
    } finally {
      setUploading(null);
    }
  };

  const handleDeleteDocument = async (ideaId: number, documentId: number) => {
    if (!confirm('Are you sure you want to delete this document?')) return;

    try {
      await api.deleteIdeaDocument(ideaId, documentId);
      await loadData();
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to delete document');
    }
  };

  const openEditModal = (idea: Idea) => {
    setEditingIdea(idea);
    setEditPrimaryAnalyst(idea.primary_analyst?.id || null);
    setEditSecondaryAnalyst(idea.secondary_analyst?.id || null);
    setEditModelPath(idea.model_path || '');
    setEditModelShareLink(idea.model_share_link || '');
    setEditThesis(idea.thesis || '');
    setEditNextSteps(idea.next_steps || '');
    setEditNotes(idea.notes || '');
    setEditInitialReview(idea.initial_review_complete);
    setEditDeepDive(idea.deep_dive_complete);
    setEditModel(idea.model_complete);
    setEditWriteup(idea.writeup_complete);
  };

  const formatPercent = (value: number | null | undefined) => {
    if (value == null) return '-';
    return `${value >= 0 ? '+' : ''}${value.toFixed(2)}%`;
  };

  const formatNumber = (value: number | null | undefined, decimals: number = 2) => {
    if (value == null) return '-';
    return value.toFixed(decimals);
  };

  const formatFileSize = (bytes: number | null) => {
    if (!bytes) return '-';
    if (bytes < 1024) return `${bytes} B`;
    if (bytes < 1024 * 1024) return `${(bytes / 1024).toFixed(1)} KB`;
    return `${(bytes / (1024 * 1024)).toFixed(1)} MB`;
  };

  const getPipelineProgress = (idea: Idea) => {
    const steps = [
      idea.initial_review_complete,
      idea.deep_dive_complete,
      idea.model_complete,
      idea.writeup_complete,
    ];
    return steps.filter(Boolean).length;
  };

  const getValueClass = (value: number | null | undefined) => {
    if (value == null) return 'text-zinc-400';
    return value >= 0 ? 'value-positive' : 'value-negative';
  };

  if (authLoading || loading) {
    return (
      <Layout>
        <div className="flex items-center justify-center h-64">
          <div className="flex items-center gap-3 text-zinc-500">
            <svg className="loading-spinner" viewBox="0 0 24 24" fill="none">
              <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
              <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
            </svg>
            <span>Loading ideas...</span>
          </div>
        </div>
      </Layout>
    );
  }

  return (
    <Layout>
      <div className="space-y-6">
        {/* Page Header */}
        <div>
          <h1 className="text-2xl font-bold text-zinc-900">Idea Pipeline</h1>
          <p className="text-sm text-zinc-500 mt-1">
            Track and research new investment ideas
          </p>
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

        {/* Add Idea Form */}
        <div className="card">
          <div className="card-header">
            <h2 className="card-title">Add New Idea</h2>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-5 gap-4">
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
            <div>
              <label className="label">Model Path</label>
              <input
                type="text"
                value={newModelPath}
                onChange={(e) => setNewModelPath(e.target.value)}
                placeholder="/models/TICKER/Model.xlsx"
                className="input"
              />
            </div>
            <div className="flex items-end">
              <button
                onClick={handleAddIdea}
                disabled={adding || !newTicker.trim()}
                className="btn btn-primary w-full"
              >
                {adding ? 'Adding...' : 'Add Idea'}
              </button>
            </div>
          </div>
        </div>

        {/* Ideas Table */}
        <div className="card p-0 overflow-hidden">
          <div className="table-container mx-0">
            <table className="table">
              <thead>
                <tr>
                  <th>Ticker</th>
                  <th>Primary</th>
                  <th>Secondary</th>
                  <th className="text-center">Pipeline</th>
                  <th className="text-right">CCM FV</th>
                  <th className="text-right">Street PT</th>
                  <th className="text-right">3Y IRR</th>
                  <th className="text-center">Model</th>
                  <th className="text-center">Actions</th>
                </tr>
              </thead>
              <tbody className="tabular-nums">
                {ideas.map((idea) => (
                  <Fragment key={idea.id}>
                    <tr
                      className={`cursor-pointer transition-colors ${expandedTicker === idea.id ? 'bg-blue-50/50' : ''}`}
                      onClick={() => setExpandedTicker(expandedTicker === idea.id ? null : idea.id)}
                    >
                      <td className="font-semibold text-zinc-900">
                        <div className="flex items-center gap-2">
                          <span>{idea.ticker}</span>
                          <svg className={`h-4 w-4 text-zinc-400 transition-transform ${expandedTicker === idea.id ? 'rotate-180' : ''}`} fill="none" viewBox="0 0 24 24" stroke="currentColor">
                            <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M19 9l-7 7-7-7" />
                          </svg>
                        </div>
                      </td>
                      <td className="text-zinc-600">{idea.primary_analyst?.name || '-'}</td>
                      <td className="text-zinc-600">{idea.secondary_analyst?.name || '-'}</td>
                      <td>
                        <div className="flex justify-center gap-1">
                          {['initial_review_complete', 'deep_dive_complete', 'model_complete', 'writeup_complete'].map((field, idx) => {
                            const isComplete = idea[field as keyof Idea] as boolean;
                            const labels = ['IR', 'DD', 'M', 'W'];
                            return (
                              <button
                                key={field}
                                onClick={(e) => {
                                  e.stopPropagation();
                                  handleUpdatePipelineStatus(idea, field, !isComplete);
                                }}
                                className={`w-7 h-7 rounded text-xs font-medium transition-colors ${
                                  isComplete
                                    ? 'bg-emerald-500 text-white'
                                    : 'bg-zinc-200 text-zinc-600 hover:bg-zinc-300'
                                }`}
                                title={['Initial Review', 'Deep Dive', 'Model', 'Writeup'][idx]}
                              >
                                {labels[idx]}
                              </button>
                            );
                          })}
                        </div>
                      </td>
                      <td className="text-right font-medium">
                        {idea.model_data?.ccm_fair_value ? `$${idea.model_data.ccm_fair_value.toFixed(2)}` : '-'}
                      </td>
                      <td className="text-right font-medium">
                        {idea.model_data?.street_price_target ? `$${idea.model_data.street_price_target.toFixed(2)}` : '-'}
                      </td>
                      <td className="text-right font-medium">
                        {idea.model_data?.irr_3yr != null ? `${(idea.model_data.irr_3yr * 100).toFixed(1)}%` : '-'}
                      </td>
                      <td className="text-center">
                        {idea.model_share_link ? (
                          <a
                            href={idea.model_share_link}
                            target="_blank"
                            rel="noopener noreferrer"
                            onClick={(e) => e.stopPropagation()}
                            className="text-blue-600 hover:text-blue-800 font-medium text-sm"
                          >
                            Open
                          </a>
                        ) : idea.model_path ? (
                          <span className="badge badge-neutral">Local</span>
                        ) : (
                          <span className="text-zinc-400">-</span>
                        )}
                      </td>
                      <td className="text-center">
                        <div className="flex items-center justify-center gap-1">
                          <button
                            onClick={(e) => { e.stopPropagation(); openEditModal(idea); }}
                            className="btn btn-ghost btn-xs"
                          >
                            Edit
                          </button>
                          {idea.model_path && (
                            <button
                              onClick={(e) => { e.stopPropagation(); handleRefreshModel(idea.id); }}
                              disabled={refreshing === idea.id}
                              className="btn btn-ghost btn-xs text-emerald-600 hover:text-emerald-700 disabled:opacity-50"
                            >
                              {refreshing === idea.id ? '...' : 'Refresh'}
                            </button>
                          )}
                          <button
                            onClick={(e) => { e.stopPropagation(); handleDeleteIdea(idea.id); }}
                            className="btn btn-ghost btn-xs text-red-600 hover:text-red-700"
                          >
                            Remove
                          </button>
                        </div>
                      </td>
                    </tr>
                    {/* Expanded Detail Row */}
                    {expandedTicker === idea.id && (
                      <tr>
                        <td colSpan={9} className="p-0">
                          <div className="bg-zinc-50/50 border-t border-zinc-100 p-6">
                            <div className="grid grid-cols-1 lg:grid-cols-3 gap-6">
                              {/* Research Pipeline */}
                              <div className="bg-white rounded-lg border border-zinc-200 p-5">
                                <h3 className="text-sm font-semibold text-zinc-900 mb-4">Research Pipeline</h3>
                                <div className="space-y-3">
                                  {[
                                    { field: 'initial_review_complete', label: 'Initial Review' },
                                    { field: 'deep_dive_complete', label: 'Deep Dive' },
                                    { field: 'model_complete', label: 'Model' },
                                    { field: 'writeup_complete', label: 'Writeup' },
                                  ].map(({ field, label }) => {
                                    const isComplete = idea[field as keyof Idea] as boolean;
                                    return (
                                      <label key={field} className="flex items-center gap-3 cursor-pointer">
                                        <input
                                          type="checkbox"
                                          checked={isComplete}
                                          onChange={(e) => handleUpdatePipelineStatus(idea, field, e.target.checked)}
                                          className="w-5 h-5 rounded border-zinc-300 text-blue-600 focus:ring-blue-500"
                                        />
                                        <span className={isComplete ? 'text-emerald-600 font-medium' : 'text-zinc-700'}>{label}</span>
                                      </label>
                                    );
                                  })}
                                </div>
                                <div className="mt-4 pt-4 border-t border-zinc-100">
                                  <div className="text-sm text-zinc-500 mb-2">Progress: {getPipelineProgress(idea)}/4 complete</div>
                                  <div className="progress-bar">
                                    <div
                                      className="progress-bar-fill bg-blue-600"
                                      style={{ width: `${(getPipelineProgress(idea) / 4) * 100}%` }}
                                    />
                                  </div>
                                </div>
                              </div>

                              {/* Thesis */}
                              <div className="bg-white rounded-lg border border-zinc-200 p-5">
                                <h3 className="text-sm font-semibold text-zinc-900 mb-3">Thesis</h3>
                                <p className="text-sm text-zinc-700 whitespace-pre-wrap leading-relaxed">
                                  {idea.thesis || <span className="text-zinc-400 italic">No thesis yet. Click Edit to add.</span>}
                                </p>
                              </div>

                              {/* Next Steps */}
                              <div className="bg-white rounded-lg border border-zinc-200 p-5">
                                <h3 className="text-sm font-semibold text-zinc-900 mb-3">Next Steps</h3>
                                <p className="text-sm text-zinc-700 whitespace-pre-wrap leading-relaxed">
                                  {idea.next_steps || <span className="text-zinc-400 italic">No next steps yet. Click Edit to add.</span>}
                                </p>
                              </div>

                              {/* Notes */}
                              <div className="bg-white rounded-lg border border-zinc-200 p-5">
                                <h3 className="text-sm font-semibold text-zinc-900 mb-3">Notes</h3>
                                <p className="text-sm text-zinc-700 whitespace-pre-wrap max-h-40 overflow-y-auto leading-relaxed">
                                  {idea.notes || <span className="text-zinc-400 italic">No notes yet. Click Edit to add.</span>}
                                </p>
                              </div>

                              {/* Documents */}
                              <div className="bg-white rounded-lg border border-zinc-200 p-5">
                                <h3 className="text-sm font-semibold text-zinc-900 mb-3">Documents</h3>
                                <div className="space-y-2 max-h-40 overflow-y-auto">
                                  {idea.documents.length === 0 ? (
                                    <p className="text-sm text-zinc-400 italic">No documents uploaded.</p>
                                  ) : (
                                    idea.documents.map((doc) => (
                                      <div key={doc.id} className="flex items-center justify-between text-sm p-2 rounded-lg hover:bg-zinc-50 transition-colors">
                                        <a
                                          href={api.getIdeaDocumentDownloadUrl(idea.id, doc.id)}
                                          target="_blank"
                                          rel="noopener noreferrer"
                                          className="text-blue-600 hover:text-blue-800 truncate max-w-[200px]"
                                          title={doc.original_filename}
                                        >
                                          {doc.original_filename}
                                        </a>
                                        <div className="flex items-center gap-2">
                                          <span className="text-zinc-400 text-xs">{formatFileSize(doc.file_size)}</span>
                                          <button
                                            onClick={() => handleDeleteDocument(idea.id, doc.id)}
                                            className="text-red-500 hover:text-red-700 font-medium"
                                          >
                                            x
                                          </button>
                                        </div>
                                      </div>
                                    ))
                                  )}
                                </div>
                                <div className="mt-3">
                                  <input
                                    type="file"
                                    ref={fileInputRef}
                                    className="hidden"
                                    multiple
                                    accept=".pdf,.doc,.docx,.xls,.xlsx,.ppt,.pptx,.txt"
                                    onChange={(e) => handleFileUpload(idea.id, e.target.files)}
                                  />
                                  <button
                                    onClick={() => fileInputRef.current?.click()}
                                    disabled={uploading === idea.id}
                                    className="w-full px-3 py-2 border-2 border-dashed border-zinc-300 rounded-lg text-sm text-zinc-600 hover:bg-zinc-50 hover:border-zinc-400 disabled:opacity-50 transition-colors"
                                  >
                                    {uploading === idea.id ? 'Uploading...' : 'Upload Documents'}
                                  </button>
                                </div>
                              </div>

                              {/* Valuation (if model data exists) */}
                              {idea.model_data && (
                                <div className="bg-white rounded-lg border border-zinc-200 p-5">
                                  <h3 className="text-sm font-semibold text-zinc-900 mb-4">Valuation</h3>
                                  <div className="grid grid-cols-2 gap-4">
                                    <div className="metric-card metric-card-blue">
                                      <div className="metric-label">CCM Fair Value</div>
                                      <div className="metric-value">
                                        {idea.model_data?.ccm_fair_value ? `$${idea.model_data.ccm_fair_value.toFixed(2)}` : '-'}
                                      </div>
                                    </div>
                                    <div className="metric-card metric-card-purple">
                                      <div className="metric-label">Street Target</div>
                                      <div className="metric-value">
                                        {idea.model_data?.street_price_target ? `$${idea.model_data.street_price_target.toFixed(2)}` : '-'}
                                      </div>
                                    </div>
                                    <div className="metric-card metric-card-teal">
                                      <div className="metric-label">CCM vs Street</div>
                                      <div className={`metric-value ${getValueClass(idea.model_data?.ccm_vs_street_diff_pct)}`}>
                                        {formatPercent(idea.model_data?.ccm_vs_street_diff_pct)}
                                      </div>
                                    </div>
                                    <div className="metric-card metric-card-green">
                                      <div className="metric-label">3-Year IRR</div>
                                      <div className="metric-value text-blue-600">
                                        {idea.model_data?.irr_3yr != null ? `${(idea.model_data.irr_3yr * 100).toFixed(1)}%` : '-'}
                                      </div>
                                    </div>
                                  </div>
                                </div>
                              )}
                            </div>

                            {/* Financial Estimates (if model data exists) */}
                            {idea.model_data && (
                              <div className="mt-6 grid grid-cols-1 lg:grid-cols-2 gap-6">
                                {['revenue', 'ebitda', 'eps', 'fcf'].map((metric) => {
                                  const data = idea.model_data?.[metric as keyof ModelData] as MetricEstimates | null;
                                  if (!data) return null;
                                  const marginData = metric === 'ebitda' ? idea.model_data?.ebitda_margin :
                                                     metric === 'fcf' ? idea.model_data?.fcf_margin : null;

                                  return (
                                    <div key={metric} className="bg-white rounded-lg border border-zinc-200 p-5">
                                      <h3 className="text-sm font-semibold text-zinc-900 mb-3 uppercase">{metric}</h3>
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
                                              <td className="py-2 text-right">{formatNumber(data.ccm_minus1yr)}</td>
                                              <td className="py-2 text-right">{formatNumber(data.ccm_1yr)}</td>
                                              <td className="py-2 text-right">{formatNumber(data.ccm_2yr)}</td>
                                              <td className="py-2 text-right">{formatNumber(data.ccm_3yr)}</td>
                                            </tr>
                                            <tr className="border-t border-zinc-100">
                                              <td className="py-2 font-medium text-zinc-700">Street</td>
                                              <td className="py-2 text-right">{formatNumber(data.street_minus1yr)}</td>
                                              <td className="py-2 text-right">{formatNumber(data.street_1yr)}</td>
                                              <td className="py-2 text-right">{formatNumber(data.street_2yr)}</td>
                                              <td className="py-2 text-right">{formatNumber(data.street_3yr)}</td>
                                            </tr>
                                            <tr className="border-t border-zinc-100 text-emerald-600">
                                              <td className="py-2 font-medium">Growth (CCM)</td>
                                              <td className="py-2 text-right">-</td>
                                              <td className="py-2 text-right">{formatPercent(data.growth_ccm_1yr)}</td>
                                              <td className="py-2 text-right">{formatPercent(data.growth_ccm_2yr)}</td>
                                              <td className="py-2 text-right">{formatPercent(data.growth_ccm_3yr)}</td>
                                            </tr>
                                            <tr className="border-t border-zinc-100 text-blue-600">
                                              <td className="py-2 font-medium">CCM vs Street</td>
                                              <td className="py-2 text-right">-</td>
                                              <td className="py-2 text-right">{formatPercent(data.diff_1yr_pct)}</td>
                                              <td className="py-2 text-right">{formatPercent(data.diff_2yr_pct)}</td>
                                              <td className="py-2 text-right">{formatPercent(data.diff_3yr_pct)}</td>
                                            </tr>
                                            {marginData && (
                                              <tr className="border-t border-zinc-100 text-violet-600">
                                                <td className="py-2 font-medium">Margin (CCM)</td>
                                                <td className="py-2 text-right">{formatPercent(marginData.ccm_minus1yr)}</td>
                                                <td className="py-2 text-right">{formatPercent(marginData.ccm_1yr)}</td>
                                                <td className="py-2 text-right">{formatPercent(marginData.ccm_2yr)}</td>
                                                <td className="py-2 text-right">{formatPercent(marginData.ccm_3yr)}</td>
                                              </tr>
                                            )}
                                          </tbody>
                                        </table>
                                      </div>
                                    </div>
                                  );
                                })}
                              </div>
                            )}
                          </div>
                        </td>
                      </tr>
                    )}
                  </Fragment>
                ))}
                {ideas.length === 0 && (
                  <tr>
                    <td colSpan={9}>
                      <div className="empty-state">
                        <svg className="empty-state-icon" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                          <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9.663 17h4.673M12 3v1m6.364 1.636l-.707.707M21 12h-1M4 12H3m3.343-5.657l-.707-.707m2.828 9.9a5 5 0 117.072 0l-.548.547A3.374 3.374 0 0014 18.469V19a2 2 0 11-4 0v-.531c0-.895-.356-1.754-.988-2.386l-.548-.547z" />
                        </svg>
                        <p className="empty-state-title">No ideas in the pipeline</p>
                        <p className="empty-state-description">Add a ticker above to get started.</p>
                      </div>
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>

        {/* Edit Modal */}
        {editingIdea && (
          <div className="modal-backdrop" onClick={() => setEditingIdea(null)}>
            <div className="modal w-full max-w-2xl max-h-[90vh]" onClick={(e) => e.stopPropagation()}>
              <div className="modal-header">
                <h2 className="text-lg font-semibold text-zinc-900">Edit {editingIdea.ticker}</h2>
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
                </div>
                <div>
                  <label className="label">Research Pipeline</label>
                  <div className="flex flex-wrap gap-4 mt-2">
                    <label className="flex items-center gap-2 cursor-pointer">
                      <input
                        type="checkbox"
                        checked={editInitialReview}
                        onChange={(e) => setEditInitialReview(e.target.checked)}
                        className="w-4 h-4 rounded border-zinc-300 text-blue-600"
                      />
                      <span className="text-sm">Initial Review</span>
                    </label>
                    <label className="flex items-center gap-2 cursor-pointer">
                      <input
                        type="checkbox"
                        checked={editDeepDive}
                        onChange={(e) => setEditDeepDive(e.target.checked)}
                        className="w-4 h-4 rounded border-zinc-300 text-blue-600"
                      />
                      <span className="text-sm">Deep Dive</span>
                    </label>
                    <label className="flex items-center gap-2 cursor-pointer">
                      <input
                        type="checkbox"
                        checked={editModel}
                        onChange={(e) => setEditModel(e.target.checked)}
                        className="w-4 h-4 rounded border-zinc-300 text-blue-600"
                      />
                      <span className="text-sm">Model</span>
                    </label>
                    <label className="flex items-center gap-2 cursor-pointer">
                      <input
                        type="checkbox"
                        checked={editWriteup}
                        onChange={(e) => setEditWriteup(e.target.checked)}
                        className="w-4 h-4 rounded border-zinc-300 text-blue-600"
                      />
                      <span className="text-sm">Writeup</span>
                    </label>
                  </div>
                </div>
                <div>
                  <label className="label">Thesis</label>
                  <textarea
                    value={editThesis}
                    onChange={(e) => setEditThesis(e.target.value)}
                    rows={3}
                    placeholder="Investment thesis..."
                    className="input resize-none"
                  />
                </div>
                <div>
                  <label className="label">Next Steps</label>
                  <textarea
                    value={editNextSteps}
                    onChange={(e) => setEditNextSteps(e.target.value)}
                    rows={3}
                    placeholder="What needs to be done next..."
                    className="input resize-none"
                  />
                </div>
                <div>
                  <label className="label">Notes</label>
                  <textarea
                    value={editNotes}
                    onChange={(e) => setEditNotes(e.target.value)}
                    rows={4}
                    placeholder="Research notes, observations, etc..."
                    className="input resize-none"
                  />
                </div>
              </div>
              <div className="modal-footer">
                <button
                  onClick={() => setEditingIdea(null)}
                  className="btn btn-secondary"
                >
                  Cancel
                </button>
                <button
                  onClick={handleUpdateIdea}
                  className="btn btn-primary"
                >
                  Save Changes
                </button>
              </div>
            </div>
          </div>
        )}
      </div>
    </Layout>
  );
}
