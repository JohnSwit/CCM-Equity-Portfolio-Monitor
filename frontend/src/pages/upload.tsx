import { useState, useEffect, useRef, useCallback } from 'react';
import Layout from '@/components/Layout';
import { api } from '@/lib/api';

interface BulkImportJob {
  job_id: string;
  status: string;
  status_message: string;
  file_name: string;
  total_rows: number;
  rows_processed: number;
  rows_imported: number;
  rows_skipped: number;
  rows_error: number;
  progress_percent: number;
  total_batches: number;
  batches_completed: number;
  current_batch: number;
  batch_size: number;
  created_at: string;
  started_at: string | null;
  completed_at: string | null;
  estimated_completion: string | null;
  avg_rows_per_second: number | null;
  errors_sample: any[];
  is_resumable: boolean;
}

interface BatchSummary {
  pending: number;
  processing: number;
  completed: number;
  failed: number;
  skipped: number;
}

interface InceptionAccount {
  account_id: number;
  account_number: string;
  display_name: string;
  inception_date: string;
  total_value: number;
  position_count: number;
}

export default function Upload() {
  // Tab state
  const [activeTab, setActiveTab] = useState<'simple' | 'bulk' | 'inception'>('simple');

  // Simple Import state
  const [file, setFile] = useState<File | null>(null);
  const [preview, setPreview] = useState<any>(null);
  const [importing, setImporting] = useState(false);
  const [importResult, setImportResult] = useState<any>(null);
  const [importHistory, setImportHistory] = useState<any[]>([]);
  const [runningJob, setRunningJob] = useState(false);
  const [jobResult, setJobResult] = useState<any>(null);

  // Bulk Import state
  const [bulkLoading, setBulkLoading] = useState(false);
  const [bulkError, setBulkError] = useState<string | null>(null);
  const bulkFileInputRef = useRef<HTMLInputElement>(null);
  const [bulkJobs, setBulkJobs] = useState<BulkImportJob[]>([]);
  const [activeJob, setActiveJob] = useState<BulkImportJob | null>(null);
  const [batchSummary, setBatchSummary] = useState<BatchSummary | null>(null);
  const [batchSize, setBatchSize] = useState(5000);
  const [skipAnalytics, setSkipAnalytics] = useState(true);
  const [validateOnly, setValidateOnly] = useState(false);
  const [autoRefresh, setAutoRefresh] = useState(true);
  const refreshIntervalRef = useRef<NodeJS.Timeout | null>(null);

  // Inception Import state
  const [inceptionFile, setInceptionFile] = useState<File | null>(null);
  const [inceptionPreview, setInceptionPreview] = useState<any>(null);
  const [inceptionImporting, setInceptionImporting] = useState(false);
  const [inceptionResult, setInceptionResult] = useState<any>(null);
  const [inceptionAccounts, setInceptionAccounts] = useState<InceptionAccount[]>([]);

  useEffect(() => {
    loadImportHistory();
    loadBulkJobs();
    loadInceptionData();
  }, []);

  // Auto-refresh for active bulk jobs
  useEffect(() => {
    if (autoRefresh && activeJob && ['pending', 'processing', 'validating'].includes(activeJob.status)) {
      refreshIntervalRef.current = setInterval(() => {
        refreshActiveJob();
      }, 2000);
    } else if (refreshIntervalRef.current) {
      clearInterval(refreshIntervalRef.current);
    }

    return () => {
      if (refreshIntervalRef.current) {
        clearInterval(refreshIntervalRef.current);
      }
    };
  }, [autoRefresh, activeJob?.job_id, activeJob?.status]);

  // Simple Import functions
  const loadImportHistory = async () => {
    try {
      const data = await api.getImportHistory(10);
      setImportHistory(data);
    } catch (error) {
      console.error('Failed to load import history:', error);
    }
  };

  const handleFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const selectedFile = e.target.files?.[0];
    if (selectedFile) {
      setFile(selectedFile);
      setPreview(null);
      setImportResult(null);
    }
  };

  const handlePreview = async () => {
    if (!file) return;

    setImporting(true);
    try {
      const data = await api.importBDTransactions(file, 'preview');
      setPreview(data);
    } catch (error: any) {
      alert('Preview failed: ' + error.response?.data?.detail);
    } finally {
      setImporting(false);
    }
  };

  const handleImport = async () => {
    if (!file) return;

    if (!confirm('Are you sure you want to import this file?')) return;

    setImporting(true);
    try {
      const data = await api.importBDTransactions(file, 'commit');
      setImportResult(data);
      setFile(null);
      setPreview(null);
      await loadImportHistory();
    } catch (error: any) {
      alert('Import failed: ' + error.response?.data?.detail);
    } finally {
      setImporting(false);
    }
  };

  const handleRunMarketDataUpdate = async () => {
    if (!confirm('Run market data update? This will fetch the latest prices for all securities.')) return;

    setRunningJob(true);
    setJobResult(null);
    try {
      const data = await api.runJob('market_data_update');
      setJobResult(data);
      alert('Market data update completed: ' + data.message);
    } catch (error: any) {
      const errorMsg = error.response?.data?.detail || error.message || 'Unknown error';
      alert('Job failed: ' + errorMsg);
      setJobResult({ status: 'failed', message: errorMsg });
    } finally {
      setRunningJob(false);
    }
  };

  const handleDeleteImport = async (importId: number, fileName: string) => {
    if (!confirm(`Delete import "${fileName}" and all its transactions? This cannot be undone. You will need to run the analytics update after deletion.`)) return;

    try {
      const result = await api.deleteImport(importId);
      alert(result.message);
      await loadImportHistory();
    } catch (error: any) {
      alert('Delete failed: ' + (error.response?.data?.detail || error.message));
    }
  };

  // Bulk Import functions
  const loadBulkJobs = async () => {
    try {
      const data = await api.listBulkImports(undefined, 50);
      setBulkJobs(data.jobs || []);
    } catch (err: any) {
      console.error('Failed to load bulk jobs:', err);
    }
  };

  const refreshActiveJob = useCallback(async () => {
    if (!activeJob) return;
    try {
      const data = await api.getBulkImportStatus(activeJob.job_id, true);
      setActiveJob(data);
      setBatchSummary(data.batch_summary || null);
      setBulkJobs(prev => prev.map(j => j.job_id === data.job_id ? data : j));
    } catch (err) {
      console.error('Failed to refresh job:', err);
    }
  }, [activeJob?.job_id]);

  const handleBulkFileUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;

    setBulkLoading(true);
    setBulkError(null);

    try {
      const result = await api.startBulkImport(file, {
        batchSize,
        skipAnalytics,
        validateOnly,
      });

      const jobData = await api.getBulkImportStatus(result.job_id, true);
      setActiveJob(jobData);
      setBatchSummary(jobData.batch_summary || null);
      setBulkJobs(prev => [jobData, ...prev]);
    } catch (err: any) {
      setBulkError(err.response?.data?.detail || 'Failed to start import');
    } finally {
      setBulkLoading(false);
      if (bulkFileInputRef.current) {
        bulkFileInputRef.current.value = '';
      }
    }
  };

  const handlePause = async () => {
    if (!activeJob) return;
    try {
      await api.pauseBulkImport(activeJob.job_id);
      refreshActiveJob();
    } catch (err: any) {
      setBulkError(err.response?.data?.detail || 'Failed to pause job');
    }
  };

  const handleResume = async () => {
    if (!activeJob) return;
    try {
      await api.resumeBulkImport(activeJob.job_id);
      refreshActiveJob();
    } catch (err: any) {
      setBulkError(err.response?.data?.detail || 'Failed to resume job');
    }
  };

  const handleCancel = async () => {
    if (!activeJob) return;
    if (!confirm('Are you sure you want to cancel this import?')) return;
    try {
      await api.cancelBulkImport(activeJob.job_id);
      refreshActiveJob();
    } catch (err: any) {
      setBulkError(err.response?.data?.detail || 'Failed to cancel job');
    }
  };

  const handleRetryFailed = async () => {
    if (!activeJob) return;
    try {
      await api.retryFailedBatches(activeJob.job_id);
      refreshActiveJob();
    } catch (err: any) {
      setBulkError(err.response?.data?.detail || 'Failed to retry batches');
    }
  };

  // Inception Import functions
  const loadInceptionData = async () => {
    try {
      const data = await api.getInceptionData();
      setInceptionAccounts(data.accounts || []);
    } catch (error) {
      console.error('Failed to load inception data:', error);
    }
  };

  const handleInceptionFileChange = (e: React.ChangeEvent<HTMLInputElement>) => {
    const selectedFile = e.target.files?.[0];
    if (selectedFile) {
      setInceptionFile(selectedFile);
      setInceptionPreview(null);
      setInceptionResult(null);
    }
  };

  const handleInceptionPreview = async () => {
    if (!inceptionFile) return;

    setInceptionImporting(true);
    try {
      const data = await api.importInceptionPositions(inceptionFile, 'preview');
      setInceptionPreview(data);
    } catch (error: any) {
      alert('Preview failed: ' + (error.response?.data?.detail || error.message));
    } finally {
      setInceptionImporting(false);
    }
  };

  const handleInceptionImport = async () => {
    if (!inceptionFile) return;

    if (!confirm('Are you sure you want to import these inception positions? This will replace any existing inception data for the affected accounts.')) return;

    setInceptionImporting(true);
    try {
      const data = await api.importInceptionPositions(inceptionFile, 'commit');
      setInceptionResult(data);
      setInceptionFile(null);
      setInceptionPreview(null);
      await loadInceptionData();
    } catch (error: any) {
      alert('Import failed: ' + (error.response?.data?.detail || error.message));
    } finally {
      setInceptionImporting(false);
    }
  };

  const handleDeleteInception = async (accountId: number, accountNumber: string) => {
    if (!confirm(`Delete inception data for account ${accountNumber}? This will remove the historical starting point for this account.`)) return;

    try {
      const result = await api.deleteAccountInception(accountId);
      alert(result.message);
      await loadInceptionData();
    } catch (error: any) {
      alert('Delete failed: ' + (error.response?.data?.detail || error.message));
    }
  };

  const formatNumber = (n: number | null | undefined) => {
    if (n == null) return '-';
    return n.toLocaleString();
  };

  const formatDate = (dateStr: string | null) => {
    if (!dateStr) return '-';
    return new Date(dateStr).toLocaleString();
  };

  const formatCurrency = (n: number | null | undefined) => {
    if (n == null) return '-';
    return new Intl.NumberFormat('en-US', { style: 'currency', currency: 'USD' }).format(n);
  };

  const getStatusColor = (status: string) => {
    switch (status) {
      case 'completed': return 'bg-green-100 text-green-800';
      case 'completed_with_errors': return 'bg-yellow-100 text-yellow-800';
      case 'processing': return 'bg-blue-100 text-blue-800';
      case 'pending': return 'bg-gray-100 text-gray-800';
      case 'paused': return 'bg-orange-100 text-orange-800';
      case 'failed': return 'bg-red-100 text-red-800';
      case 'cancelled': return 'bg-gray-100 text-gray-500';
      default: return 'bg-gray-100 text-gray-800';
    }
  };

  return (
    <Layout>
      <div className="space-y-6">
        <div>
          <h1 className="text-2xl font-bold">Data Import</h1>
          <p className="text-sm text-gray-500 mt-1">
            Import Black Diamond transaction data
          </p>
        </div>

        {/* Tab Navigation */}
        <div className="border-b border-gray-200">
          <nav className="-mb-px flex space-x-8">
            <button
              onClick={() => setActiveTab('simple')}
              className={`py-2 px-1 border-b-2 font-medium text-sm ${
                activeTab === 'simple'
                  ? 'border-blue-500 text-blue-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
              }`}
            >
              Simple Import
            </button>
            <button
              onClick={() => setActiveTab('bulk')}
              className={`py-2 px-1 border-b-2 font-medium text-sm ${
                activeTab === 'bulk'
                  ? 'border-blue-500 text-blue-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
              }`}
            >
              Bulk Import
            </button>
            <button
              onClick={() => setActiveTab('inception')}
              className={`py-2 px-1 border-b-2 font-medium text-sm ${
                activeTab === 'inception'
                  ? 'border-blue-500 text-blue-600'
                  : 'border-transparent text-gray-500 hover:text-gray-700 hover:border-gray-300'
              }`}
            >
              Historical Inception
            </button>
          </nav>
        </div>

        {/* Simple Import Tab */}
        {activeTab === 'simple' && (
          <div className="space-y-6">
            {/* Upload Form */}
            <div className="card">
              <h2 className="text-lg font-semibold mb-4">Select CSV File</h2>
              <p className="text-sm text-gray-600 mb-4">
                Use this for smaller files (under 50K rows). For larger files, use the Bulk Import tab.
              </p>
              <div className="space-y-4">
                <div>
                  <input
                    type="file"
                    accept=".csv,.tsv"
                    onChange={handleFileChange}
                    className="input"
                  />
                  {file && (
                    <div className="mt-2 text-sm text-gray-600">
                      Selected: {file.name} ({(file.size / 1024).toFixed(1)} KB)
                    </div>
                  )}
                </div>

                <div className="flex space-x-4">
                  <button
                    onClick={handlePreview}
                    disabled={!file || importing}
                    className="btn btn-secondary"
                  >
                    {importing ? 'Previewing...' : 'Preview'}
                  </button>
                  <button
                    onClick={handleImport}
                    disabled={!file || importing}
                    className="btn btn-primary"
                  >
                    {importing ? 'Importing...' : 'Import'}
                  </button>
                </div>
              </div>
            </div>

            {/* Preview */}
            {preview && (
              <div className="card">
                <h2 className="text-lg font-semibold mb-4">
                  Preview ({preview.total_rows} rows)
                </h2>

                {preview.has_errors && (
                  <div className="bg-yellow-100 border border-yellow-400 text-yellow-700 px-4 py-3 rounded mb-4">
                    Warning: Some rows have errors
                  </div>
                )}

                <div className="mb-4">
                  <h3 className="font-semibold mb-2">Detected Transaction Type Mappings:</h3>
                  <div className="grid grid-cols-2 md:grid-cols-3 gap-2">
                    {Object.entries(preview.detected_mappings).map(([raw, normalized]) => (
                      <div key={raw} className="text-sm">
                        <span className="font-medium">{raw}</span> → {String(normalized)}
                      </div>
                    ))}
                  </div>
                </div>

                <div className="overflow-x-auto">
                  <table className="table text-sm">
                    <thead>
                      <tr>
                        <th>Row</th>
                        <th>Account</th>
                        <th>Symbol</th>
                        <th>Trade Date</th>
                        <th>Type</th>
                        <th>Units</th>
                        <th>Errors</th>
                      </tr>
                    </thead>
                    <tbody>
                      {preview.preview_rows.map((row: any) => (
                        <tr key={row.row_num} className={row.errors.length > 0 ? 'bg-red-50' : ''}>
                          <td>{row.row_num}</td>
                          <td>{row.data['Account Number']}</td>
                          <td>{row.data['Symbol']}</td>
                          <td>{row.data['Trade Date']}</td>
                          <td>{row.data['Transaction Type']}</td>
                          <td>{row.data['Units']}</td>
                          <td>
                            {row.errors.length > 0 && (
                              <span className="text-red-600 text-xs">{row.errors.join(', ')}</span>
                            )}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}

            {/* Import Result */}
            {importResult && (
              <div className="card">
                <h2 className="text-lg font-semibold mb-4">Import Complete</h2>
                <div className="space-y-2">
                  <div>
                    <span className="font-medium">Status:</span> {importResult.status}
                  </div>
                  <div>
                    <span className="font-medium">Rows Processed:</span> {importResult.rows_processed}
                  </div>
                  <div>
                    <span className="font-medium">Rows Imported:</span> {importResult.rows_imported}
                  </div>
                  <div>
                    <span className="font-medium">Rows with Errors:</span> {importResult.rows_error}
                  </div>

                  {importResult.errors && importResult.errors.length > 0 && (
                    <div className="mt-4">
                      <h3 className="font-semibold mb-2">Errors:</h3>
                      <div className="space-y-1 text-sm max-h-40 overflow-y-auto">
                        {importResult.errors.map((err: any, idx: number) => (
                          <div key={idx} className="text-red-600">
                            Row {err.row}: {err.error}
                          </div>
                        ))}
                      </div>
                    </div>
                  )}
                </div>
              </div>
            )}

            {/* Market Data Update */}
            <div className="card">
              <h2 className="text-lg font-semibold mb-4">Market Data & Analytics</h2>
              <p className="text-sm text-gray-600 mb-4">
                After importing transactions, run the market data update to fetch the latest prices and compute analytics.
              </p>
              <button
                onClick={handleRunMarketDataUpdate}
                disabled={runningJob}
                className="btn btn-primary"
              >
                {runningJob ? 'Running...' : 'Update Market Data & Compute Analytics'}
              </button>
              {jobResult && (
                <div className={`mt-4 p-3 rounded ${jobResult.status === 'success' ? 'bg-green-100 text-green-800' : 'bg-red-100 text-red-800'}`}>
                  {jobResult.message}
                </div>
              )}
            </div>

            {/* Import History */}
            <div className="card">
              <h2 className="text-lg font-semibold mb-4">Recent Imports</h2>
              <table className="table">
                <thead>
                  <tr>
                    <th>File Name</th>
                    <th>Date</th>
                    <th>Status</th>
                    <th>Processed</th>
                    <th>Imported</th>
                    <th>Errors</th>
                    <th>Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {importHistory.map((imp) => (
                    <tr key={imp.id}>
                      <td>{imp.file_name}</td>
                      <td>{new Date(imp.created_at).toLocaleString()}</td>
                      <td>
                        <span
                          className={`px-2 py-1 rounded text-xs ${
                            imp.status === 'completed'
                              ? 'bg-green-100 text-green-800'
                              : imp.status === 'completed_with_errors'
                              ? 'bg-yellow-100 text-yellow-800'
                              : 'bg-red-100 text-red-800'
                          }`}
                        >
                          {imp.status}
                        </span>
                      </td>
                      <td>{imp.rows_processed}</td>
                      <td>{imp.rows_imported}</td>
                      <td>{imp.rows_error}</td>
                      <td>
                        <button
                          onClick={() => handleDeleteImport(imp.id, imp.file_name)}
                          className="text-red-600 hover:text-red-800 text-sm font-medium"
                        >
                          Delete
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>

              {importHistory.length === 0 && (
                <div className="text-center py-8 text-gray-500">
                  No imports yet. Upload your first Black Diamond CSV file.
                </div>
              )}
            </div>
          </div>
        )}

        {/* Bulk Import Tab */}
        {activeTab === 'bulk' && (
          <div className="space-y-6">
            {bulkError && (
              <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded">
                {bulkError}
                <button onClick={() => setBulkError(null)} className="ml-4 text-red-500 hover:text-red-700">
                  Dismiss
                </button>
              </div>
            )}

            {/* Upload Section */}
            <div className="card p-6">
              <h2 className="text-lg font-semibold mb-4">Start New Bulk Import</h2>
              <p className="text-sm text-gray-600 mb-4">
                For large files (200K+ rows) with progress tracking, pause/resume, and fault tolerance.
              </p>

              <div className="grid grid-cols-1 md:grid-cols-4 gap-4 mb-4">
                <div>
                  <label className="block text-sm font-medium text-gray-700 mb-1">Batch Size</label>
                  <input
                    type="number"
                    value={batchSize}
                    onChange={(e) => setBatchSize(Number(e.target.value))}
                    min={100}
                    max={50000}
                    className="w-full px-3 py-2 border rounded"
                  />
                  <p className="text-xs text-gray-500 mt-1">Rows per batch (100-50,000)</p>
                </div>

                <div className="flex items-center">
                  <label className="flex items-center cursor-pointer">
                    <input
                      type="checkbox"
                      checked={skipAnalytics}
                      onChange={(e) => setSkipAnalytics(e.target.checked)}
                      className="mr-2"
                    />
                    <span className="text-sm">Skip Analytics</span>
                  </label>
                  <p className="text-xs text-gray-500 ml-2">(Recommended)</p>
                </div>

                <div className="flex items-center">
                  <label className="flex items-center cursor-pointer">
                    <input
                      type="checkbox"
                      checked={validateOnly}
                      onChange={(e) => setValidateOnly(e.target.checked)}
                      className="mr-2"
                    />
                    <span className="text-sm">Validate Only</span>
                  </label>
                </div>

                <div>
                  <input
                    ref={bulkFileInputRef}
                    type="file"
                    accept=".csv,.txt"
                    onChange={handleBulkFileUpload}
                    className="hidden"
                  />
                  <button
                    onClick={() => bulkFileInputRef.current?.click()}
                    disabled={bulkLoading}
                    className="w-full px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50"
                  >
                    {bulkLoading ? 'Uploading...' : 'Select CSV File'}
                  </button>
                </div>
              </div>
            </div>

            {/* Active Job Monitor */}
            {activeJob && (
              <div className="card p-6">
                <div className="flex justify-between items-start mb-4">
                  <div>
                    <h2 className="text-lg font-semibold">{activeJob.file_name}</h2>
                    <p className="text-sm text-gray-500">Job ID: {activeJob.job_id}</p>
                  </div>
                  <div className="flex items-center gap-2">
                    <span className={`px-3 py-1 rounded-full text-sm font-medium ${getStatusColor(activeJob.status)}`}>
                      {activeJob.status.replace('_', ' ')}
                    </span>
                    <label className="flex items-center text-sm text-gray-500">
                      <input
                        type="checkbox"
                        checked={autoRefresh}
                        onChange={(e) => setAutoRefresh(e.target.checked)}
                        className="mr-1"
                      />
                      Auto-refresh
                    </label>
                  </div>
                </div>

                {/* Progress Bar */}
                <div className="mb-4">
                  <div className="flex justify-between text-sm text-gray-600 mb-1">
                    <span>{activeJob.status_message}</span>
                    <span>{activeJob.progress_percent.toFixed(1)}%</span>
                  </div>
                  <div className="w-full bg-gray-200 rounded-full h-4">
                    <div
                      className={`h-4 rounded-full transition-all ${
                        activeJob.status === 'completed' ? 'bg-green-500' :
                        activeJob.status === 'failed' ? 'bg-red-500' :
                        activeJob.rows_error > 0 ? 'bg-yellow-500' : 'bg-blue-500'
                      }`}
                      style={{ width: `${activeJob.progress_percent}%` }}
                    />
                  </div>
                </div>

                {/* Stats Grid */}
                <div className="grid grid-cols-2 md:grid-cols-5 gap-4 mb-4">
                  <div className="text-center p-3 bg-gray-50 rounded">
                    <div className="text-2xl font-bold">{formatNumber(activeJob.total_rows)}</div>
                    <div className="text-xs text-gray-500">Total Rows</div>
                  </div>
                  <div className="text-center p-3 bg-gray-50 rounded">
                    <div className="text-2xl font-bold text-blue-600">{formatNumber(activeJob.rows_processed)}</div>
                    <div className="text-xs text-gray-500">Processed</div>
                  </div>
                  <div className="text-center p-3 bg-gray-50 rounded">
                    <div className="text-2xl font-bold text-green-600">{formatNumber(activeJob.rows_imported)}</div>
                    <div className="text-xs text-gray-500">Imported</div>
                  </div>
                  <div className="text-center p-3 bg-gray-50 rounded">
                    <div className="text-2xl font-bold text-gray-600">{formatNumber(activeJob.rows_skipped)}</div>
                    <div className="text-xs text-gray-500">Skipped (Duplicates)</div>
                  </div>
                  <div className="text-center p-3 bg-gray-50 rounded">
                    <div className="text-2xl font-bold text-red-600">{formatNumber(activeJob.rows_error)}</div>
                    <div className="text-xs text-gray-500">Errors</div>
                  </div>
                </div>

                {/* Batch Progress */}
                {batchSummary && (
                  <div className="grid grid-cols-5 gap-2 mb-4 text-sm">
                    <div className="text-center p-2 bg-gray-100 rounded">
                      <div className="font-bold">{batchSummary.pending}</div>
                      <div className="text-xs text-gray-500">Pending</div>
                    </div>
                    <div className="text-center p-2 bg-blue-100 rounded">
                      <div className="font-bold">{batchSummary.processing}</div>
                      <div className="text-xs text-blue-600">Processing</div>
                    </div>
                    <div className="text-center p-2 bg-green-100 rounded">
                      <div className="font-bold">{batchSummary.completed}</div>
                      <div className="text-xs text-green-600">Completed</div>
                    </div>
                    <div className="text-center p-2 bg-red-100 rounded">
                      <div className="font-bold">{batchSummary.failed}</div>
                      <div className="text-xs text-red-600">Failed</div>
                    </div>
                    <div className="text-center p-2 bg-gray-100 rounded">
                      <div className="font-bold">{batchSummary.skipped}</div>
                      <div className="text-xs text-gray-500">Skipped</div>
                    </div>
                  </div>
                )}

                {/* Timing Info */}
                <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mb-4 text-sm">
                  <div>
                    <span className="text-gray-500">Started:</span>{' '}
                    <span>{formatDate(activeJob.started_at)}</span>
                  </div>
                  {activeJob.avg_rows_per_second && (
                    <div>
                      <span className="text-gray-500">Speed:</span>{' '}
                      <span>{formatNumber(Math.round(activeJob.avg_rows_per_second))} rows/sec</span>
                    </div>
                  )}
                  {activeJob.estimated_completion && activeJob.status === 'processing' && (
                    <div>
                      <span className="text-gray-500">ETA:</span>{' '}
                      <span>{formatDate(activeJob.estimated_completion)}</span>
                    </div>
                  )}
                  {activeJob.completed_at && (
                    <div>
                      <span className="text-gray-500">Completed:</span>{' '}
                      <span>{formatDate(activeJob.completed_at)}</span>
                    </div>
                  )}
                </div>

                {/* Actions */}
                <div className="flex gap-2">
                  {activeJob.status === 'processing' && (
                    <>
                      <button
                        onClick={handlePause}
                        className="px-4 py-2 bg-yellow-500 text-white rounded hover:bg-yellow-600"
                      >
                        Pause
                      </button>
                      <button
                        onClick={handleCancel}
                        className="px-4 py-2 bg-red-500 text-white rounded hover:bg-red-600"
                      >
                        Cancel
                      </button>
                    </>
                  )}
                  {activeJob.is_resumable && activeJob.status !== 'processing' && (
                    <button
                      onClick={handleResume}
                      className="px-4 py-2 bg-green-500 text-white rounded hover:bg-green-600"
                    >
                      Resume
                    </button>
                  )}
                  {activeJob.status === 'completed_with_errors' && batchSummary && batchSummary.failed > 0 && (
                    <button
                      onClick={handleRetryFailed}
                      className="px-4 py-2 bg-blue-500 text-white rounded hover:bg-blue-600"
                    >
                      Retry Failed Batches
                    </button>
                  )}
                  <button
                    onClick={refreshActiveJob}
                    className="px-4 py-2 border border-gray-300 rounded hover:bg-gray-50"
                  >
                    Refresh
                  </button>
                </div>

                {/* Errors Sample */}
                {activeJob.errors_sample && activeJob.errors_sample.length > 0 && (
                  <div className="mt-4">
                    <h3 className="font-medium text-red-700 mb-2">Error Samples:</h3>
                    <div className="bg-red-50 p-3 rounded text-sm max-h-40 overflow-y-auto">
                      {activeJob.errors_sample.slice(0, 10).map((err: any, idx: number) => (
                        <div key={idx} className="text-red-700 mb-1">
                          {err.row ? `Row ${err.row}: ` : ''}{err.error || JSON.stringify(err)}
                        </div>
                      ))}
                    </div>
                  </div>
                )}
              </div>
            )}

            {/* Jobs History */}
            <div className="card p-6">
              <div className="flex justify-between items-center mb-4">
                <h2 className="text-lg font-semibold">Bulk Import History</h2>
                <button
                  onClick={loadBulkJobs}
                  className="text-sm text-blue-600 hover:text-blue-800"
                >
                  Refresh
                </button>
              </div>

              <div className="overflow-x-auto">
                <table className="min-w-full divide-y divide-gray-200">
                  <thead className="bg-gray-50">
                    <tr>
                      <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">File</th>
                      <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Status</th>
                      <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Total</th>
                      <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Imported</th>
                      <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Skipped</th>
                      <th className="px-4 py-3 text-right text-xs font-medium text-gray-500 uppercase">Errors</th>
                      <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Created</th>
                      <th className="px-4 py-3 text-left text-xs font-medium text-gray-500 uppercase">Actions</th>
                    </tr>
                  </thead>
                  <tbody className="bg-white divide-y divide-gray-200">
                    {bulkJobs.map((job) => (
                      <tr
                        key={job.job_id}
                        className={`hover:bg-gray-50 cursor-pointer ${activeJob?.job_id === job.job_id ? 'bg-blue-50' : ''}`}
                        onClick={() => {
                          api.getBulkImportStatus(job.job_id, true).then((data) => {
                            setActiveJob(data);
                            setBatchSummary(data.batch_summary || null);
                          });
                        }}
                      >
                        <td className="px-4 py-3">
                          <div className="font-medium">{job.file_name}</div>
                          <div className="text-xs text-gray-500">{job.job_id.slice(0, 8)}...</div>
                        </td>
                        <td className="px-4 py-3">
                          <span className={`px-2 py-1 rounded text-xs font-medium ${getStatusColor(job.status)}`}>
                            {job.status.replace('_', ' ')}
                          </span>
                        </td>
                        <td className="px-4 py-3 text-right">{formatNumber(job.total_rows)}</td>
                        <td className="px-4 py-3 text-right text-green-600">{formatNumber(job.rows_imported)}</td>
                        <td className="px-4 py-3 text-right text-gray-600">{formatNumber(job.rows_skipped)}</td>
                        <td className="px-4 py-3 text-right text-red-600">{formatNumber(job.rows_error)}</td>
                        <td className="px-4 py-3 text-sm text-gray-500">{formatDate(job.created_at)}</td>
                        <td className="px-4 py-3">
                          {job.is_resumable && job.status !== 'processing' && (
                            <button
                              onClick={(e) => {
                                e.stopPropagation();
                                api.resumeBulkImport(job.job_id).then(() => loadBulkJobs());
                              }}
                              className="text-xs text-blue-600 hover:text-blue-800"
                            >
                              Resume
                            </button>
                          )}
                        </td>
                      </tr>
                    ))}
                    {bulkJobs.length === 0 && (
                      <tr>
                        <td colSpan={8} className="px-4 py-8 text-center text-gray-500">
                          No bulk import jobs found
                        </td>
                      </tr>
                    )}
                  </tbody>
                </table>
              </div>
            </div>
          </div>
        )}

        {/* Historical Inception Tab */}
        {activeTab === 'inception' && (
          <div className="space-y-6">
            {/* Info Banner */}
            <div className="bg-blue-50 border border-blue-200 text-blue-800 px-4 py-3 rounded">
              <h3 className="font-semibold mb-1">Historical Portfolio Inception</h3>
              <p className="text-sm">
                Upload a CSV file with portfolio positions at a historical starting date (e.g., 12/31/2020).
                This establishes a baseline for computing returns and analytics from that point forward.
                Transaction imports will then build on top of this inception data.
              </p>
            </div>

            {/* Upload Form */}
            <div className="card">
              <h2 className="text-lg font-semibold mb-4">Upload Inception Positions</h2>
              <p className="text-sm text-gray-600 mb-4">
                Expected CSV columns: Account Number, Account Display Name, Class, Asset Name, Symbol, Units, Price, Market Value, Inception Date
              </p>
              <div className="space-y-4">
                <div>
                  <input
                    type="file"
                    accept=".csv,.tsv"
                    onChange={handleInceptionFileChange}
                    className="input"
                  />
                  {inceptionFile && (
                    <div className="mt-2 text-sm text-gray-600">
                      Selected: {inceptionFile.name} ({(inceptionFile.size / 1024).toFixed(1)} KB)
                    </div>
                  )}
                </div>

                <div className="flex space-x-4">
                  <button
                    onClick={handleInceptionPreview}
                    disabled={!inceptionFile || inceptionImporting}
                    className="btn btn-secondary"
                  >
                    {inceptionImporting ? 'Previewing...' : 'Preview'}
                  </button>
                  <button
                    onClick={handleInceptionImport}
                    disabled={!inceptionFile || inceptionImporting}
                    className="btn btn-primary"
                  >
                    {inceptionImporting ? 'Importing...' : 'Import'}
                  </button>
                </div>
              </div>
            </div>

            {/* Preview */}
            {inceptionPreview && (
              <div className="card">
                <h2 className="text-lg font-semibold mb-4">
                  Preview ({inceptionPreview.total_rows} positions)
                </h2>

                {inceptionPreview.has_errors && (
                  <div className="bg-yellow-100 border border-yellow-400 text-yellow-700 px-4 py-3 rounded mb-4">
                    Warning: Some rows have errors
                  </div>
                )}

                {inceptionPreview.multiple_dates_warning && (
                  <div className="bg-red-100 border border-red-400 text-red-700 px-4 py-3 rounded mb-4">
                    Error: Multiple inception dates detected. All positions must have the same inception date.
                  </div>
                )}

                {inceptionPreview.inception_date && (
                  <div className="mb-4 p-3 bg-green-50 rounded">
                    <span className="font-semibold">Inception Date: </span>
                    <span className="text-green-800">{inceptionPreview.inception_date}</span>
                  </div>
                )}

                {/* Accounts Summary */}
                {inceptionPreview.accounts_summary && inceptionPreview.accounts_summary.length > 0 && (
                  <div className="mb-4">
                    <h3 className="font-semibold mb-2">Accounts Summary</h3>
                    <div className="overflow-x-auto">
                      <table className="table text-sm">
                        <thead>
                          <tr>
                            <th>Account Number</th>
                            <th>Display Name</th>
                            <th className="text-right">Positions</th>
                            <th className="text-right">Total Value</th>
                          </tr>
                        </thead>
                        <tbody>
                          {inceptionPreview.accounts_summary.map((acct: any) => (
                            <tr key={acct.account_number}>
                              <td className="font-mono">{acct.account_number}</td>
                              <td>{acct.display_name}</td>
                              <td className="text-right">{acct.position_count}</td>
                              <td className="text-right">{formatCurrency(acct.total_value)}</td>
                            </tr>
                          ))}
                        </tbody>
                      </table>
                    </div>
                  </div>
                )}

                {/* Preview Rows */}
                <div className="overflow-x-auto">
                  <table className="table text-sm">
                    <thead>
                      <tr>
                        <th>Row</th>
                        <th>Account</th>
                        <th>Symbol</th>
                        <th>Asset Name</th>
                        <th className="text-right">Units</th>
                        <th className="text-right">Price</th>
                        <th className="text-right">Market Value</th>
                        <th>Errors</th>
                      </tr>
                    </thead>
                    <tbody>
                      {inceptionPreview.preview_rows?.map((row: any) => (
                        <tr key={row.row_num} className={row.errors?.length > 0 ? 'bg-red-50' : ''}>
                          <td>{row.row_num}</td>
                          <td>{row.data['Account Number']}</td>
                          <td className="font-mono">{row.data['Symbol']}</td>
                          <td>{row.data['Asset Name']}</td>
                          <td className="text-right">{formatNumber(row.data['Units'])}</td>
                          <td className="text-right">{formatCurrency(row.data['Price'])}</td>
                          <td className="text-right">{formatCurrency(row.data['Market Value'])}</td>
                          <td>
                            {row.errors?.length > 0 && (
                              <span className="text-red-600 text-xs">{row.errors.join(', ')}</span>
                            )}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </div>
              </div>
            )}

            {/* Import Result */}
            {inceptionResult && (
              <div className="card">
                <h2 className="text-lg font-semibold mb-4">Import Complete</h2>
                <div className="space-y-2">
                  <div className="p-3 bg-green-50 rounded">
                    <span className="font-semibold text-green-800">{inceptionResult.message}</span>
                  </div>
                  <div className="grid grid-cols-2 md:grid-cols-4 gap-4 mt-4">
                    <div className="text-center p-3 bg-gray-50 rounded">
                      <div className="text-2xl font-bold text-blue-600">{inceptionResult.accounts_created + inceptionResult.accounts_updated}</div>
                      <div className="text-xs text-gray-500">Accounts</div>
                    </div>
                    <div className="text-center p-3 bg-gray-50 rounded">
                      <div className="text-2xl font-bold text-green-600">{inceptionResult.positions_created}</div>
                      <div className="text-xs text-gray-500">Positions</div>
                    </div>
                    <div className="text-center p-3 bg-gray-50 rounded">
                      <div className="text-2xl font-bold">{inceptionResult.securities_created}</div>
                      <div className="text-xs text-gray-500">New Securities</div>
                    </div>
                    <div className="text-center p-3 bg-gray-50 rounded">
                      <div className="text-2xl font-bold">{formatCurrency(inceptionResult.total_value)}</div>
                      <div className="text-xs text-gray-500">Total Value</div>
                    </div>
                  </div>

                  {inceptionResult.errors && inceptionResult.errors.length > 0 && (
                    <div className="mt-4">
                      <h3 className="font-semibold mb-2 text-red-700">Errors:</h3>
                      <div className="space-y-1 text-sm max-h-40 overflow-y-auto bg-red-50 p-3 rounded">
                        {inceptionResult.errors.map((err: string, idx: number) => (
                          <div key={idx} className="text-red-600">{err}</div>
                        ))}
                      </div>
                    </div>
                  )}
                </div>
              </div>
            )}

            {/* Existing Inception Data */}
            <div className="card">
              <div className="flex justify-between items-center mb-4">
                <h2 className="text-lg font-semibold">Accounts with Inception Data</h2>
                <button
                  onClick={loadInceptionData}
                  className="text-sm text-blue-600 hover:text-blue-800"
                >
                  Refresh
                </button>
              </div>
              <table className="table">
                <thead>
                  <tr>
                    <th>Account Number</th>
                    <th>Display Name</th>
                    <th>Inception Date</th>
                    <th className="text-right">Positions</th>
                    <th className="text-right">Total Value</th>
                    <th>Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {inceptionAccounts.map((acct) => (
                    <tr key={acct.account_id}>
                      <td className="font-mono">{acct.account_number}</td>
                      <td>{acct.display_name}</td>
                      <td>{acct.inception_date}</td>
                      <td className="text-right">{acct.position_count}</td>
                      <td className="text-right">{formatCurrency(acct.total_value)}</td>
                      <td>
                        <button
                          onClick={() => handleDeleteInception(acct.account_id, acct.account_number)}
                          className="text-red-600 hover:text-red-800 text-sm font-medium"
                        >
                          Delete
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>

              {inceptionAccounts.length === 0 && (
                <div className="text-center py-8 text-gray-500">
                  No accounts have inception data. Upload an inception CSV to establish historical starting points.
                </div>
              )}
            </div>

            {/* Instructions */}
            <div className="card bg-gray-50">
              <h2 className="text-lg font-semibold mb-4">CSV Format Requirements</h2>
              <div className="text-sm text-gray-700 space-y-2">
                <p><strong>Required Columns:</strong></p>
                <ul className="list-disc list-inside ml-4 space-y-1">
                  <li><code className="bg-gray-200 px-1 rounded">Account Number</code> - Account identifier</li>
                  <li><code className="bg-gray-200 px-1 rounded">Account Display Name</code> - Human-readable account name</li>
                  <li><code className="bg-gray-200 px-1 rounded">Class</code> - Asset class (Equity, ETF, Option, etc.)</li>
                  <li><code className="bg-gray-200 px-1 rounded">Asset Name</code> - Full security name</li>
                  <li><code className="bg-gray-200 px-1 rounded">Symbol</code> - Ticker symbol</li>
                  <li><code className="bg-gray-200 px-1 rounded">Units</code> - Number of shares</li>
                  <li><code className="bg-gray-200 px-1 rounded">Price</code> - Price per share at inception</li>
                  <li><code className="bg-gray-200 px-1 rounded">Market Value</code> - Total position value</li>
                  <li><code className="bg-gray-200 px-1 rounded">Inception Date</code> - The inception date (e.g., 12/31/2020)</li>
                </ul>
                <p className="mt-4"><strong>Notes:</strong></p>
                <ul className="list-disc list-inside ml-4 space-y-1">
                  <li>All positions must have the same inception date</li>
                  <li>Only positions with Units {'>'} 0 will be imported</li>
                  <li>Importing will replace any existing inception data for affected accounts</li>
                  <li>After importing inception data, upload transactions from after the inception date</li>
                </ul>
              </div>
            </div>
          </div>
        )}
      </div>
    </Layout>
  );
}
