import { useState, useEffect, useRef, useCallback } from 'react';
import { useRouter } from 'next/router';
import Layout from '../components/Layout';
import { useAuth } from '../contexts/AuthContext';
import { api } from '../lib/api';

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

export default function BulkImportPage() {
  const router = useRouter();
  const { user, loading: authLoading } = useAuth();
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const fileInputRef = useRef<HTMLInputElement>(null);

  // Job tracking
  const [jobs, setJobs] = useState<BulkImportJob[]>([]);
  const [activeJob, setActiveJob] = useState<BulkImportJob | null>(null);
  const [batchSummary, setBatchSummary] = useState<BatchSummary | null>(null);

  // Upload options
  const [batchSize, setBatchSize] = useState(5000);
  const [skipAnalytics, setSkipAnalytics] = useState(true);
  const [validateOnly, setValidateOnly] = useState(false);

  // Auto-refresh for active jobs
  const [autoRefresh, setAutoRefresh] = useState(true);
  const refreshIntervalRef = useRef<NodeJS.Timeout | null>(null);

  useEffect(() => {
    if (!authLoading && !user) {
      router.push('/login');
    }
  }, [authLoading, user, router]);

  useEffect(() => {
    if (user) {
      loadJobs();
    }
  }, [user]);

  // Auto-refresh for active jobs
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

  const loadJobs = async () => {
    try {
      const data = await api.listBulkImports(undefined, 50);
      setJobs(data.jobs || []);
    } catch (err: any) {
      console.error('Failed to load jobs:', err);
    }
  };

  const refreshActiveJob = useCallback(async () => {
    if (!activeJob) return;
    try {
      const data = await api.getBulkImportStatus(activeJob.job_id, true);
      setActiveJob(data);
      setBatchSummary(data.batch_summary || null);

      // Update in jobs list
      setJobs(prev => prev.map(j => j.job_id === data.job_id ? data : j));
    } catch (err) {
      console.error('Failed to refresh job:', err);
    }
  }, [activeJob?.job_id]);

  const handleFileUpload = async (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0];
    if (!file) return;

    setLoading(true);
    setError(null);

    try {
      const result = await api.startBulkImport(file, {
        batchSize,
        skipAnalytics,
        validateOnly,
      });

      // Set as active job
      const jobData = await api.getBulkImportStatus(result.job_id, true);
      setActiveJob(jobData);
      setBatchSummary(jobData.batch_summary || null);

      // Add to jobs list
      setJobs(prev => [jobData, ...prev]);

    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to start import');
    } finally {
      setLoading(false);
      if (fileInputRef.current) {
        fileInputRef.current.value = '';
      }
    }
  };

  const handlePause = async () => {
    if (!activeJob) return;
    try {
      await api.pauseBulkImport(activeJob.job_id);
      refreshActiveJob();
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to pause job');
    }
  };

  const handleResume = async () => {
    if (!activeJob) return;
    try {
      await api.resumeBulkImport(activeJob.job_id);
      refreshActiveJob();
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to resume job');
    }
  };

  const handleCancel = async () => {
    if (!activeJob) return;
    if (!confirm('Are you sure you want to cancel this import?')) return;
    try {
      await api.cancelBulkImport(activeJob.job_id);
      refreshActiveJob();
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to cancel job');
    }
  };

  const handleRetryFailed = async () => {
    if (!activeJob) return;
    try {
      await api.retryFailedBatches(activeJob.job_id);
      refreshActiveJob();
    } catch (err: any) {
      setError(err.response?.data?.detail || 'Failed to retry batches');
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

  if (authLoading) {
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
        <div>
          <h1 className="text-2xl font-bold text-gray-900">Bulk Import</h1>
          <p className="text-sm text-gray-500 mt-1">
            Import large transaction files with progress tracking and resume capability
          </p>
        </div>

        {error && (
          <div className="bg-red-50 border border-red-200 text-red-700 px-4 py-3 rounded">
            {error}
            <button onClick={() => setError(null)} className="ml-4 text-red-500 hover:text-red-700">
              Dismiss
            </button>
          </div>
        )}

        {/* Upload Section */}
        <div className="card p-6">
          <h2 className="text-lg font-semibold mb-4">Start New Import</h2>

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
              <p className="text-xs text-gray-500 ml-2">(Recommended for bulk imports)</p>
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
                ref={fileInputRef}
                type="file"
                accept=".csv,.txt"
                onChange={handleFileUpload}
                className="hidden"
              />
              <button
                onClick={() => fileInputRef.current?.click()}
                disabled={loading}
                className="w-full px-4 py-2 bg-blue-600 text-white rounded hover:bg-blue-700 disabled:opacity-50"
              >
                {loading ? 'Uploading...' : 'Select CSV File'}
              </button>
            </div>
          </div>

          <p className="text-sm text-gray-600">
            Supports Black Diamond transaction CSV format. Large files (200K+ rows) are processed in batches
            with automatic checkpointing for fault tolerance.
          </p>
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
            <h2 className="text-lg font-semibold">Import History</h2>
            <button
              onClick={loadJobs}
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
                {jobs.map((job) => (
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
                            api.resumeBulkImport(job.job_id).then(() => loadJobs());
                          }}
                          className="text-xs text-blue-600 hover:text-blue-800"
                        >
                          Resume
                        </button>
                      )}
                    </td>
                  </tr>
                ))}
                {jobs.length === 0 && (
                  <tr>
                    <td colSpan={8} className="px-4 py-8 text-center text-gray-500">
                      No import jobs found
                    </td>
                  </tr>
                )}
              </tbody>
            </table>
          </div>
        </div>
      </div>
    </Layout>
  );
}
