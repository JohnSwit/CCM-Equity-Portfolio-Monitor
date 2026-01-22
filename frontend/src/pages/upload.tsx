import { useState, useEffect } from 'react';
import Layout from '@/components/Layout';
import { api } from '@/lib/api';

export default function Upload() {
  const [file, setFile] = useState<File | null>(null);
  const [preview, setPreview] = useState<any>(null);
  const [importing, setImporting] = useState(false);
  const [importResult, setImportResult] = useState<any>(null);
  const [importHistory, setImportHistory] = useState<any[]>([]);
  const [runningJob, setRunningJob] = useState(false);
  const [jobResult, setJobResult] = useState<any>(null);

  useEffect(() => {
    loadImportHistory();
  }, []);

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

  return (
    <Layout>
      <div className="space-y-6">
        <h1 className="text-2xl font-bold">Upload Black Diamond Transactions</h1>

        {/* Upload Form */}
        <div className="card">
          <h2 className="text-lg font-semibold mb-4">Select CSV File</h2>
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
                    <span className="font-medium">{raw}</span> → {normalized}
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
    </Layout>
  );
}
