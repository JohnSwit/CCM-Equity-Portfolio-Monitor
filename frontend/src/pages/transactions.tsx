import { useState, useEffect } from 'react';
import dynamic from 'next/dynamic';
import Layout from '@/components/Layout';
import { api } from '@/lib/api';
import { format } from 'date-fns';

const Select = dynamic(() => import('react-select'), { ssr: false }) as any;

const selectStyles = {
  control: (base: any, state: any) => ({
    ...base,
    borderColor: state.isFocused ? '#3b82f6' : '#e4e4e7',
    boxShadow: state.isFocused ? '0 0 0 2px rgba(59, 130, 246, 0.1)' : 'none',
    borderRadius: '0.5rem',
    minHeight: '42px',
    '&:hover': { borderColor: '#a1a1aa' },
  }),
  option: (base: any, state: any) => ({
    ...base,
    backgroundColor: state.isSelected ? '#3b82f6' : state.isFocused ? '#f4f4f5' : 'white',
    color: state.isSelected ? 'white' : '#27272a',
    padding: '8px 12px',
  }),
  menu: (base: any) => ({
    ...base,
    borderRadius: '0.5rem',
    boxShadow: '0 10px 15px -3px rgb(0 0 0 / 0.1)',
    border: '1px solid #e4e4e7',
  }),
};

export default function TransactionsPage() {
  const [transactions, setTransactions] = useState<any[]>([]);
  const [accounts, setAccounts] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);
  const [totalCount, setTotalCount] = useState(0);
  const [selectedTransactions, setSelectedTransactions] = useState<Set<number>>(new Set());
  const [selectAll, setSelectAll] = useState(false);

  // Filters
  const [selectedAccount, setSelectedAccount] = useState<any>(null);
  const [symbolFilter, setSymbolFilter] = useState('');
  const [limit, setLimit] = useState(1000);
  const [offset, setOffset] = useState(0);

  useEffect(() => {
    loadAccounts();
    loadTransactions();
  }, []);

  useEffect(() => {
    loadTransactions();
  }, [selectedAccount, symbolFilter, limit, offset]);

  const loadAccounts = async () => {
    try {
      const data = await api.getAccountsWithTransactionCounts();
      setAccounts(data);
    } catch (error) {
      console.error('Failed to load accounts:', error);
    }
  };

  const loadTransactions = async () => {
    setLoading(true);
    try {
      const params: any = { limit, offset };
      if (selectedAccount) params.account_id = selectedAccount.id;
      if (symbolFilter) params.symbol = symbolFilter;

      const data = await api.getTransactions(params);
      setTransactions(data.transactions);
      setTotalCount(data.total_count);
      setSelectedTransactions(new Set()); // Clear selection when reloading
      setSelectAll(false);
    } catch (error) {
      console.error('Failed to load transactions:', error);
      alert('Failed to load transactions');
    } finally {
      setLoading(false);
    }
  };

  const handleDeleteTransaction = async (transactionId: number, symbol: string, accountNumber: string) => {
    if (!confirm(`Delete this transaction for ${symbol} in account ${accountNumber}?\n\nThis will automatically recompute all analytics.`)) {
      return;
    }

    try {
      const result = await api.deleteTransaction(transactionId);
      alert(result.message);
      await loadTransactions();
      await loadAccounts(); // Refresh counts
    } catch (error: any) {
      alert('Delete failed: ' + (error.response?.data?.detail || error.message));
    }
  };

  const handleDeleteAllAccountTransactions = async (accountId: number, accountNumber: string, transactionCount: number) => {
    if (!confirm(`Delete ALL ${transactionCount} transactions for account ${accountNumber}?\n\nThis cannot be undone and will automatically recompute all analytics.`)) {
      return;
    }

    try {
      const result = await api.deleteAllAccountTransactions(accountId);
      alert(result.message);
      await loadTransactions();
      await loadAccounts(); // Refresh counts
    } catch (error: any) {
      alert('Delete failed: ' + (error.response?.data?.detail || error.message));
    }
  };

  const handleBulkDelete = async () => {
    if (selectedTransactions.size === 0) {
      alert('No transactions selected');
      return;
    }

    if (!confirm(`Delete ${selectedTransactions.size} selected transactions?\n\nThis cannot be undone and will automatically recompute all analytics.`)) {
      return;
    }

    try {
      const result = await api.deleteTransactionsBulk(Array.from(selectedTransactions));
      alert(result.message);
      await loadTransactions();
      await loadAccounts(); // Refresh counts
    } catch (error: any) {
      alert('Bulk delete failed: ' + (error.response?.data?.detail || error.message));
    }
  };

  const toggleTransaction = (id: number) => {
    const newSelected = new Set(selectedTransactions);
    if (newSelected.has(id)) {
      newSelected.delete(id);
    } else {
      newSelected.add(id);
    }
    setSelectedTransactions(newSelected);
  };

  const toggleSelectAll = () => {
    if (selectAll) {
      setSelectedTransactions(new Set());
    } else {
      setSelectedTransactions(new Set(transactions.map(t => t.id)));
    }
    setSelectAll(!selectAll);
  };

  const accountOptions = accounts.map(a => ({
    value: a,
    label: `${a.account_number} - ${a.account_name} (${a.transaction_count} txns)`
  }));

  const formatCurrency = (value: number | null) => {
    if (value === null) return 'N/A';
    return new Intl.NumberFormat('en-US', {
      style: 'currency',
      currency: 'USD',
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    }).format(value);
  };

  const totalPages = Math.ceil(totalCount / limit);
  const currentPage = Math.floor(offset / limit) + 1;

  return (
    <Layout>
      <div className="space-y-6">
        {/* Page Header */}
        <div>
          <h1 className="text-2xl font-bold text-zinc-900">Transaction Management</h1>
          <p className="text-sm text-zinc-500 mt-1">
            View, filter, and delete transactions. All deletions automatically recompute analytics across all levels.
          </p>
        </div>

        {/* Filters */}
        <div className="card">
          <div className="card-header">
            <h2 className="card-title">Filters</h2>
          </div>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <label className="label">Account</label>
              <Select
                options={accountOptions}
                value={accountOptions.find(o => o.value === selectedAccount)}
                onChange={(option) => {
                  setSelectedAccount(option?.value || null);
                  setOffset(0);
                }}
                styles={selectStyles}
                isClearable
                placeholder="All accounts..."
                className="w-full"
              />
            </div>
            <div>
              <label className="label">Symbol</label>
              <input
                type="text"
                value={symbolFilter}
                onChange={(e) => {
                  setSymbolFilter(e.target.value);
                  setOffset(0);
                }}
                placeholder="Filter by symbol (e.g., AAPL)"
                className="input"
              />
            </div>
          </div>

          <div className="mt-4 flex justify-between items-center">
            <div className="flex gap-2 items-center">
              <button
                onClick={loadTransactions}
                className="btn btn-secondary"
              >
                <svg className="h-4 w-4" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                  <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={2} d="M4 4v5h.582m15.356 2A8.001 8.001 0 004.582 9m0 0H9m11 11v-5h-.581m0 0a8.003 8.003 0 01-15.357-2m15.357 2H15" />
                </svg>
                Refresh
              </button>
              {selectedAccount && (
                <button
                  onClick={() => handleDeleteAllAccountTransactions(
                    selectedAccount.id,
                    selectedAccount.account_number,
                    selectedAccount.transaction_count
                  )}
                  className="btn btn-danger"
                >
                  Delete All for {selectedAccount.account_number}
                </button>
              )}
            </div>
            <div className="text-sm text-zinc-500">
              Total: <span className="font-medium text-zinc-700">{totalCount.toLocaleString()}</span> transactions
            </div>
          </div>
        </div>

        {/* Bulk Actions */}
        {selectedTransactions.size > 0 && (
          <div className="card bg-blue-50 border-blue-200">
            <div className="flex justify-between items-center">
              <div className="text-blue-800 font-medium">
                {selectedTransactions.size} transaction(s) selected
              </div>
              <button
                onClick={handleBulkDelete}
                className="btn btn-danger"
              >
                Delete Selected
              </button>
            </div>
          </div>
        )}

        {/* Transactions Table */}
        <div className="card p-0 overflow-hidden">
          <div className="px-6 py-4 border-b border-zinc-100">
            <h2 className="text-base font-semibold text-zinc-900">
              Transactions
              <span className="text-sm font-normal text-zinc-500 ml-2">
                (Page {currentPage} of {totalPages || 1})
              </span>
            </h2>
          </div>

          {loading ? (
            <div className="flex items-center justify-center py-12">
              <div className="flex items-center gap-3 text-zinc-500">
                <svg className="loading-spinner" viewBox="0 0 24 24" fill="none">
                  <circle className="opacity-25" cx="12" cy="12" r="10" stroke="currentColor" strokeWidth="4" />
                  <path className="opacity-75" fill="currentColor" d="M4 12a8 8 0 018-8V0C5.373 0 0 5.373 0 12h4z" />
                </svg>
                <span>Loading transactions...</span>
              </div>
            </div>
          ) : transactions.length === 0 ? (
            <div className="empty-state py-12">
              <svg className="empty-state-icon" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M9 5H7a2 2 0 00-2 2v12a2 2 0 002 2h10a2 2 0 002-2V7a2 2 0 00-2-2h-2M9 5a2 2 0 002 2h2a2 2 0 002-2M9 5a2 2 0 012-2h2a2 2 0 012 2" />
              </svg>
              <p className="empty-state-title">No transactions found</p>
              <p className="empty-state-description">Try adjusting your filters.</p>
            </div>
          ) : (
            <>
              <div className="table-container mx-0">
                <table className="table">
                  <thead>
                    <tr>
                      <th className="w-12">
                        <input
                          type="checkbox"
                          checked={selectAll}
                          onChange={toggleSelectAll}
                          className="rounded border-zinc-300"
                        />
                      </th>
                      <th>Date</th>
                      <th>Account</th>
                      <th>Symbol</th>
                      <th>Asset Name</th>
                      <th>Type</th>
                      <th className="text-right">Quantity</th>
                      <th className="text-right">Price</th>
                      <th className="text-right">Amount</th>
                      <th className="text-center">Actions</th>
                    </tr>
                  </thead>
                  <tbody className="tabular-nums">
                    {transactions.map((txn) => (
                      <tr key={txn.id} className={selectedTransactions.has(txn.id) ? 'bg-blue-50/50' : ''}>
                        <td>
                          <input
                            type="checkbox"
                            checked={selectedTransactions.has(txn.id)}
                            onChange={() => toggleTransaction(txn.id)}
                            className="rounded border-zinc-300"
                          />
                        </td>
                        <td className="whitespace-nowrap text-zinc-600">
                          {format(new Date(txn.trade_date), 'MMM d, yyyy')}
                        </td>
                        <td className="whitespace-nowrap">
                          <div className="text-sm">
                            <div className="font-medium text-zinc-900">{txn.account_number}</div>
                            <div className="text-zinc-500 text-xs">{txn.account_name}</div>
                          </div>
                        </td>
                        <td className="font-semibold text-zinc-900">{txn.symbol}</td>
                        <td className="max-w-xs truncate text-zinc-600">{txn.asset_name}</td>
                        <td>
                          <span className={`badge ${
                            txn.transaction_type === 'BUY' ? 'badge-success' :
                            txn.transaction_type === 'SELL' ? 'badge-danger' :
                            'badge-neutral'
                          }`}>
                            {txn.transaction_type}
                          </span>
                        </td>
                        <td className="text-right">{txn.quantity.toFixed(4)}</td>
                        <td className="text-right">{formatCurrency(txn.price)}</td>
                        <td className="text-right font-medium">{formatCurrency(txn.amount)}</td>
                        <td className="text-center">
                          <button
                            onClick={() => handleDeleteTransaction(txn.id, txn.symbol, txn.account_number)}
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

              {/* Pagination */}
              <div className="px-6 py-4 border-t border-zinc-100 flex justify-between items-center">
                <div className="text-sm text-zinc-500">
                  Showing {offset + 1} to {Math.min(offset + limit, totalCount)} of {totalCount.toLocaleString()}
                </div>
                <div className="flex gap-2">
                  <button
                    onClick={() => setOffset(Math.max(0, offset - limit))}
                    disabled={offset === 0}
                    className="btn btn-secondary btn-sm"
                  >
                    Previous
                  </button>
                  <button
                    onClick={() => setOffset(offset + limit)}
                    disabled={offset + limit >= totalCount}
                    className="btn btn-secondary btn-sm"
                  >
                    Next
                  </button>
                </div>
              </div>
            </>
          )}
        </div>
      </div>
    </Layout>
  );
}
