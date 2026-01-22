import { useState, useEffect } from 'react';
import Layout from '@/components/Layout';
import { api } from '@/lib/api';
import { format } from 'date-fns';
import Select from 'react-select';

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
        {/* Header */}
        <div className="card">
          <h1 className="text-2xl font-bold mb-2">Transaction Management</h1>
          <p className="text-gray-600">
            View, filter, and delete transactions. All deletions automatically recompute analytics across all levels (account, group, firm).
          </p>
        </div>

        {/* Filters */}
        <div className="card">
          <h2 className="text-lg font-semibold mb-4">Filters</h2>
          <div className="grid grid-cols-1 md:grid-cols-2 gap-4">
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">Account</label>
              <Select
                options={accountOptions}
                value={accountOptions.find(o => o.value === selectedAccount)}
                onChange={(option) => {
                  setSelectedAccount(option?.value || null);
                  setOffset(0);
                }}
                isClearable
                placeholder="All accounts..."
                className="w-full"
              />
            </div>
            <div>
              <label className="block text-sm font-medium text-gray-700 mb-2">Symbol</label>
              <input
                type="text"
                value={symbolFilter}
                onChange={(e) => {
                  setSymbolFilter(e.target.value);
                  setOffset(0);
                }}
                placeholder="Filter by symbol (e.g., AAPL)"
                className="w-full px-3 py-2 border border-gray-300 rounded-md"
              />
            </div>
          </div>

          <div className="mt-4 flex justify-between items-center">
            <div className="flex gap-2 items-center">
              <button
                onClick={loadTransactions}
                className="btn-primary"
              >
                Refresh
              </button>
              {selectedAccount && (
                <button
                  onClick={() => handleDeleteAllAccountTransactions(
                    selectedAccount.id,
                    selectedAccount.account_number,
                    selectedAccount.transaction_count
                  )}
                  className="btn-danger"
                >
                  Delete All Transactions for {selectedAccount.account_number}
                </button>
              )}
            </div>
            <div className="text-sm text-gray-600">
              Total: {totalCount} transactions
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
                className="btn-danger"
              >
                Delete Selected
              </button>
            </div>
          </div>
        )}

        {/* Transactions Table */}
        <div className="card">
          <h2 className="text-lg font-semibold mb-4">
            Transactions
            <span className="text-sm font-normal text-gray-600 ml-2">
              (Page {currentPage} of {totalPages})
            </span>
          </h2>

          {loading ? (
            <div className="text-center py-8">Loading transactions...</div>
          ) : transactions.length === 0 ? (
            <div className="text-center py-8 text-gray-500">No transactions found</div>
          ) : (
            <>
              <div className="overflow-x-auto">
                <table className="table">
                  <thead>
                    <tr>
                      <th className="w-12">
                        <input
                          type="checkbox"
                          checked={selectAll}
                          onChange={toggleSelectAll}
                          className="rounded"
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
                      <th>Actions</th>
                    </tr>
                  </thead>
                  <tbody>
                    {transactions.map((txn) => (
                      <tr key={txn.id} className={selectedTransactions.has(txn.id) ? 'bg-blue-50' : ''}>
                        <td>
                          <input
                            type="checkbox"
                            checked={selectedTransactions.has(txn.id)}
                            onChange={() => toggleTransaction(txn.id)}
                            className="rounded"
                          />
                        </td>
                        <td className="whitespace-nowrap">
                          {format(new Date(txn.trade_date), 'MMM d, yyyy')}
                        </td>
                        <td className="whitespace-nowrap">
                          <div className="text-sm">
                            <div className="font-medium">{txn.account_number}</div>
                            <div className="text-gray-500 text-xs">{txn.account_name}</div>
                          </div>
                        </td>
                        <td className="font-semibold">{txn.symbol}</td>
                        <td className="max-w-xs truncate">{txn.asset_name}</td>
                        <td>
                          <span className={`px-2 py-1 rounded text-xs font-medium ${
                            txn.transaction_type === 'BUY' ? 'bg-green-100 text-green-800' :
                            txn.transaction_type === 'SELL' ? 'bg-red-100 text-red-800' :
                            'bg-gray-100 text-gray-800'
                          }`}>
                            {txn.transaction_type}
                          </span>
                        </td>
                        <td className="text-right">{txn.quantity.toFixed(4)}</td>
                        <td className="text-right">{formatCurrency(txn.price)}</td>
                        <td className="text-right">{formatCurrency(txn.amount)}</td>
                        <td>
                          <button
                            onClick={() => handleDeleteTransaction(txn.id, txn.symbol, txn.account_number)}
                            className="text-red-600 hover:text-red-800 text-sm font-medium"
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
              <div className="mt-4 flex justify-between items-center">
                <div className="text-sm text-gray-600">
                  Showing {offset + 1} to {Math.min(offset + limit, totalCount)} of {totalCount}
                </div>
                <div className="flex gap-2">
                  <button
                    onClick={() => setOffset(Math.max(0, offset - limit))}
                    disabled={offset === 0}
                    className="px-3 py-1 border rounded disabled:opacity-50 disabled:cursor-not-allowed"
                  >
                    Previous
                  </button>
                  <button
                    onClick={() => setOffset(offset + limit)}
                    disabled={offset + limit >= totalCount}
                    className="px-3 py-1 border rounded disabled:opacity-50 disabled:cursor-not-allowed"
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
