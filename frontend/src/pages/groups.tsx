import { useState, useEffect } from 'react';
import dynamic from 'next/dynamic';
import Layout from '@/components/Layout';
import { api } from '@/lib/api';

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
  multiValue: (base: any) => ({
    ...base,
    backgroundColor: '#eff6ff',
    borderRadius: '0.375rem',
  }),
  multiValueLabel: (base: any) => ({
    ...base,
    color: '#1d4ed8',
  }),
  multiValueRemove: (base: any) => ({
    ...base,
    color: '#1d4ed8',
    '&:hover': {
      backgroundColor: '#dbeafe',
      color: '#1e40af',
    },
  }),
  menu: (base: any) => ({
    ...base,
    borderRadius: '0.5rem',
    boxShadow: '0 10px 15px -3px rgb(0 0 0 / 0.1)',
    border: '1px solid #e4e4e7',
  }),
};

export default function Groups() {
  const [groups, setGroups] = useState<any[]>([]);
  const [accounts, setAccounts] = useState<any[]>([]);
  const [showCreate, setShowCreate] = useState(false);
  const [newGroupName, setNewGroupName] = useState('');
  const [newGroupType, setNewGroupType] = useState('family');
  const [selectedGroup, setSelectedGroup] = useState<any>(null);
  const [selectedAccounts, setSelectedAccounts] = useState<any[]>([]);
  const [loading, setLoading] = useState(false);

  useEffect(() => {
    loadGroups();
    loadAccounts();
  }, []);

  const loadGroups = async () => {
    try {
      const data = await api.getGroups();
      setGroups(data.filter((g: any) => g.type !== 'firm'));
    } catch (error) {
      console.error('Failed to load groups:', error);
    }
  };

  const loadAccounts = async () => {
    try {
      const data = await api.getAccounts();
      setAccounts(data);
    } catch (error) {
      console.error('Failed to load accounts:', error);
    }
  };

  const handleCreateGroup = async (e: React.FormEvent) => {
    e.preventDefault();
    setLoading(true);

    try {
      await api.createGroup({
        name: newGroupName,
        type: newGroupType,
      });

      setNewGroupName('');
      setNewGroupType('family');
      setShowCreate(false);
      await loadGroups();
    } catch (error: any) {
      alert('Failed to create group: ' + error.response?.data?.detail);
    } finally {
      setLoading(false);
    }
  };

  const handleAddMembers = async () => {
    if (!selectedGroup || selectedAccounts.length === 0) return;

    setLoading(true);
    try {
      await api.addGroupMembers(
        selectedGroup.id,
        selectedAccounts.map((a) => a.id)
      );
      alert('Members added successfully');
      setSelectedAccounts([]);
      await loadGroups();
    } catch (error: any) {
      alert('Failed to add members: ' + error.response?.data?.detail);
    } finally {
      setLoading(false);
    }
  };

  const accountOptions = accounts.map((a) => ({
    value: a.id,
    label: `${a.display_name} (${a.account_number})`,
    ...a,
  }));

  return (
    <Layout>
      <div className="space-y-6">
        {/* Page Header */}
        <div className="flex justify-between items-center">
          <div>
            <h1 className="text-2xl font-bold text-zinc-900">Groups Management</h1>
            <p className="text-sm text-zinc-500 mt-1">
              Organize accounts into groups for aggregated portfolio views
            </p>
          </div>
          <button
            onClick={() => setShowCreate(!showCreate)}
            className={`btn ${showCreate ? 'btn-secondary' : 'btn-primary'}`}
          >
            {showCreate ? 'Cancel' : 'Create Group'}
          </button>
        </div>

        {/* Create Group Form */}
        {showCreate && (
          <div className="card">
            <div className="card-header">
              <h2 className="card-title">Create New Group</h2>
            </div>
            <form onSubmit={handleCreateGroup} className="space-y-4">
              <div>
                <label className="label">Group Name</label>
                <input
                  type="text"
                  value={newGroupName}
                  onChange={(e) => setNewGroupName(e.target.value)}
                  className="input"
                  placeholder="Enter group name"
                  required
                />
              </div>

              <div>
                <label className="label">Group Type</label>
                <select
                  value={newGroupType}
                  onChange={(e) => setNewGroupType(e.target.value)}
                  className="select"
                >
                  <option value="family">Family</option>
                  <option value="estate">Estate</option>
                  <option value="custom">Custom</option>
                </select>
              </div>

              <button type="submit" disabled={loading} className="btn btn-primary">
                {loading ? 'Creating...' : 'Create Group'}
              </button>
            </form>
          </div>
        )}

        {/* Groups List */}
        <div className="card">
          <div className="card-header">
            <h2 className="card-title">Existing Groups</h2>
          </div>
          {groups.length === 0 ? (
            <div className="empty-state">
              <svg className="empty-state-icon" fill="none" viewBox="0 0 24 24" stroke="currentColor">
                <path strokeLinecap="round" strokeLinejoin="round" strokeWidth={1.5} d="M17 20h5v-2a3 3 0 00-5.356-1.857M17 20H7m10 0v-2c0-.656-.126-1.283-.356-1.857M7 20H2v-2a3 3 0 015.356-1.857M7 20v-2c0-.656.126-1.283.356-1.857m0 0a5.002 5.002 0 019.288 0M15 7a3 3 0 11-6 0 3 3 0 016 0zm6 3a2 2 0 11-4 0 2 2 0 014 0zM7 10a2 2 0 11-4 0 2 2 0 014 0z" />
              </svg>
              <p className="empty-state-title">No groups yet</p>
              <p className="empty-state-description">Create your first group to organize accounts.</p>
            </div>
          ) : (
            <div className="table-container mx-0 -mb-6">
              <table className="table">
                <thead>
                  <tr>
                    <th>Name</th>
                    <th>Type</th>
                    <th className="text-right">Members</th>
                    <th className="text-center">Actions</th>
                  </tr>
                </thead>
                <tbody>
                  {groups.map((group) => (
                    <tr key={group.id}>
                      <td className="font-semibold text-zinc-900">{group.name}</td>
                      <td>
                        <span className="badge badge-neutral capitalize">{group.type}</span>
                      </td>
                      <td className="text-right tabular-nums">{group.member_count}</td>
                      <td className="text-center">
                        <button
                          onClick={() => setSelectedGroup(group)}
                          className="btn btn-secondary btn-sm"
                        >
                          Manage Members
                        </button>
                      </td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </div>
          )}
        </div>

        {/* Manage Members */}
        {selectedGroup && (
          <div className="card">
            <div className="card-header">
              <div>
                <h2 className="card-title">Add Members to {selectedGroup.name}</h2>
                <p className="card-subtitle">Select accounts to add to this group</p>
              </div>
              <button
                onClick={() => setSelectedGroup(null)}
                className="btn btn-ghost btn-sm"
              >
                Close
              </button>
            </div>

            <div className="space-y-4">
              <div>
                <label className="label">Select Accounts</label>
                <Select
                  isMulti
                  options={accountOptions}
                  value={selectedAccounts}
                  onChange={(selected) => setSelectedAccounts(selected as any[])}
                  styles={selectStyles}
                  placeholder="Search and select accounts..."
                />
              </div>

              <button
                onClick={handleAddMembers}
                disabled={loading || selectedAccounts.length === 0}
                className="btn btn-primary"
              >
                {loading ? 'Adding...' : `Add ${selectedAccounts.length} Account(s)`}
              </button>
            </div>
          </div>
        )}
      </div>
    </Layout>
  );
}
