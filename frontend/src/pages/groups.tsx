import { useState, useEffect } from 'react';
import Layout from '@/components/Layout';
import { api } from '@/lib/api';
import Select from 'react-select';

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
        <div className="flex justify-between items-center">
          <h1 className="text-2xl font-bold">Groups Management</h1>
          <button
            onClick={() => setShowCreate(!showCreate)}
            className="btn btn-primary"
          >
            {showCreate ? 'Cancel' : 'Create Group'}
          </button>
        </div>

        {/* Create Group Form */}
        {showCreate && (
          <div className="card">
            <h2 className="text-lg font-semibold mb-4">Create New Group</h2>
            <form onSubmit={handleCreateGroup} className="space-y-4">
              <div>
                <label className="block text-sm font-medium mb-1">Group Name</label>
                <input
                  type="text"
                  value={newGroupName}
                  onChange={(e) => setNewGroupName(e.target.value)}
                  className="input"
                  required
                />
              </div>

              <div>
                <label className="block text-sm font-medium mb-1">Group Type</label>
                <select
                  value={newGroupType}
                  onChange={(e) => setNewGroupType(e.target.value)}
                  className="input"
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
          <h2 className="text-lg font-semibold mb-4">Existing Groups</h2>
          <table className="table">
            <thead>
              <tr>
                <th>Name</th>
                <th>Type</th>
                <th>Members</th>
                <th>Actions</th>
              </tr>
            </thead>
            <tbody>
              {groups.map((group) => (
                <tr key={group.id}>
                  <td className="font-semibold">{group.name}</td>
                  <td className="capitalize">{group.type}</td>
                  <td>{group.member_count}</td>
                  <td>
                    <button
                      onClick={() => setSelectedGroup(group)}
                      className="btn btn-secondary text-sm"
                    >
                      Manage Members
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>

          {groups.length === 0 && (
            <div className="text-center py-8 text-gray-500">
              No groups yet. Create your first group to organize accounts.
            </div>
          )}
        </div>

        {/* Manage Members */}
        {selectedGroup && (
          <div className="card">
            <h2 className="text-lg font-semibold mb-4">
              Add Members to {selectedGroup.name}
            </h2>

            <div className="space-y-4">
              <div>
                <label className="block text-sm font-medium mb-1">Select Accounts</label>
                <Select
                  isMulti
                  options={accountOptions}
                  value={selectedAccounts}
                  onChange={(selected) => setSelectedAccounts(selected as any[])}
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
