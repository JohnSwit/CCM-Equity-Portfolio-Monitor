import axios, { AxiosInstance } from 'axios';

const API_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

class APIClient {
  private client: AxiosInstance;

  constructor() {
    this.client = axios.create({
      baseURL: API_URL,
      headers: {
        'Content-Type': 'application/json',
      },
    });

    // Add auth token to requests
    this.client.interceptors.request.use((config) => {
      const token = localStorage.getItem('token');
      if (token) {
        config.headers.Authorization = `Bearer ${token}`;
      }
      return config;
    });

    // Handle 401 errors
    this.client.interceptors.response.use(
      (response) => response,
      (error) => {
        if (error.response?.status === 401) {
          localStorage.removeItem('token');
          window.location.href = '/login';
        }
        return Promise.reject(error);
      }
    );
  }

  // Auth
  async login(email: string, password: string) {
    const response = await this.client.post('/auth/login', { email, password });
    return response.data;
  }

  async getMe() {
    const response = await this.client.get('/auth/me');
    return response.data;
  }

  // Accounts & Groups
  async getAccounts(search?: string) {
    const response = await this.client.get('/accounts', {
      params: { search },
    });
    return response.data;
  }

  async getGroups() {
    const response = await this.client.get('/groups');
    return response.data;
  }

  async createGroup(data: { name: string; type: string }) {
    const response = await this.client.post('/groups', data);
    return response.data;
  }

  async addGroupMembers(groupId: number, accountIds: number[]) {
    const response = await this.client.post(`/groups/${groupId}/members`, {
      account_ids: accountIds,
    });
    return response.data;
  }

  async removeGroupMember(groupId: number, accountId: number) {
    const response = await this.client.delete(`/groups/${groupId}/members/${accountId}`);
    return response.data;
  }

  async getAllViews() {
    const response = await this.client.get('/views');
    return response.data;
  }

  // Analytics
  async getSummary(viewType: string, viewId: number) {
    const response = await this.client.get('/analytics/summary', {
      params: { view_type: viewType, view_id: viewId },
    });
    return response.data;
  }

  async getReturns(viewType: string, viewId: number, startDate?: string, endDate?: string) {
    const response = await this.client.get('/analytics/returns', {
      params: { view_type: viewType, view_id: viewId, start_date: startDate, end_date: endDate },
    });
    return response.data;
  }

  async getHoldings(viewType: string, viewId: number, asOfDate?: string) {
    const response = await this.client.get('/analytics/holdings', {
      params: { view_type: viewType, view_id: viewId, as_of_date: asOfDate },
    });
    return response.data;
  }

  async getRisk(viewType: string, viewId: number, asOfDate?: string) {
    const response = await this.client.get('/analytics/risk', {
      params: { view_type: viewType, view_id: viewId, as_of_date: asOfDate },
    });
    return response.data;
  }

  async getBenchmarkMetrics(viewType: string, viewId: number, benchmark: string, window: number = 252) {
    const response = await this.client.get('/analytics/benchmark', {
      params: { view_type: viewType, view_id: viewId, benchmark, window },
    });
    return response.data;
  }

  async getFactorExposures(viewType: string, viewId: number, factorSet: string = 'STYLE7', window: number = 252) {
    const response = await this.client.get('/analytics/factors', {
      params: { view_type: viewType, view_id: viewId, factor_set: factorSet, window },
    });
    return response.data;
  }

  async getUnpricedInstruments(asOfDate?: string) {
    const response = await this.client.get('/analytics/unpriced-instruments', {
      params: { as_of_date: asOfDate },
    });
    return response.data;
  }

  async getBenchmarkReturns(benchmarkCodes: string[], startDate?: string, endDate?: string) {
    const response = await this.client.get('/analytics/benchmark-returns', {
      params: { benchmark_codes: benchmarkCodes.join(','), start_date: startDate, end_date: endDate },
    });
    return response.data;
  }

  // Imports
  async importBDTransactions(file: File, mode: 'preview' | 'commit') {
    const formData = new FormData();
    formData.append('file', file);

    const response = await this.client.post(
      `/imports/blackdiamond/transactions?mode=${mode}`,
      formData,
      {
        headers: {
          'Content-Type': 'multipart/form-data',
        },
      }
    );
    return response.data;
  }

  async getImportHistory(limit: number = 50) {
    const response = await this.client.get('/imports', {
      params: { limit },
    });
    return response.data;
  }

  async deleteImport(importId: number) {
    const response = await this.client.delete(`/imports/${importId}`);
    return response.data;
  }

  // Baskets
  async getBaskets() {
    const response = await this.client.get('/baskets');
    return response.data;
  }

  async createBasket(data: any) {
    const response = await this.client.post('/baskets', data);
    return response.data;
  }

  async updateBasket(basketId: number, data: any) {
    const response = await this.client.put(`/baskets/${basketId}`, data);
    return response.data;
  }

  // Jobs
  async runJob(jobName: string) {
    const response = await this.client.post(`/jobs/run?job_name=${jobName}`);
    return response.data;
  }
}

export const api = new APIClient();
