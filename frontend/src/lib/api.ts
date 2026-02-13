import axios, { AxiosInstance } from 'axios';

const API_URL = process.env.NEXT_PUBLIC_API_URL || 'http://localhost:8000';

// Simple TTL cache for GET requests to avoid redundant API calls
// when switching between views or re-rendering
interface CacheEntry {
  data: any;
  expiry: number;
}

class RequestCache {
  private cache = new Map<string, CacheEntry>();
  private defaultTTL: number;

  constructor(defaultTTLMs: number = 30000) {  // 30 second default
    this.defaultTTL = defaultTTLMs;
  }

  get(key: string): any | null {
    const entry = this.cache.get(key);
    if (!entry) return null;
    if (Date.now() > entry.expiry) {
      this.cache.delete(key);
      return null;
    }
    return entry.data;
  }

  set(key: string, data: any, ttlMs?: number): void {
    this.cache.set(key, {
      data,
      expiry: Date.now() + (ttlMs ?? this.defaultTTL),
    });
    // Evict old entries if cache grows too large
    if (this.cache.size > 200) {
      const now = Date.now();
      for (const [k, v] of this.cache) {
        if (now > v.expiry) this.cache.delete(k);
      }
    }
  }

  invalidate(pattern?: string): void {
    if (!pattern) {
      this.cache.clear();
      return;
    }
    for (const key of this.cache.keys()) {
      if (key.includes(pattern)) this.cache.delete(key);
    }
  }
}

class APIClient {
  private client: AxiosInstance;
  private requestCache = new RequestCache();

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

  // Cached GET helper - caches based on URL + params
  private async cachedGet(url: string, params?: any, ttlMs?: number): Promise<any> {
    const cacheKey = url + '?' + JSON.stringify(params || {});
    const cached = this.requestCache.get(cacheKey);
    if (cached) return cached;

    const response = await this.client.get(url, { params });
    this.requestCache.set(cacheKey, response.data, ttlMs);
    return response.data;
  }

  // Invalidate cache (call after mutations that change analytics data)
  invalidateCache(pattern?: string): void {
    this.requestCache.invalidate(pattern);
  }

  // Auth
  async login(email: string, password: string) {
    const response = await this.client.post('/auth/login', { email, password });
    this.requestCache.invalidate('/auth/me');
    return response.data;
  }

  async getMe() {
    return this.cachedGet('/auth/me', undefined, 120000);  // 2 min cache
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
    return this.cachedGet('/views', undefined, 60000);  // 1 min cache
  }

  // Analytics
  async getSummary(viewType: string, viewId: number) {
    return this.cachedGet('/analytics/summary', { view_type: viewType, view_id: viewId });
  }

  async getReturns(viewType: string, viewId: number, startDate?: string, endDate?: string) {
    return this.cachedGet('/analytics/returns', { view_type: viewType, view_id: viewId, start_date: startDate, end_date: endDate });
  }

  async getHoldings(viewType: string, viewId: number, asOfDate?: string) {
    return this.cachedGet('/analytics/holdings', { view_type: viewType, view_id: viewId, as_of_date: asOfDate });
  }

  async getRisk(viewType: string, viewId: number, asOfDate?: string) {
    return this.cachedGet('/analytics/risk', { view_type: viewType, view_id: viewId, as_of_date: asOfDate });
  }

  async getBenchmarkMetrics(viewType: string, viewId: number, benchmark: string, window: number = 252) {
    return this.cachedGet('/analytics/benchmark', { view_type: viewType, view_id: viewId, benchmark, window }, 60000);
  }

  async getFactorExposures(viewType: string, viewId: number, factorSet: string = 'STYLE7', window: number = 252) {
    return this.cachedGet('/analytics/factors', { view_type: viewType, view_id: viewId, factor_set: factorSet, window }, 60000);
  }

  async getUnpricedInstruments(asOfDate?: string) {
    return this.cachedGet('/analytics/unpriced-instruments', { as_of_date: asOfDate }, 60000);
  }

  async getBenchmarkReturns(benchmarkCodes: string[], startDate?: string, endDate?: string) {
    return this.cachedGet('/analytics/benchmark-returns',
      { benchmark_codes: benchmarkCodes.join(','), start_date: startDate, end_date: endDate },
      60000  // 1 min cache - benchmarks change less frequently
    );
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
    if (mode === 'commit') this.requestCache.invalidate();
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
    this.requestCache.invalidate();  // Clear all caches after jobs run
    return response.data;
  }

  async classifySecurities() {
    const response = await this.client.post('/jobs/classify-securities?unclassified_only=true');
    return response.data;
  }

  // Transactions
  async getTransactions(params?: {
    account_id?: number;
    account_number?: string;
    symbol?: string;
    start_date?: string;
    end_date?: string;
    limit?: number;
    offset?: number;
  }) {
    const response = await this.client.get('/transactions', { params });
    return response.data;
  }

  async getAccountsWithTransactionCounts() {
    return this.cachedGet('/transactions/accounts', undefined, 60000);
  }

  async deleteTransaction(transactionId: number) {
    const response = await this.client.delete(`/transactions/${transactionId}`);
    return response.data;
  }

  async deleteAllAccountTransactions(accountId: number) {
    const response = await this.client.delete(`/transactions/accounts/${accountId}/all`);
    return response.data;
  }

  async deleteTransactionsBulk(transactionIds: number[]) {
    const response = await this.client.delete('/transactions/bulk', {
      data: { transaction_ids: transactionIds },
    });
    return response.data;
  }

  // Portfolio Statistics
  async getContributionToReturns(viewType: string, viewId: number, startDate?: string, endDate?: string, topN: number = 20) {
    const response = await this.client.get('/portfolio-stats/contribution-to-returns', {
      params: { view_type: viewType, view_id: viewId, start_date: startDate, end_date: endDate, top_n: topN },
    });
    return response.data;
  }

  async getVolatilityMetrics(viewType: string, viewId: number, benchmark: string = 'SPY', window: number = 252) {
    return this.cachedGet('/portfolio-stats/volatility-metrics', { view_type: viewType, view_id: viewId, benchmark, window }, 60000);
  }

  async getDrawdownAnalysis(viewType: string, viewId: number) {
    return this.cachedGet('/portfolio-stats/drawdown-analysis', { view_type: viewType, view_id: viewId }, 60000);
  }

  async getVarCvar(viewType: string, viewId: number, confidenceLevels: string = '95,99', window: number = 252) {
    return this.cachedGet('/portfolio-stats/var-cvar', { view_type: viewType, view_id: viewId, confidence_levels: confidenceLevels, window }, 60000);
  }

  async getFactorAnalysis(viewType: string, viewId: number, asOfDate?: string) {
    return this.cachedGet('/portfolio-stats/factor-analysis', { view_type: viewType, view_id: viewId, as_of_date: asOfDate }, 60000);
  }

  async getComprehensiveStatistics(viewType: string, viewId: number, benchmark: string = 'SPY', window: number = 252) {
    return this.cachedGet('/portfolio-stats/comprehensive', { view_type: viewType, view_id: viewId, benchmark, window }, 60000);
  }

  // Phase 2: Advanced Analytics
  async getTurnoverAnalysis(viewType: string, viewId: number, startDate?: string, endDate?: string, period: string = 'monthly') {
    return this.cachedGet('/portfolio-stats/turnover', { view_type: viewType, view_id: viewId, start_date: startDate, end_date: endDate, period }, 60000);
  }

  async getSectorWeights(viewType: string, viewId: number, asOfDate?: string) {
    return this.cachedGet('/portfolio-stats/sector-weights', { view_type: viewType, view_id: viewId, as_of_date: asOfDate });
  }

  async getSectorComparison(viewType: string, viewId: number, benchmark: string = 'SP500', asOfDate?: string) {
    return this.cachedGet('/portfolio-stats/sector-comparison', { view_type: viewType, view_id: viewId, benchmark, as_of_date: asOfDate }, 60000);
  }

  async getBrinsonAttribution(viewType: string, viewId: number, benchmark: string = 'SP500', startDate?: string, endDate?: string) {
    const response = await this.client.get('/portfolio-stats/brinson-attribution', {
      params: { view_type: viewType, view_id: viewId, benchmark, start_date: startDate, end_date: endDate },
    });
    return response.data;
  }

  async getFactorAttribution(viewType: string, viewId: number, startDate?: string, endDate?: string) {
    const response = await this.client.get('/portfolio-stats/factor-attribution', {
      params: { view_type: viewType, view_id: viewId, start_date: startDate, end_date: endDate },
    });
    return response.data;
  }

  async getFactorCrowding(viewType: string, viewId: number) {
    const response = await this.client.get('/portfolio-stats/factor-crowding', {
      params: { view_type: viewType, view_id: viewId },
    });
    return response.data;
  }

  async getHistoricalFactorExposures(viewType: string, viewId: number, lookbackDays: number = 504, rollingWindow: number = 63) {
    const response = await this.client.get('/portfolio-stats/factor-historical', {
      params: { view_type: viewType, view_id: viewId, lookback_days: lookbackDays, rolling_window: rollingWindow },
    });
    return response.data;
  }

  async getFactorRiskDecomposition(viewType: string, viewId: number, startDate?: string, endDate?: string) {
    const response = await this.client.get('/portfolio-stats/factor-risk-decomposition', {
      params: { view_type: viewType, view_id: viewId, start_date: startDate, end_date: endDate },
    });
    return response.data;
  }

  // Data Management
  async refreshClassifications(limit?: number) {
    const response = await this.client.post('/data-management/refresh-classifications', null, {
      params: { limit },
    });
    return response.data;
  }

  async refreshSingleClassification(securityId: number) {
    const response = await this.client.post(`/data-management/refresh-classification/${securityId}`);
    return response.data;
  }

  async refreshSP500Benchmark() {
    const response = await this.client.post('/data-management/refresh-benchmark');
    return response.data;
  }

  async refreshFactorReturns(startDate?: string) {
    const response = await this.client.post('/data-management/refresh-factor-returns', null, {
      params: { start_date: startDate },
    });
    return response.data;
  }

  async getDataStatus() {
    const response = await this.client.get('/data-management/status');
    return response.data;
  }

  async getMissingClassifications(limit: number = 100) {
    const response = await this.client.get('/data-management/missing-classifications', {
      params: { limit },
    });
    return response.data;
  }

  // Factor Benchmarking + Attribution
  async getFactorModels() {
    const response = await this.client.get('/analytics/factor-models');
    return response.data;
  }

  async getFactorBenchmarking(
    viewType: string,
    viewId: number,
    modelCode: string = 'US_CORE',
    period: string = '1Y',
    useExcessReturns: boolean = false,
    useRobust: boolean = false,
    benchmarkCode?: string
  ) {
    const response = await this.client.get('/analytics/factor-benchmarking', {
      params: {
        view_type: viewType,
        view_id: viewId,
        model_code: modelCode,
        period,
        use_excess_returns: useExcessReturns,
        use_robust: useRobust,
        benchmark_code: benchmarkCode,
      },
    });
    return response.data;
  }

  async getFactorRollingAnalysis(
    viewType: string,
    viewId: number,
    modelCode: string = 'US_CORE',
    period: string = '1Y',
    windowDays: number = 63,
    useExcessReturns: boolean = false
  ) {
    const response = await this.client.get('/analytics/factor-rolling-analysis', {
      params: {
        view_type: viewType,
        view_id: viewId,
        model_code: modelCode,
        period,
        window_days: windowDays,
        use_excess_returns: useExcessReturns,
      },
    });
    return response.data;
  }

  async getFactorContributionOverTime(
    viewType: string,
    viewId: number,
    modelCode: string = 'US_CORE',
    period: string = '1Y',
    frequency: string = 'M',
    useExcessReturns: boolean = false
  ) {
    const response = await this.client.get('/analytics/factor-contribution-over-time', {
      params: {
        view_type: viewType,
        view_id: viewId,
        model_code: modelCode,
        period,
        frequency,
        use_excess_returns: useExcessReturns,
      },
    });
    return response.data;
  }

  async getAvailableBenchmarks() {
    const response = await this.client.get('/analytics/available-benchmarks');
    return response.data;
  }

  async refreshFactorData(modelCode: string = 'US_CORE', startDate?: string, endDate?: string) {
    const response = await this.client.post('/analytics/refresh-factor-data', null, {
      params: { model_code: modelCode, start_date: startDate, end_date: endDate },
    });
    return response.data;
  }

  async getFactorDataStatus(modelCode: string = 'US_CORE', period: string = '1Y') {
    const response = await this.client.get('/analytics/factor-data-status', {
      params: { model_code: modelCode, period },
    });
    return response.data;
  }

  // New Funds Allocation
  async parseIndustryCSV(file: File) {
    const formData = new FormData();
    formData.append('file', file);
    const response = await this.client.post('/new-funds/parse-industry-csv', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    });
    return response.data;
  }

  async parsePortfolioCSV(file: File) {
    const formData = new FormData();
    formData.append('file', file);
    const response = await this.client.post('/new-funds/parse-portfolio-csv', formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    });
    return response.data;
  }

  async getNewFundsAccounts() {
    const response = await this.client.get('/new-funds/accounts');
    return response.data;
  }

  async calculateAllocation(data: {
    total_amount: number;
    account_id: number;
    industries: any[];
    ticker_allocations?: any[];
  }) {
    const response = await this.client.post('/new-funds/calculate-allocation', data);
    return response.data;
  }

  async getTickerPrice(ticker: string) {
    const response = await this.client.post('/new-funds/get-ticker-price', null, {
      params: { ticker },
    });
    return response.data;
  }

  async calculateShares(ticker: string, dollarAmount: number) {
    const response = await this.client.post('/new-funds/calculate-shares', null, {
      params: { ticker, dollar_amount: dollarAmount },
    });
    return response.data;
  }

  async generateSchwabCSV(accountNumber: string, allocations: any[]) {
    const response = await this.client.post('/new-funds/generate-schwab-csv', {
      account_number: accountNumber,
      allocations,
    }, {
      responseType: 'blob',
    });
    return response.data;
  }

  async validateAllocation(data: {
    total_amount: number;
    account_id: number;
    industries: any[];
    ticker_allocations: any[];
  }) {
    const response = await this.client.post('/new-funds/validate-allocation', data);
    return response.data;
  }

  // Active Coverage
  async getAnalysts() {
    const response = await this.client.get('/coverage/analysts');
    return response.data;
  }

  async createAnalyst(name: string) {
    const response = await this.client.post('/coverage/analysts', { name });
    return response.data;
  }

  async initAnalysts() {
    const response = await this.client.post('/coverage/init-analysts');
    return response.data;
  }

  async getCoverageList(activeOnly: boolean = true) {
    const response = await this.client.get('/coverage', {
      params: { active_only: activeOnly },
    });
    return response.data;
  }

  async getCoverage(coverageId: number) {
    const response = await this.client.get(`/coverage/${coverageId}`);
    return response.data;
  }

  async createCoverage(data: {
    ticker: string;
    primary_analyst_id?: number;
    secondary_analyst_id?: number;
    model_path?: string;
    model_share_link?: string;
    notes?: string;
  }) {
    const response = await this.client.post('/coverage', data);
    return response.data;
  }

  async updateCoverage(coverageId: number, data: {
    primary_analyst_id?: number;
    secondary_analyst_id?: number;
    model_path?: string;
    model_share_link?: string;
    notes?: string;
    is_active?: boolean;
    model_updated?: boolean;
    thesis?: string;
    bull_case?: string;
    bear_case?: string;
    alert?: string;
  }) {
    const response = await this.client.put(`/coverage/${coverageId}`, data);
    return response.data;
  }

  async deleteCoverage(coverageId: number) {
    const response = await this.client.delete(`/coverage/${coverageId}`);
    return response.data;
  }

  async refreshModelData(coverageId: number, createSnapshot: boolean = true) {
    const response = await this.client.post(`/coverage/${coverageId}/refresh-model-data`, null, {
      params: { create_snapshot: createSnapshot },
    });
    return response.data;
  }

  async refreshAllModels() {
    const response = await this.client.post('/coverage/refresh-all-models');
    return response.data;
  }

  // Coverage Documents
  async getCoverageDocuments(coverageId: number) {
    const response = await this.client.get(`/coverage/${coverageId}/documents`);
    return response.data;
  }

  async addCoverageDocument(coverageId: number, data: {
    file_name: string;
    file_path: string;
    file_type?: string;
    file_size?: number;
    description?: string;
  }) {
    const response = await this.client.post(`/coverage/${coverageId}/documents`, null, {
      params: data,
    });
    return response.data;
  }

  async deleteCoverageDocument(coverageId: number, documentId: number) {
    const response = await this.client.delete(`/coverage/${coverageId}/documents/${documentId}`);
    return response.data;
  }

  // Coverage Snapshots
  async getCoverageSnapshots(coverageId: number) {
    const response = await this.client.get(`/coverage/${coverageId}/snapshots`);
    return response.data;
  }

  async getSnapshotDiff(coverageId: number, snapshotId: number) {
    const response = await this.client.get(`/coverage/${coverageId}/snapshots/${snapshotId}/diff`);
    return response.data;
  }

  async deleteCoverageSnapshot(coverageId: number, snapshotId: number) {
    const response = await this.client.delete(`/coverage/${coverageId}/snapshots/${snapshotId}`);
    return response.data;
  }

  // Idea Pipeline
  async getIdeas(activeOnly: boolean = true) {
    const response = await this.client.get('/ideas', {
      params: { active_only: activeOnly },
    });
    return response.data;
  }

  async getIdea(ideaId: number) {
    const response = await this.client.get(`/ideas/${ideaId}`);
    return response.data;
  }

  async createIdea(data: {
    ticker: string;
    primary_analyst_id?: number;
    secondary_analyst_id?: number;
    model_path?: string;
    model_share_link?: string;
    thesis?: string;
    next_steps?: string;
    notes?: string;
  }) {
    const response = await this.client.post('/ideas', data);
    return response.data;
  }

  async updateIdea(ideaId: number, data: {
    primary_analyst_id?: number;
    secondary_analyst_id?: number;
    model_path?: string;
    model_share_link?: string;
    initial_review_complete?: boolean;
    deep_dive_complete?: boolean;
    model_complete?: boolean;
    writeup_complete?: boolean;
    thesis?: string;
    bull_case?: string;
    bear_case?: string;
    next_steps?: string;
    notes?: string;
    is_active?: boolean;
  }) {
    const response = await this.client.put(`/ideas/${ideaId}`, data);
    return response.data;
  }

  async deleteIdea(ideaId: number) {
    const response = await this.client.delete(`/ideas/${ideaId}`);
    return response.data;
  }

  async refreshIdeaModelData(ideaId: number) {
    const response = await this.client.post(`/ideas/${ideaId}/refresh-model-data`);
    return response.data;
  }

  async getIdeaDocuments(ideaId: number) {
    const response = await this.client.get(`/ideas/${ideaId}/documents`);
    return response.data;
  }

  async uploadIdeaDocument(ideaId: number, file: File) {
    const formData = new FormData();
    formData.append('file', file);
    const response = await this.client.post(`/ideas/${ideaId}/documents`, formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    });
    return response.data;
  }

  async deleteIdeaDocument(ideaId: number, documentId: number) {
    const response = await this.client.delete(`/ideas/${ideaId}/documents/${documentId}`);
    return response.data;
  }

  getIdeaDocumentDownloadUrl(ideaId: number, documentId: number): string {
    const token = localStorage.getItem('token');
    return `${API_URL}/ideas/${ideaId}/documents/${documentId}/download?token=${token}`;
  }

  // Tax Optimization
  async buildTaxLots(accountId?: number) {
    const response = await this.client.post('/tax/build-lots', null, {
      params: accountId ? { account_id: accountId } : {},
    });
    return response.data;
  }

  async getTaxLots(params?: { account_id?: number; symbol?: string; include_closed?: boolean }) {
    const response = await this.client.get('/tax/lots', { params });
    return response.data;
  }

  async getTaxLotsBySymbol(symbol: string, accountId?: number) {
    const response = await this.client.get(`/tax/lots/${symbol}`, {
      params: accountId ? { account_id: accountId } : {},
    });
    return response.data;
  }

  async getRealizedGains(params?: { account_id?: number; tax_year?: number }) {
    const response = await this.client.get('/tax/realized-gains', { params });
    return response.data;
  }

  async getTaxSummary(params?: { account_id?: number; tax_year?: number }) {
    const response = await this.client.get('/tax/summary', { params });
    return response.data;
  }

  async getHarvestCandidates(params?: { account_id?: number; min_loss?: number }) {
    const response = await this.client.get('/tax/harvest-candidates', { params });
    return response.data;
  }

  async checkWashSale(accountId: number, symbol: string, tradeDate?: string) {
    const response = await this.client.get('/tax/wash-sale-check', {
      params: { account_id: accountId, symbol, trade_date: tradeDate },
    });
    return response.data;
  }

  async getTradeImpact(accountId: number, symbol: string, shares: number, price?: number) {
    const response = await this.client.get('/tax/trade-impact', {
      params: { account_id: accountId, symbol, shares, price },
    });
    return response.data;
  }

  async getSellSuggestions(accountId: number, symbol: string, shares: number, objective: string = 'minimize_tax') {
    const response = await this.client.get('/tax/sell-suggestions', {
      params: { account_id: accountId, symbol, shares, objective },
    });
    return response.data;
  }

  async getTaxAccounts() {
    const response = await this.client.get('/tax/accounts');
    return response.data;
  }

  async simulateSelectedLots(lotIds: number[]) {
    const response = await this.client.post('/tax/simulate-lots', { lot_ids: lotIds });
    return response.data;
  }

  // Bulk Import
  async startBulkImport(file: File, options?: { batchSize?: number; skipAnalytics?: boolean; validateOnly?: boolean }) {
    const formData = new FormData();
    formData.append('file', file);

    const params = new URLSearchParams();
    if (options?.batchSize) params.append('batch_size', options.batchSize.toString());
    if (options?.skipAnalytics !== undefined) params.append('skip_analytics', options.skipAnalytics.toString());
    if (options?.validateOnly !== undefined) params.append('validate_only', options.validateOnly.toString());

    const response = await this.client.post(`/imports/bulk/start?${params.toString()}`, formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    });
    return response.data;
  }

  async getBulkImportStatus(jobId: string, includeBatches: boolean = false) {
    const response = await this.client.get(`/imports/bulk/${jobId}/status`, {
      params: { include_batches: includeBatches },
    });
    return response.data;
  }

  async listBulkImports(status?: string, limit: number = 20) {
    const response = await this.client.get('/imports/bulk', {
      params: { status, limit },
    });
    return response.data;
  }

  async pauseBulkImport(jobId: string) {
    const response = await this.client.post(`/imports/bulk/${jobId}/pause`);
    return response.data;
  }

  async resumeBulkImport(jobId: string) {
    const response = await this.client.post(`/imports/bulk/${jobId}/resume`);
    return response.data;
  }

  async cancelBulkImport(jobId: string) {
    const response = await this.client.post(`/imports/bulk/${jobId}/cancel`);
    return response.data;
  }

  async getBulkImportErrors(jobId: string) {
    const response = await this.client.get(`/imports/bulk/${jobId}/errors`);
    return response.data;
  }

  async retryFailedBatches(jobId: string) {
    const response = await this.client.post(`/imports/bulk/${jobId}/retry-failed-batches`);
    return response.data;
  }

  async deleteBulkImport(jobId: string, deleteTransactions: boolean = false) {
    const response = await this.client.delete(`/imports/bulk/${jobId}`, {
      params: { delete_transactions: deleteTransactions },
    });
    return response.data;
  }

  // Tax Lot Import (separate from transaction imports)
  async importTaxLots(file: File, mode: 'preview' | 'commit') {
    const formData = new FormData();
    formData.append('file', file);
    const response = await this.client.post(`/tax-lots/import?mode=${mode}`, formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    });
    return response.data;
  }

  async getTaxLotImports(limit: number = 50) {
    const response = await this.client.get('/tax-lots/imports', {
      params: { limit },
    });
    return response.data;
  }

  async deleteTaxLotImport(importId: number) {
    const response = await this.client.delete(`/tax-lots/imports/${importId}`);
    return response.data;
  }

  async getTaxLotsFromImport(params?: { account_id?: number; symbol?: string; include_closed?: boolean; limit?: number; offset?: number }) {
    const response = await this.client.get('/tax-lots/', { params });
    return response.data;
  }

  async getTaxLotSummary(accountId?: number) {
    const response = await this.client.get('/tax-lots/summary', {
      params: accountId ? { account_id: accountId } : {},
    });
    return response.data;
  }

  async deleteAllTaxLots(confirm: boolean = false) {
    const response = await this.client.delete('/tax-lots/all', {
      params: { confirm },
    });
    return response.data;
  }

  // Inception Positions
  async importInceptionPositions(file: File, mode: 'preview' | 'commit') {
    const formData = new FormData();
    formData.append('file', file);
    const response = await this.client.post(`/imports/inception?mode=${mode}`, formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    });
    return response.data;
  }

  async getInceptionData() {
    const response = await this.client.get('/imports/inception');
    return response.data;
  }

  async getAccountInception(accountId: number) {
    const response = await this.client.get(`/imports/inception/${accountId}`);
    return response.data;
  }

  async deleteAccountInception(accountId: number) {
    const response = await this.client.delete(`/imports/inception/${accountId}`);
    return response.data;
  }

  // Classification imports
  async importClassifications(file: File, mode: 'preview' | 'commit') {
    const formData = new FormData();
    formData.append('file', file);
    const response = await this.client.post(`/imports/classifications?mode=${mode}`, formData, {
      headers: { 'Content-Type': 'multipart/form-data' },
    });
    return response.data;
  }

  async getClassificationSummary() {
    const response = await this.client.get('/imports/classifications');
    return response.data;
  }
}

export const api = new APIClient();
