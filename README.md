# Portfolio Monitoring & Analytics System

A comprehensive internal portfolio monitoring and analytics web application for investment teams. Built with FastAPI (backend), PostgreSQL (database), and Next.js (frontend).

## Features

### Core Capabilities
- **100+ Account Support**: Manage large portfolios with fast, searchable account selection
- **Custom Groups**: Create Families, Estates, and Custom Groups to organize accounts
- **Firm-Wide Overview**: Automatic rollup of all accounts into firm-level analytics
- **Black Diamond CSV Import**: Manual upload with preview, error handling, and idempotency
- **Automated Market Data**: Daily fetching from Stooq (primary) and yfinance (fallback)
- **Equity Sleeve Analytics**: Returns computed correctly, ignoring cash inflows/outflows

### Analytics
- **Performance Tracking**: Daily returns using holdings-based approach (no cash flow contamination)
- **Benchmarking**: Compare against SPY, QQQ, and Dow Jones with beta, alpha, tracking error
- **Custom Baskets**: Create custom weighted baskets for comparison
- **Factor Analysis**: STYLE7 factor exposures (Market, Size, Value, Growth, Quality, Volatility, Momentum)
- **Risk Metrics**: Volatility (21d, 63d), Max Drawdown, Historical VaR
- **Holdings View**: Position-level detail with weights and market values

### Data Integrity
- **Idempotent Imports**: Re-uploading files won't double-count transactions
- **Options Handling**: Options without reliable pricing are excluded from analytics (not silently mispriced)
- **Transaction Type Mapping**: Configurable mapping of raw transaction types to normalized types
- **Fee Tracking**: Transaction fees properly reduce returns

## Architecture

```
/backend          Python + FastAPI backend
  /app
    /api          REST API endpoints
    /core         Config, database, security
    /models       SQLAlchemy models + Pydantic schemas
    /services     Business logic (parsers, analytics engines)
    /workers      Scheduled jobs

/frontend         Next.js + TypeScript frontend
  /src
    /pages        Pages (dashboard, groups, upload)
    /components   Reusable UI components
    /lib          API client
    /contexts     Auth context

/infra            Docker Compose configuration
/examples         Sample CSV files
```

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Git

### Setup & Run

1. **Clone and navigate to the repository**
   ```bash
   cd CCM-Equity-Portfolio-Monitor
   ```

2. **Start all services**
   ```bash
   cd infra
   docker-compose up --build
   ```

   This will start:
   - PostgreSQL (port 5432)
   - Backend API (port 8000)
   - Worker service (background)
   - Frontend (port 3000)

3. **Access the application**
   - Frontend: http://localhost:3000
   - Backend API docs: http://localhost:8000/docs
   - Default login: `admin@example.com` / `admin`

4. **First-time setup**
   - Login to the frontend
   - Navigate to "Upload" and upload the sample CSV from `/examples/sample_blackdiamond_transactions.csv`
   - Run market data update: Use the API or wait for the scheduled job
   - Analytics will be computed automatically

### Manual Job Triggers

You can trigger jobs manually via API:

```bash
# Update market data
curl -X POST "http://localhost:8000/jobs/run?job_name=market_data_update" \
  -H "Authorization: Bearer YOUR_TOKEN"

# Recompute analytics
curl -X POST "http://localhost:8000/jobs/run?job_name=recompute_analytics" \
  -H "Authorization: Bearer YOUR_TOKEN"
```

## How It Works

### 1. Transaction Import

Black Diamond CSV files are uploaded manually. The system:

1. Parses the CSV (supports tab and comma delimiters)
2. Validates required headers
3. Shows preview with detected transaction type mappings
4. On commit, creates:
   - **Accounts** (from Account Number + Display Name)
   - **Securities** (from Symbol + Asset Name + Class)
   - **Transactions** with idempotency keys (hash of key fields)

**Idempotency**: Re-uploading the same file won't create duplicates. The system hashes key transaction fields (account, symbol, date, type, units, price) to detect duplicates.

### 2. Market Data Fetching

The worker runs daily to fetch prices:

- **Securities**: All symbols referenced in transactions
- **Benchmarks**: SPY, QQQ, INDU (Dow via ^DJI)
- **Factor ETFs**: SPY, IWM, IVE, IVW, QUAL, SPLV, MTUM

**Symbol Mapping**:
- US equities: `AAPL` → `AAPL.US` (Stooq format)
- Special tickers: `BRK.B` → `BRK-B.US`
- Dow Jones: `INDU` → `^DJI`

**Fallback**: If Stooq fails, yfinance is used (optional via env var).

### 3. Positions Engine

Daily positions are built from transactions:

```
For each account + security:
  1. Get all transaction deltas (BUY/SELL/TRANSFER_IN/TRANSFER_OUT)
  2. Compute cumulative shares by date
  3. Forward-fill across trading calendar
  4. Store in positions_eod table
```

Trades update positions **after close** of trade date (avoids counting trades as performance).

### 4. Returns Computation (Equity Sleeve Logic)

**Critical**: Returns are computed to avoid treating buys/sells/transfers as performance.

Method:
```
For each trading day t:
  1. V_{t-1} = Σ (shares_{i,t-1} × price_{i,t-1})  # Yesterday's portfolio value
  2. V_t^{no-trade} = Σ (shares_{i,t-1} × price_{i,t})  # Use YESTERDAY's shares with TODAY's prices
  3. r_gross = V_t^{no-trade} / V_{t-1} - 1
  4. fee_drag = fees_t / V_{t-1}
  5. r_net = r_gross - fee_drag
  6. index_t = index_{t-1} × (1 + r_net)
```

This produces a "time-weighted-like" return series for the invested equity sleeve.

### 5. Groups & Firm Rollup

Groups are created manually (Families, Estates, Custom).

**Group Values**: Sum of member account values
**Group Returns**: Value-weighted returns
```
r_group_t = Σ (weight_a_{t-1} × r_a_t)
where weight_a_{t-1} = value_a_{t-1} / group_value_{t-1}
```

**Firm**: A special group containing all accounts, auto-maintained.

### 6. Benchmarks

Default benchmarks (SPY, QQQ, INDU) are maintained automatically.

**Metrics computed (252-day rolling)**:
- **Beta**: Covariance(portfolio, benchmark) / Variance(benchmark)
- **Alpha**: Annualized excess return after adjusting for beta
- **Tracking Error**: Annualized volatility of active returns
- **Correlation**: Linear correlation of returns

### 7. Factors (STYLE7)

Factor exposures are computed using ETF proxies:

| Factor    | ETF   | Construction              |
|-----------|-------|---------------------------|
| MKT       | SPY   | return(SPY)               |
| SIZE      | IWM   | return(IWM) - return(SPY) |
| VALUE     | IVE   | return(IVE) - return(SPY) |
| GROWTH    | IVW   | return(IVW) - return(SPY) |
| QUALITY   | QUAL  | return(QUAL) - return(SPY)|
| VOL       | SPLV  | return(SPLV) - return(SPY)|
| MOM       | MTUM  | return(MTUM) - return(SPY)|

Spreads vs. SPY reduce market collinearity.

**Regression**: OLS regression of portfolio returns on 7 factors yields betas, alpha, R².

### 8. Risk Metrics

- **Volatility (21d, 63d)**: Rolling standard deviation, annualized
- **Max Drawdown (1Y)**: Maximum peak-to-trough decline over trailing 252 days
- **VaR 95% (1D)**: Historical 5th percentile of daily returns (trailing 252 days)

### 9. Options Handling

Options are detected via:
- `Class` column contains "option"
- Symbol patterns (if applicable)

**Behavior**:
- Options are stored in transactions
- BUT excluded from automated analytics if no reliable price
- Appear in "Unpriced Instruments" panel
- Can be manually priced via `manual_prices` table (optional upload)

This prevents incorrect analytics from bad option pricing.

## Database Schema

Key tables:
- **accounts**: Account metadata
- **securities**: Symbol, name, asset class, is_option flag
- **transactions**: Trade date, type, units, price, fees, idempotency key
- **transaction_type_map**: Configurable mapping of raw → normalized types
- **prices_eod**: Daily close prices (source: stooq/yfinance)
- **positions_eod**: Daily holdings (account × security × date × shares)
- **portfolio_value_eod**: Daily portfolio values (per view)
- **returns_eod**: Daily returns + index (per view)
- **groups**: Family/Estate/Custom groups
- **group_members**: Account memberships
- **benchmark_definitions/levels/returns/metrics**: Benchmark data
- **baskets/basket_constituents**: Custom baskets
- **factor_sets/factor_returns/factor_regressions**: Factor analysis
- **risk_eod**: Risk metrics

## Configuration

### Environment Variables

See `infra/.env.template`:

```bash
DATABASE_URL=postgresql://portfolio_user:portfolio_pass@postgres:5432/portfolio_db
SECRET_KEY=your-secret-key-here
ENABLE_YFINANCE_FALLBACK=true
CORS_ORIGINS=http://localhost:3000
NEXT_PUBLIC_API_URL=http://localhost:8000
```

### Scheduled Jobs

Worker runs daily at 23:00 UTC (configurable in `backend/app/workers/scheduler.py`):

1. **market_data_update**: Fetch prices for all securities + benchmarks + factor ETFs
2. **recompute_analytics**: Rebuild positions, values, returns, benchmarks, factors, risk

## API Endpoints

### Auth
- `POST /auth/login` - Login and get JWT token
- `GET /auth/me` - Get current user

### Imports
- `POST /imports/blackdiamond/transactions?mode=preview|commit` - Upload BD CSV
- `GET /imports` - Import history

### Views
- `GET /accounts` - List accounts (with search)
- `GET /groups` - List groups
- `POST /groups` - Create group
- `POST /groups/{id}/members` - Add accounts to group
- `GET /views` - All views (accounts + groups + firm)

### Analytics
- `GET /analytics/summary` - Summary KPIs for a view
- `GET /analytics/returns` - Returns series
- `GET /analytics/holdings` - Holdings as of date
- `GET /analytics/risk` - Risk metrics
- `GET /analytics/benchmark` - Benchmark metrics (beta, alpha, etc.)
- `GET /analytics/factors` - Factor exposures
- `GET /analytics/unpriced-instruments` - Securities without prices

### Baskets
- `GET /baskets` - List custom baskets
- `POST /baskets` - Create basket
- `PUT /baskets/{id}` - Update basket

### Jobs
- `POST /jobs/run?job_name=market_data_update|recompute_analytics` - Trigger job

Full API docs: http://localhost:8000/docs

## Frontend Pages

- **Dashboard (/)**: View selector + performance chart + holdings + risk + factors
- **Groups (/groups)**: Create/manage groups, add accounts to groups
- **Upload (/upload)**: Upload BD CSV with preview, commit, and import history

## Troubleshooting

### No data showing after import

1. Check if transactions were imported:
   ```bash
   docker exec -it portfolio_backend psql $DATABASE_URL -c "SELECT COUNT(*) FROM transactions;"
   ```

2. Trigger market data update:
   ```bash
   curl -X POST "http://localhost:8000/jobs/run?job_name=market_data_update" \
     -H "Authorization: Bearer YOUR_TOKEN"
   ```

3. Trigger analytics recomputation:
   ```bash
   curl -X POST "http://localhost:8000/jobs/run?job_name=recompute_analytics" \
     -H "Authorization: Bearer YOUR_TOKEN"
   ```

### Symbol not found

Check symbol mapping in `backend/app/services/market_data.py`:
- US equities need `.US` suffix for Stooq
- Use yfinance fallback if Stooq doesn't have the symbol
- Enable fallback: `ENABLE_YFINANCE_FALLBACK=true`

### Import errors

Check import logs:
```bash
docker logs portfolio_backend
```

Common issues:
- Missing required CSV headers
- Invalid date formats (use MM/DD/YYYY or ISO)
- Missing transaction type mapping (add to `transaction_type_map` table)

### Worker not running

Check worker logs:
```bash
docker logs portfolio_worker
```

Verify scheduler is running:
```bash
docker exec -it portfolio_worker ps aux | grep scheduler
```

## Development

### Backend Development

```bash
cd backend
pip install -r requirements.txt
uvicorn app.main:app --reload
```

### Frontend Development

```bash
cd frontend
npm install
npm run dev
```

### Database Migrations

Use Alembic for migrations (optional):

```bash
cd backend
alembic revision --autogenerate -m "Add new table"
alembic upgrade head
```

## Testing

### Unit Tests

```bash
cd backend
pytest
```

Example test for transaction idempotency:

```python
def test_transaction_idempotency():
    parser = BDParser(db)
    # Import same file twice
    parser.import_transactions(df, "test.csv", "hash1")
    count1 = db.query(Transaction).count()

    parser.import_transactions(df, "test.csv", "hash1")
    count2 = db.query(Transaction).count()

    assert count1 == count2  # No duplicates
```

### Integration Testing

1. Upload sample CSV
2. Verify accounts/securities created
3. Trigger market data update
4. Trigger analytics recomputation
5. Verify dashboard shows data

## Security Notes

- Change default admin password in production
- Use strong `SECRET_KEY` in production
- Consider HTTPS for production deployment
- Database credentials should be rotated
- API rate limiting recommended for production

## Performance Considerations

- **100+ accounts**: System is designed for this scale
- **Database indexes**: Already optimized on key fields (date, view_type, view_id)
- **Frontend search**: Uses React-Select with built-in virtualization
- **Analytics computation**: Can take 1-2 minutes for 100+ accounts (runs as background job)

## Limitations & Future Enhancements

**Current Limitations**:
- Options are excluded from analytics (by design, due to pricing limitations)
- Cash balances not tracked (equity sleeve only)
- No fixed income analytics
- No intraday data

**Potential Enhancements**:
- Automatic Black Diamond API integration (requires BD API access)
- Fixed income sleeve analytics
- Tax lot tracking
- Performance attribution
- Client portal (read-only access for clients)
- Email alerts for large moves
- PDF report generation

## Support

For issues or questions:
1. Check logs: `docker logs portfolio_backend` or `docker logs portfolio_worker`
2. Review API docs: http://localhost:8000/docs
3. Check database directly: `docker exec -it portfolio_db psql -U portfolio_user portfolio_db`

## License

Internal use only. Proprietary and confidential.