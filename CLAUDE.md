# CLAUDE.md - AI Assistant Guide for Portfolio Monitor

This document provides context for AI assistants working with the CCM Equity Portfolio Monitor codebase.

## Project Overview

**Purpose**: Internal portfolio monitoring and analytics web application for investment teams to track and analyze equity portfolios across 100+ accounts with daily market data updates, performance benchmarking, factor analysis, and risk metrics.

**Tech Stack**:
- **Backend**: FastAPI (Python 3.11), SQLAlchemy ORM, PostgreSQL 15
- **Frontend**: Next.js 14, React 18, TypeScript, Recharts, Tailwind CSS
- **Infrastructure**: Docker Compose

## Quick Commands

```bash
# Start all services (from repo root)
cd infra && docker-compose up --build

# Backend development (standalone)
cd backend && pip install -r requirements.txt && uvicorn app.main:app --reload

# Frontend development (standalone)
cd frontend && npm install && npm run dev

# Run backend tests
cd backend && pytest

# View logs
docker logs portfolio_backend
docker logs portfolio_worker
docker logs portfolio_frontend
```

**Service URLs**:
- Frontend: http://localhost:3000
- Backend API: http://localhost:8000
- API Docs: http://localhost:8000/docs
- PostgreSQL: localhost:5432

**Default Credentials**: `admin@example.com` / `admin`

## Directory Structure

```
/backend/app/
├── main.py                 # FastAPI app entry point, startup event
├── api/                    # REST API endpoints (routers)
│   ├── auth.py             # Authentication (JWT login)
│   ├── imports.py          # CSV import with preview/commit
│   ├── views.py            # Accounts, groups, firm views
│   ├── analytics.py        # Analytics endpoints (returns, risk, factors)
│   ├── baskets.py          # Custom weighted baskets
│   ├── jobs.py             # Background job triggers (admin only)
│   ├── transactions.py     # Transaction listing
│   ├── portfolio_stats.py  # Portfolio statistics
│   └── data_management.py  # Data management utilities
├── core/
│   ├── config.py           # Settings from environment variables
│   ├── database.py         # SQLAlchemy session management
│   └── security.py         # JWT and password hashing
├── models/
│   ├── models.py           # SQLAlchemy ORM models (25+ tables)
│   ├── schemas.py          # Pydantic request/response schemas
│   └── sector_models.py    # Sector classification models
├── services/               # Business logic layer
│   ├── bd_parser.py        # Black Diamond CSV parser
│   ├── market_data.py      # Price service (orchestrates providers)
│   ├── market_data_providers.py  # Data providers (Tiingo primary, Stooq/yfinance fallback)
│   ├── positions.py        # Daily positions engine
│   ├── returns.py          # TWR returns computation
│   ├── benchmarks.py       # Benchmark metrics (beta, alpha, tracking error)
│   ├── factors.py          # STYLE7 factor analysis
│   ├── risk.py             # Risk metrics (volatility, drawdown, VaR)
│   ├── groups.py           # Group management
│   ├── baskets.py          # Custom baskets
│   └── data_sourcing.py    # External data sourcing
├── workers/
│   ├── scheduler.py        # APScheduler (daily at 23:00 UTC)
│   └── jobs.py             # Job definitions
└── utils/
    └── ticker_utils.py     # Ticker normalization

/frontend/src/
├── pages/                  # Next.js pages (routes)
│   ├── index.tsx           # Dashboard (main page)
│   ├── login.tsx           # Login page
│   ├── upload.tsx          # CSV import page
│   ├── groups.tsx          # Group management
│   ├── transactions.tsx    # Transaction listing
│   ├── statistics.tsx      # Statistics page
│   └── _app.tsx            # App wrapper
├── components/
│   └── Layout.tsx          # Main layout with navigation
├── contexts/
│   └── AuthContext.tsx     # Auth context provider (JWT)
├── lib/
│   └── api.ts              # Axios API client
└── styles/
    └── globals.css         # Tailwind global styles

/infra/
├── docker-compose.yml      # Docker services definition
└── .env.template           # Environment variables template
```

## Key Models (backend/app/models/models.py)

| Model | Purpose |
|-------|---------|
| `User` | Authentication, admin flag |
| `Account` | Investment accounts |
| `Security` | Securities (symbol, asset class, is_option) |
| `Transaction` | Trades with idempotency keys |
| `PricesEOD` | Daily closing prices |
| `PositionsEOD` | Daily holdings (shares by date) |
| `PortfolioValueEOD` | Daily portfolio values |
| `ReturnsEOD` | Daily returns with TWR index |
| `RiskEOD` | Risk metrics (vol, drawdown, VaR) |
| `BenchmarkDefinition/Return/Metric` | Benchmark data |
| `FactorSet/FactorReturn/FactorRegression` | Factor analysis |
| `Group/GroupMember` | Account groupings |

**ViewType enum**: `ACCOUNT`, `GROUP`, `FIRM` - used throughout analytics queries.

## Critical Business Logic

### Returns Computation (Equity Sleeve)
Location: `backend/app/services/returns.py`

Returns are calculated to exclude cash flow effects:
```
r_t = V_t^{no-trade} / V_{t-1} - 1 - fee_drag
where:
  V_{t-1} = sum(shares_{t-1} * price_{t-1})
  V_t^{no-trade} = sum(shares_{t-1} * price_t)  # Yesterday's shares, today's prices
```

This produces time-weighted returns for the invested equity sleeve only.

### Transaction Idempotency
Location: `backend/app/services/bd_parser.py`

Transaction uniqueness is enforced via SHA hash of: `(account_id, symbol, trade_date, transaction_type, units, price)`. Re-uploading the same CSV won't create duplicates.

### FIRM vs GROUP Views
`FIRM` views are stored in the database as `GROUP` type with `is_firm=True`. When querying, use `get_db_view_type()` helper (see `analytics.py:34-39`).

### Factor Analysis (STYLE7)
Location: `backend/app/services/factors.py`

Seven factors computed using ETF proxies:
- MKT (SPY), SIZE (IWM-SPY), VALUE (IVE-SPY), GROWTH (IVW-SPY)
- QUALITY (QUAL-SPY), VOL (SPLV-SPY), MOM (MTUM-SPY)

Spreads vs SPY reduce market collinearity. 252-day OLS regression yields betas, alpha, R².

### Options Handling
Options are detected and stored but **excluded from analytics** due to unreliable pricing. They appear in the "Unpriced Instruments" panel. This is intentional to avoid incorrect analytics.

## Code Conventions

### Backend (Python)

1. **Router Pattern**: Each API module is a FastAPI router with prefix and tags
   ```python
   router = APIRouter(prefix="/analytics", tags=["analytics"])
   ```

2. **Dependency Injection**: Use `Depends()` for database sessions and auth
   ```python
   def get_endpoint(
       db: Session = Depends(get_db),
       current_user: User = Depends(get_current_user)
   ):
   ```

3. **Service Layer**: Business logic lives in `/services/`, not in API endpoints

4. **Pydantic Schemas**: All request/response models defined in `models/schemas.py`

5. **Database Queries**: Use SQLAlchemy ORM with `and_()` for filters
   ```python
   db.query(Model).filter(and_(Model.field == value, ...))
   ```

### Frontend (TypeScript)

1. **Pages Pattern**: Each route is a file in `/pages/` with default export

2. **API Client**: All API calls go through `/lib/api.ts`
   ```typescript
   const data = await api.getSummary(viewType, viewId);
   ```

3. **State Management**: React hooks (`useState`, `useEffect`) with AuthContext

4. **Styling**: Tailwind CSS utility classes, custom `.card` and `.table` classes

5. **Charts**: Recharts with ResponsiveContainer pattern

## Common Tasks

### Adding a New API Endpoint

1. Create or edit router in `backend/app/api/`
2. Add Pydantic schemas to `backend/app/models/schemas.py` if needed
3. Add business logic to appropriate service in `backend/app/services/`
4. Include router in `backend/app/main.py` if new file

### Adding a New Frontend Page

1. Create page in `frontend/src/pages/`
2. Wrap with `<Layout>` component
3. Add navigation link in `frontend/src/components/Layout.tsx`
4. Add API methods to `frontend/src/lib/api.ts` if needed

### Modifying Database Schema

1. Update models in `backend/app/models/models.py`
2. Tables auto-create on startup if missing (development mode)
3. For production, use Alembic migrations:
   ```bash
   cd backend && alembic revision --autogenerate -m "Description"
   ```

### Triggering Background Jobs

```bash
# Via API (requires auth token)
curl -X POST "http://localhost:8000/jobs/run?job_name=market_data_update" \
  -H "Authorization: Bearer TOKEN"

# Available jobs: market_data_update, recompute_analytics
```

## Testing

**Test Location**: `backend/tests/`

**Run Tests**:
```bash
cd backend && pytest
```

**Test Database**: Tests use in-memory SQLite via `test_db` fixture.

**Key Test Areas**:
- Transaction idempotency (no duplicates on re-import)
- CSV header validation
- Transaction type mapping

## Environment Variables

Key variables (see `infra/.env.template`):

| Variable | Purpose |
|----------|---------|
| `DATABASE_URL` | PostgreSQL connection string |
| `SECRET_KEY` | JWT signing key |
| `TIINGO_API_KEY` | Tiingo API key (primary market data source) |
| `ENABLE_YFINANCE_FALLBACK` | Use yfinance as last-resort fallback |
| `CORS_ORIGINS` | Allowed CORS origins |
| `NEXT_PUBLIC_API_URL` | Frontend API endpoint |

## Gotchas and Warnings

1. **FIRM View Storage**: FIRM views use `ViewType.GROUP` in database with `is_firm=True`

2. **Market Data Providers**: Tiingo is primary (requires API key), Stooq and yfinance are fallbacks

3. **Trade Date Effects**: Trades update positions **after close** of trade date

4. **Options Exclusion**: Options intentionally excluded from analytics - don't "fix" this

5. **Index Values**: TWR index starts at 1.0, represents cumulative growth (1.05 = +5%)

6. **Background Worker**: Separate Docker service (`portfolio_worker`), same codebase

7. **Admin Endpoints**: Job triggers require `is_admin=True` on user

8. **Date Handling**: Always use `date` type, not `datetime`, for EOD data

9. **Fee Drag**: Transaction fees reduce returns via `fee_drag = fees / V_{t-1}`

10. **Benchmark Codes**: SPY, QQQ, INDU (not ^DJI) - mapping happens in market_data.py

## API Quick Reference

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/auth/login` | POST | Get JWT token |
| `/imports/blackdiamond/transactions` | POST | Upload CSV (mode=preview\|commit) |
| `/views` | GET | List all accounts/groups/firm |
| `/analytics/summary` | GET | KPIs for a view |
| `/analytics/returns` | GET | Returns time series |
| `/analytics/holdings` | GET | Current holdings |
| `/analytics/risk` | GET | Risk metrics |
| `/analytics/benchmark` | GET | Benchmark metrics |
| `/analytics/factors` | GET | Factor exposures |
| `/jobs/run` | POST | Trigger background job (admin) |
| `/jobs/incremental-update` | POST | Incremental update (recommended) |
| `/jobs/smart-update` | POST | Auto-selects best update strategy |
| `/jobs/update-status` | GET | Update health and metrics |
| `/jobs/job-history` | GET | Recent job execution history |

All analytics endpoints require `view_type` and `view_id` query parameters.

## Incremental Update System

The portfolio monitor includes an optimized incremental update system designed for scalability:

### Key Features

1. **Single Source of Truth**: Tiingo is primary provider; fallbacks only on failure
2. **Incremental Fetching**: Only fetches missing date ranges, not full history
3. **Dependency-Aware**: Only recomputes analytics when inputs change
4. **Provider Tracking**: Remembers which providers work for each ticker
5. **Observable**: Detailed metrics per run for monitoring

### Update Commands

```bash
# Recommended: Run incremental update (smart caching + dependency tracking)
curl -X POST "http://localhost:8000/jobs/incremental-update" \
  -H "Authorization: Bearer TOKEN"

# Auto-select best strategy based on time since last update
curl -X POST "http://localhost:8000/jobs/smart-update" \
  -H "Authorization: Bearer TOKEN"

# Check update status and health
curl "http://localhost:8000/jobs/update-status" \
  -H "Authorization: Bearer TOKEN"

# View recent job history
curl "http://localhost:8000/jobs/job-history" \
  -H "Authorization: Bearer TOKEN"
```

### Database Tables for Update Tracking

| Table | Purpose |
|-------|---------|
| `ticker_provider_coverage` | Tracks which providers work for each ticker |
| `data_update_state` | Tracks last update date for incremental fetching |
| `computation_dependencies` | Tracks input hashes for dependency-aware computation |
| `update_job_runs` | Stores execution metrics for observability |

### How It Works

1. **Price Updates**: Checks `data_update_state` for last fetch date, only requests missing dates
2. **Provider Selection**: Checks `ticker_provider_coverage` for working provider, falls back if needed
3. **Analytics**: Computes input hash, compares with `computation_dependencies`, skips if unchanged
4. **Metrics**: Records all statistics in `update_job_runs` for monitoring

## Debugging Tips

1. **Check logs**: `docker logs portfolio_backend -f`

2. **Database inspection**:
   ```bash
   docker exec -it portfolio_db psql -U portfolio_user portfolio_db
   ```

3. **Test data**: Use `examples/sample_blackdiamond_transactions.csv`

4. **API testing**: Use http://localhost:8000/docs (Swagger UI)

5. **Worker issues**: Check `docker logs portfolio_worker`

## Security Notes

- Change default admin password in production
- Use strong `SECRET_KEY` in production
- JWT tokens expire after 24 hours (configurable in config.py)
- Admin privileges required for job triggers
- Transaction data is sensitive - no public endpoints
