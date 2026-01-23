from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware
from app.core.config import settings
from app.core.database import init_db, get_db
from app.core.security import get_password_hash
from app.models import User
from app.api import auth, imports, views, analytics, baskets, jobs, transactions, portfolio_stats, data_management

# Create FastAPI app
app = FastAPI(
    title="Portfolio Monitor API",
    description="Internal portfolio monitoring and analytics system",
    version="1.0.0"
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=settings.cors_origins_list,
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

# Include routers
app.include_router(auth.router)
app.include_router(imports.router)
app.include_router(views.router)
app.include_router(analytics.router)
app.include_router(baskets.router)
app.include_router(jobs.router)
app.include_router(transactions.router)
app.include_router(portfolio_stats.router)
app.include_router(data_management.router)


@app.on_event("startup")
async def startup_event():
    """Initialize database and create default admin user"""
    # Create tables
    init_db()

    # Create default admin user if not exists
    db = next(get_db())
    try:
        admin = db.query(User).filter(User.email == "admin@example.com").first()
        if not admin:
            admin = User(
                email="admin@example.com",
                hashed_password=get_password_hash("admin"),
                full_name="Admin User",
                is_active=True,
                is_admin=True
            )
            db.add(admin)
            db.commit()
            print("Created default admin user: admin@example.com / admin")
    finally:
        db.close()


@app.get("/")
def root():
    """Root endpoint"""
    return {
        "name": "Portfolio Monitor API",
        "version": "1.0.0",
        "status": "running"
    }


@app.get("/health")
def health_check():
    """Health check endpoint"""
    return {"status": "healthy"}
