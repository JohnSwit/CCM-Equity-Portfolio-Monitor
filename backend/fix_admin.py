#!/usr/bin/env python3
"""
Quick script to ensure the admin user has admin privileges
"""
from app.core.database import SessionLocal
from app.models import User

def fix_admin_user():
    db = SessionLocal()
    try:
        # Find admin user
        admin = db.query(User).filter(User.email == "admin@example.com").first()

        if not admin:
            print("❌ Admin user not found!")
            return

        print(f"✓ Found admin user: {admin.email}")
        print(f"  - is_active: {admin.is_active}")
        print(f"  - is_admin: {admin.is_admin}")

        if not admin.is_admin:
            print("\n🔧 Fixing admin user - setting is_admin=True")
            admin.is_admin = True
            db.commit()
            print("✓ Admin user fixed!")
        else:
            print("\n✓ Admin user already has admin privileges")

    finally:
        db.close()

if __name__ == "__main__":
    fix_admin_user()
