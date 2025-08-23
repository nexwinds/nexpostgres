#!/usr/bin/env python3
"""
Migration: Add postgres_data_dir column to VpsServer table

This migration adds the postgres_data_dir column to the vps_server table
with a default value of '/var/lib/postgresql/data'.
"""

import sys
import os

# Add the parent directory to the path so we can import the app
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

# Set required environment variables for the migration
os.environ.setdefault('SECRET_KEY', 'migration-temp-key')
os.environ.setdefault('DATABASE_URL', 'sqlite:///nexdb.db')

from app import create_app
from app.models.database import db
from sqlalchemy import text

def run_migration():
    """Run the migration to add postgres_data_dir column."""
    app = create_app()
    
    with app.app_context():
        try:
            # Check if column already exists (SQLite compatible)
            result = db.engine.execute(text(
                "PRAGMA table_info(vps_server)"
            ))
            
            columns = [row[1] for row in result.fetchall()]
            if 'postgres_data_dir' in columns:
                print("Column 'postgres_data_dir' already exists in vps_server table.")
                return
            
            # Add the column
            db.engine.execute(text(
                "ALTER TABLE vps_server ADD COLUMN postgres_data_dir VARCHAR(255) DEFAULT '/var/lib/postgresql/data'"
            ))
            
            # Update existing records to have the default value
            db.engine.execute(text(
                "UPDATE vps_server SET postgres_data_dir = '/var/lib/postgresql/data' WHERE postgres_data_dir IS NULL"
            ))
            
            print("Successfully added postgres_data_dir column to vps_server table.")
            
        except Exception as e:
            print(f"Error running migration: {str(e)}")
            raise

if __name__ == '__main__':
    run_migration()