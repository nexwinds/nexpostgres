#!/usr/bin/env python3
"""
Script to reset the admin user password to match the ADMIN_DEFAULT_PASSWORD environment variable.
"""

import os
import sys
from dotenv import load_dotenv
from app.models.database import db, User
from app.app import create_app

# Load environment variables
load_dotenv()

# Add the app directory to the Python path
sys.path.insert(0, os.path.dirname(__file__))

def reset_admin_password():
    """Reset the admin user password."""
    app = create_app()
    
    with app.app_context():
        # Get the admin user
        admin_user = User.query.filter_by(username='admin').first()
        
        if not admin_user:
            print("Admin user not found!")
            return False
        
        # Get the password from environment variable
        new_password = os.environ.get('ADMIN_DEFAULT_PASSWORD', 'nexpostgres')
        
        # Update the password
        admin_user.set_password(new_password)
        admin_user.is_first_login = True  # Reset first login flag
        
        try:
            db.session.commit()
            print(f"Admin password has been reset to: {new_password}")
            print("Username: admin")
            print("You can now login with these credentials.")
            return True
        except Exception as e:
            print(f"Error updating password: {e}")
            db.session.rollback()
            return False

if __name__ == '__main__':
    if reset_admin_password():
        print("Password reset successful!")
    else:
        print("Password reset failed!")
        sys.exit(1)