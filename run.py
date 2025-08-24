#!/usr/bin/env python3
"""
Main entry point for the NexPostgres application.
"""

import sys
import os
from dotenv import load_dotenv

# Load environment variables from .env file BEFORE importing app modules
load_dotenv()

from app.app import create_app # noqa

# Add the project root to Python path
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

if __name__ == '__main__':
    app = create_app()
    app.run(host='0.0.0.0', port=5000, debug=True)