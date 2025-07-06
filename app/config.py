import os
import logging
from datetime import timedelta

class Config:
    """Application configuration settings"""
    # Flask configuration
    SECRET_KEY = os.environ.get('SECRET_KEY', 'dev-secret-key-change-in-production')
    DEBUG = os.environ.get('FLASK_DEBUG', '0') == '1'
    
    # Database configuration
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URI', 'sqlite:///nexpostgres.sqlite')
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    
    # Session configuration
    SESSION_TYPE = 'sqlalchemy'
    SESSION_COOKIE_NAME = 'nexpostgres_session'
    SESSION_COOKIE_SECURE = os.environ.get('SESSION_COOKIE_SECURE', '0') == '1'
    SESSION_COOKIE_HTTPONLY = True
    SESSION_COOKIE_SAMESITE = 'Lax'
    PERMANENT_SESSION_LIFETIME = timedelta(hours=int(os.environ.get('SESSION_LIFETIME_HOURS', '24')))
    
    # APScheduler configuration
    SCHEDULER_API_ENABLED = os.environ.get('SCHEDULER_API_ENABLED', '0') == '1'
    SCHEDULER_TIMEZONE = os.environ.get('SCHEDULER_TIMEZONE', 'UTC')
    
    # Application specific configurations
    DEFAULT_SSH_PORT = int(os.environ.get('DEFAULT_SSH_PORT', '22'))
    DEFAULT_POSTGRES_PORT = int(os.environ.get('DEFAULT_POSTGRES_PORT', '5432'))
    DEFAULT_BACKUP_RETENTION_COUNT = int(os.environ.get('DEFAULT_BACKUP_RETENTION_COUNT', '7'))
    
    # File paths
    BASE_DIR = os.path.dirname(os.path.abspath(__file__))
    SSH_KEYS_DIRECTORY = os.environ.get('SSH_KEYS_DIR', os.path.join(BASE_DIR, 'ssh_keys'))
    
    # Logging configuration
    LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
    LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    @staticmethod
    def init_app(app):
        """Initialize the application with this configuration"""
        # Create necessary directories
        os.makedirs(Config.SSH_KEYS_DIRECTORY, exist_ok=True)
        
        # Configure logging
        log_level = getattr(logging, Config.LOG_LEVEL.upper(), logging.INFO)
        logging.basicConfig(
            level=log_level,
            format=Config.LOG_FORMAT
        )
        
        # Additional app configuration can be added here
        if not app.debug and not app.testing:
            # Production settings
            pass 