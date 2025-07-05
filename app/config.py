import os
from datetime import timedelta

class Config:
    # Flask configuration
    SECRET_KEY = os.environ.get('SECRET_KEY', 'dev-secret-key-change-in-production')
    
    # Database configuration
    SQLALCHEMY_DATABASE_URI = os.environ.get('DATABASE_URI', 'sqlite:///nexpostgres.sqlite')
    SQLALCHEMY_TRACK_MODIFICATIONS = False
    
    # Session configuration
    SESSION_TYPE = 'filesystem'
    PERMANENT_SESSION_LIFETIME = timedelta(hours=24)
    
    # APScheduler configuration
    SCHEDULER_API_ENABLED = True
    SCHEDULER_TIMEZONE = "UTC"
    
    # Application specific configurations
    DEFAULT_SSH_PORT = 22
    DEFAULT_POSTGRES_PORT = 5432
    
    # Default file paths for SSH keys
    SSH_KEYS_DIRECTORY = os.environ.get('SSH_KEYS_DIR', os.path.join(os.path.dirname(os.path.abspath(__file__)), 'ssh_keys'))
    
    # Logging configuration
    LOG_LEVEL = os.environ.get('LOG_LEVEL', 'INFO')
    LOG_FORMAT = '%(asctime)s - %(name)s - %(levelname)s - %(message)s'
    
    @staticmethod
    def init_app(app):
        os.makedirs(Config.SSH_KEYS_DIRECTORY, exist_ok=True) 