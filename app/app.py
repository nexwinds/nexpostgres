import os
import logging
from flask import Flask, redirect, url_for
from flask_login import LoginManager
from app.config import Config
from app.models.database import init_db, User
from app.utils.scheduler import init_scheduler
from app.utils.session_manager import init_session
from app.utils.error_middleware import error_handler
from app.utils.rate_limiter import init_rate_limiter, rate_limit_exceeded_handler
from app.utils.session_security import init_session_security
from app.routes.auth import auth_bp
from app.routes.servers import servers_bp
from app.routes.databases import databases_bp
from app.routes.backups import backups_bp
from app.routes.dashboard import dashboard_bp
from app.routes.app_backup import app_backup_bp
from app.routes.s3_storage import s3_storage_bp



def create_app(config_class=Config):
    app = Flask(__name__)
    app.config.from_object(config_class)
    
    # Initialize extensions
    Config.init_app(app)
    init_db(app)
    init_session(app)
    
    # Initialize security components
    error_handler.init_app(app)
    init_rate_limiter(app)
    init_session_security(app)
    
    # Register rate limit exceeded handler
    app.register_error_handler(429, rate_limit_exceeded_handler)
    
    # Initialize Flask-Login
    login_manager = LoginManager()
    login_manager.init_app(app)
    login_manager.login_view = 'auth.login'
    login_manager.login_message = 'Please login to access this page'
    login_manager.login_message_category = 'warning'
    
    @login_manager.user_loader
    def load_user(user_id):
        return User.query.get(int(user_id))
    
    # Create app_backups directory
    os.makedirs(os.path.join(app.root_path, 'app_backups'), exist_ok=True)
    
    # Set up enhanced logging
    logging.basicConfig(
        level=getattr(logging, app.config['LOG_LEVEL']), 
        format=app.config['LOG_FORMAT'],
        handlers=[
            logging.StreamHandler(),
            logging.FileHandler('app.log') if not app.debug else logging.NullHandler()
        ]
    )
    
    # Add security logging
    security_logger = logging.getLogger('security')
    security_handler = logging.FileHandler('security.log')
    security_handler.setFormatter(logging.Formatter(
        '%(asctime)s - SECURITY - %(levelname)s - %(message)s'
    ))
    security_logger.addHandler(security_handler)
    security_logger.setLevel(logging.WARNING)
    
    # Register blueprints
    blueprints = [auth_bp, servers_bp, databases_bp, backups_bp, dashboard_bp, app_backup_bp, s3_storage_bp]
    for bp in blueprints:
        app.register_blueprint(bp)
    
    # Redirect root URL to dashboard
    @app.route('/')
    def index():
        return redirect(url_for('dashboard.index'))
    
    # Initialize scheduler after app is fully set up
    with app.app_context():
        init_scheduler(app)
    
    return app

if __name__ == '__main__':
    app = create_app()
    app.run(host='0.0.0.0', debug=True)