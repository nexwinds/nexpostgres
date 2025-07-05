import os
import logging
from flask import Flask, redirect, url_for
from app.config import Config
from app.models.database import init_db
from app.utils.scheduler import init_scheduler
from app.utils.session_manager import init_session
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
    
    # Create app_backups directory
    os.makedirs(os.path.join(app.root_path, 'app_backups'), exist_ok=True)
    
    # Set up logging
    logging.basicConfig(level=getattr(logging, app.config['LOG_LEVEL']), format=app.config['LOG_FORMAT'])
    
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