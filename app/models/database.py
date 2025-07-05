from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
from werkzeug.security import generate_password_hash, check_password_hash

db = SQLAlchemy()

class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    password_hash = db.Column(db.String(128), nullable=False)
    is_first_login = db.Column(db.Boolean, default=True)
    
    def set_password(self, password):
        self.password_hash = generate_password_hash(password)
        
    def check_password(self, password):
        return check_password_hash(self.password_hash, password)

class FlaskSession(db.Model):
    """Model for storing Flask session data in the database"""
    id = db.Column(db.String(255), primary_key=True)
    user_id = db.Column(db.Integer, nullable=True)  # To link sessions to users for deletion
    data = db.Column(db.LargeBinary, nullable=False)
    expiry = db.Column(db.DateTime, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    
class VpsServer(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    host = db.Column(db.String(255), nullable=False)
    port = db.Column(db.Integer, default=22)
    username = db.Column(db.String(100), nullable=False)
    ssh_key_path = db.Column(db.Text, nullable=True)
    ssh_key_content = db.Column(db.Text, nullable=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationship
    databases = db.relationship('PostgresDatabase', backref='server', lazy=True, cascade="all, delete-orphan")
    backup_jobs = db.relationship('BackupJob', backref='server', lazy=True, cascade="all, delete-orphan")

class PostgresDatabase(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    port = db.Column(db.Integer, default=5432)
    username = db.Column(db.String(100), nullable=False)
    password = db.Column(db.String(100), nullable=False)
    vps_server_id = db.Column(db.Integer, db.ForeignKey('vps_server.id'), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationship
    backup_jobs = db.relationship('BackupJob', backref='database', lazy=True, cascade="all, delete-orphan")

class S3Storage(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    bucket = db.Column(db.String(255), nullable=False)
    region = db.Column(db.String(50), nullable=False)
    access_key = db.Column(db.String(100), nullable=False)
    secret_key = db.Column(db.String(100), nullable=False)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationship
    backup_jobs = db.relationship('BackupJob', backref='s3_storage', lazy=True)
    
    def __repr__(self):
        return f"<S3Storage {self.name}>"

class BackupJob(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    name = db.Column(db.String(100), nullable=False)
    vps_server_id = db.Column(db.Integer, db.ForeignKey('vps_server.id'), nullable=False)
    database_id = db.Column(db.Integer, db.ForeignKey('postgres_database.id'), nullable=False)
    backup_type = db.Column(db.String(20), nullable=False)  # full, incremental
    cron_expression = db.Column(db.String(100), nullable=False)
    enabled = db.Column(db.Boolean, default=True)
    
    # S3 configuration - reference to S3Storage
    s3_storage_id = db.Column(db.Integer, db.ForeignKey('s3_storage.id'), nullable=False)
    
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)
    
    # Relationship
    backup_logs = db.relationship('BackupLog', backref='job', lazy=True, cascade="all, delete-orphan")

class BackupLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    backup_job_id = db.Column(db.Integer, db.ForeignKey('backup_job.id'), nullable=False)
    status = db.Column(db.String(20), nullable=False)  # success, failed, in_progress
    backup_type = db.Column(db.String(20), nullable=False)  # full, incremental
    start_time = db.Column(db.DateTime, default=datetime.utcnow)
    end_time = db.Column(db.DateTime, nullable=True)
    log_output = db.Column(db.Text, nullable=True)
    size_bytes = db.Column(db.BigInteger, nullable=True)
    is_manual = db.Column(db.Boolean, default=False)

class RestoreLog(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    backup_log_id = db.Column(db.Integer, db.ForeignKey('backup_log.id'), nullable=True)
    database_id = db.Column(db.Integer, db.ForeignKey('postgres_database.id'), nullable=False)
    status = db.Column(db.String(20), nullable=False)  # success, failed, in_progress
    start_time = db.Column(db.DateTime, default=datetime.utcnow)
    end_time = db.Column(db.DateTime, nullable=True)
    log_output = db.Column(db.Text, nullable=True)
    restore_point = db.Column(db.DateTime, nullable=True)  # For PITR
    
    database = db.relationship('PostgresDatabase')
    backup = db.relationship('BackupLog')

def init_db(app):
    db.init_app(app)
    
    with app.app_context():
        db.create_all()
        
        # Create default admin user if it doesn't exist
        if not User.query.filter_by(username='admin').first():
            admin = User(username='admin')
            admin.set_password('admin')  # Default password that must be changed on first login
            admin.is_first_login = True
            db.session.add(admin)
            db.session.commit() 