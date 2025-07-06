from flask_sqlalchemy import SQLAlchemy
from datetime import datetime
from werkzeug.security import generate_password_hash, check_password_hash

db = SQLAlchemy()

# Base model with common fields
class BaseModel(db.Model):
    __abstract__ = True
    id = db.Column(db.Integer, primary_key=True)
    created_at = db.Column(db.DateTime, default=datetime.utcnow)
    updated_at = db.Column(db.DateTime, default=datetime.utcnow, onupdate=datetime.utcnow)

class User(db.Model):
    id = db.Column(db.Integer, primary_key=True)
    username = db.Column(db.String(80), unique=True, nullable=False)
    password_hash = db.Column(db.String(128), nullable=False)
    is_first_login = db.Column(db.Boolean, default=True)
    
    def set_password(self, password):
        self.password_hash = generate_password_hash(password)
        
    def check_password(self, password):
        return check_password_hash(self.password_hash, password)

class VpsServer(BaseModel):
    name = db.Column(db.String(80), nullable=False)
    host = db.Column(db.String(120), nullable=False)
    port = db.Column(db.Integer, default=22)  # SSH port
    postgres_port = db.Column(db.Integer, default=5432)  # PostgreSQL port
    username = db.Column(db.String(80))
    ssh_key_content = db.Column(db.Text)
    initialized = db.Column(db.Boolean, default=False)
    
    # Relationships
    databases = db.relationship('PostgresDatabase', backref='server', lazy=True, cascade="all, delete-orphan")
    backup_jobs = db.relationship('BackupJob', backref='server', lazy=True, cascade="all, delete-orphan")
    
    def __repr__(self):
        return f'<VpsServer {self.name}>'

class PostgresDatabase(BaseModel):
    name = db.Column(db.String(80), nullable=False)
    vps_server_id = db.Column(db.Integer, db.ForeignKey('vps_server.id', ondelete='CASCADE'), nullable=False)
    size = db.Column(db.String(20))
    
    # Relationships
    backup_jobs = db.relationship('BackupJob', backref='database', lazy=True, cascade="all, delete-orphan")
    restore_logs = db.relationship('RestoreLog', backref='database', lazy=True, cascade="all, delete-orphan")
    users = db.relationship('PostgresDatabaseUser', backref='database', lazy=True, cascade="all, delete-orphan")

class PostgresDatabaseUser(BaseModel):
    username = db.Column(db.String(80), nullable=False)
    password = db.Column(db.String(128), nullable=False)
    database_id = db.Column(db.Integer, db.ForeignKey('postgres_database.id', ondelete='CASCADE'), nullable=False)
    is_primary = db.Column(db.Boolean, default=False)  # Flag for the primary user (auto-generated from database name)
    
    def __repr__(self):
        return f'<PostgresDatabaseUser {self.username} for database {self.database_id}>'

class S3Storage(BaseModel):
    name = db.Column(db.String(80), nullable=False)
    bucket = db.Column(db.String(120), nullable=False)
    region = db.Column(db.String(50), nullable=False)
    access_key = db.Column(db.String(120), nullable=False)
    secret_key = db.Column(db.String(120), nullable=False)
    
    # Relationships
    backup_jobs = db.relationship('BackupJob', backref='s3_storage', lazy=True)

class BackupJob(BaseModel):
    name = db.Column(db.String(80), nullable=False)
    database_id = db.Column(db.Integer, db.ForeignKey('postgres_database.id', ondelete='CASCADE'), nullable=False)
    vps_server_id = db.Column(db.Integer, db.ForeignKey('vps_server.id', ondelete='CASCADE'), nullable=False)
    backup_type = db.Column(db.String(20), default='full')  # full or incr
    cron_expression = db.Column(db.String(50), nullable=False)
    enabled = db.Column(db.Boolean, default=True)
    s3_storage_id = db.Column(db.Integer, db.ForeignKey('s3_storage.id'), nullable=False)
    retention_count = db.Column(db.Integer, default=7)  # Maximum number of backups to keep
    
    # Relationships
    logs = db.relationship('BackupLog', backref='backup_job', lazy=True, cascade="all, delete-orphan")

class BackupLog(BaseModel):
    backup_job_id = db.Column(db.Integer, db.ForeignKey('backup_job.id', ondelete='CASCADE'), nullable=False)
    start_time = db.Column(db.DateTime, default=datetime.utcnow)
    end_time = db.Column(db.DateTime)
    status = db.Column(db.String(20))  # success, failed, in_progress
    backup_type = db.Column(db.String(20))  # full or incr
    size_bytes = db.Column(db.BigInteger)
    log_output = db.Column(db.Text)
    manual = db.Column(db.Boolean, default=False)
    
    # Relationships
    restore_logs = db.relationship('RestoreLog', backref='backup_log', lazy=True)

class RestoreLog(BaseModel):
    backup_log_id = db.Column(db.Integer, db.ForeignKey('backup_log.id'), nullable=True)
    database_id = db.Column(db.Integer, db.ForeignKey('postgres_database.id', ondelete='CASCADE'), nullable=False)
    start_time = db.Column(db.DateTime, default=datetime.utcnow)
    end_time = db.Column(db.DateTime)
    status = db.Column(db.String(20))  # success, failed, in_progress
    log_output = db.Column(db.Text)
    restore_point = db.Column(db.DateTime, nullable=True)  # For point-in-time recovery

class FlaskSession(db.Model):
    id = db.Column(db.String(255), primary_key=True)
    session_data = db.Column(db.LargeBinary)
    expiry = db.Column(db.DateTime)
    user_id = db.Column(db.Integer)
    
    @property
    def is_expired(self):
        return self.expiry is not None and datetime.utcnow() > self.expiry
    
    def __repr__(self):
        return f'<Session: {self.id}>'

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