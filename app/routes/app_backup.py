import os
import sqlite3
import json
import shutil
import tempfile
import logging
from datetime import datetime
from flask import Blueprint, render_template, request, redirect, url_for, flash, send_file, current_app
from flask_login import login_required
from werkzeug.utils import secure_filename
from app.routes.auth import first_login_required
from app.models.database import db, User, VpsServer, PostgresDatabase, BackupJob, BackupLog, RestoreLog

# Set up logger
logger = logging.getLogger(__name__)

app_backup_bp = Blueprint('app_backup', __name__, url_prefix='/app-backup')

@app_backup_bp.route('/')
@login_required
@first_login_required
def index():
    # Get a list of previous backups if they exist
    backup_dir = os.path.join(current_app.root_path, 'app_backups')
    os.makedirs(backup_dir, exist_ok=True)
    
    backups = []
    for filename in os.listdir(backup_dir):
        if filename.endswith('.sqlite'):
            file_path = os.path.join(backup_dir, filename)
            file_stats = os.stat(file_path)
            
            # Parse timestamp from filename (format: backup_YYYYMMDD_HHMMSS.sqlite)
            try:
                timestamp_str = filename.replace('backup_', '').replace('.sqlite', '')
                timestamp = datetime.strptime(timestamp_str, '%Y%m%d_%H%M%S')
            except ValueError:
                timestamp = datetime.fromtimestamp(file_stats.st_mtime)
                
            backups.append({
                'filename': filename,
                'size': file_stats.st_size,
                'created_at': timestamp,
                'size_formatted': format_size(file_stats.st_size)
            })
    
    # Sort backups by creation time (newest first)
    backups.sort(key=lambda x: x['created_at'], reverse=True)
    
    return render_template('app_backup/index.html', backups=backups)

@app_backup_bp.route('/export', methods=['POST'])
@login_required
@first_login_required
def export_db():
    try:
        # Get database path from app config
        db_uri = current_app.config['SQLALCHEMY_DATABASE_URI']
        logger.info(f"Database URI from config: {db_uri}")
        
        # Try to find the actual database file
        if db_uri.startswith('sqlite:////'):  # Absolute path
            db_path = db_uri.replace('sqlite:////', '')
            logger.info(f"Absolute path detected: {db_path}")
        elif db_uri.startswith('sqlite:///'):  # Relative path
            # Try different approaches to find the database
            app_root = current_app.root_path
            parent_dir = os.path.dirname(app_root)
            
            # Option 1: Database in the app directory
            path1 = os.path.join(app_root, db_uri.replace('sqlite:///', ''))
            # Option 2: Database in the parent directory
            path2 = os.path.join(parent_dir, db_uri.replace('sqlite:///', ''))
            # Option 3: Database in the instance folder
            path3 = os.path.join(parent_dir, 'instance', db_uri.replace('sqlite:///', ''))
            
            logger.info(f"Checking paths: {path1}, {path2}, {path3}")
            
            if os.path.exists(path1):
                db_path = path1
                logger.info(f"Found database at path1: {db_path}")
            elif os.path.exists(path2):
                db_path = path2
                logger.info(f"Found database at path2: {db_path}")
            elif os.path.exists(path3):
                db_path = path3
                logger.info(f"Found database at path3: {db_path}")
            else:
                # Default to the app root path
                db_path = path1
                logger.warning(f"Database not found, defaulting to: {db_path}")
        else:
            # If using a different format, default to the instance folder
            db_path = os.path.join(os.path.dirname(current_app.root_path), 'nexpostgres.sqlite')
            logger.info(f"Using default path: {db_path}")
        
        # Check if the file exists
        if not os.path.exists(db_path):
            logger.error(f"Database file not found at: {db_path}")
            flash(f'Database file not found at: {db_path}', 'danger')
            return redirect(url_for('app_backup.index'))
            
        # Create backup directory if it doesn't exist
        backup_dir = os.path.join(current_app.root_path, 'app_backups')
        os.makedirs(backup_dir, exist_ok=True)
        
        # Generate backup filename with timestamp
        timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
        backup_filename = f"backup_{timestamp}.sqlite"
        backup_path = os.path.join(backup_dir, backup_filename)
        
        # Create a copy of the database
        shutil.copy2(db_path, backup_path)
        
        flash('Database backup created successfully', 'success')
        return redirect(url_for('app_backup.index'))
        
    except Exception as e:
        flash(f'Error creating database backup: {str(e)}', 'danger')
        return redirect(url_for('app_backup.index'))

@app_backup_bp.route('/download/<filename>')
@login_required
@first_login_required
def download_backup(filename):
    # Sanitize filename to prevent directory traversal
    filename = secure_filename(filename)
    backup_dir = os.path.join(current_app.root_path, 'app_backups')
    file_path = os.path.join(backup_dir, filename)
    
    if os.path.exists(file_path) and os.path.isfile(file_path):
        return send_file(
            file_path,
            as_attachment=True,
            download_name=filename,
            mimetype='application/x-sqlite3'
        )
    else:
        flash('Backup file not found', 'danger')
        return redirect(url_for('app_backup.index'))

@app_backup_bp.route('/delete/<filename>', methods=['POST'])
@login_required
@first_login_required
def delete_backup(filename):
    # Sanitize filename to prevent directory traversal
    filename = secure_filename(filename)
    backup_dir = os.path.join(current_app.root_path, 'app_backups')
    file_path = os.path.join(backup_dir, filename)
    
    if os.path.exists(file_path) and os.path.isfile(file_path):
        try:
            os.remove(file_path)
            flash('Backup deleted successfully', 'success')
        except Exception as e:
            flash(f'Error deleting backup: {str(e)}', 'danger')
    else:
        flash('Backup file not found', 'danger')
        
    return redirect(url_for('app_backup.index'))

@app_backup_bp.route('/import', methods=['GET', 'POST'])
@login_required
@first_login_required
def import_db():
    if request.method == 'POST':
        # Check if a file was uploaded
        if 'backup_file' not in request.files:
            flash('No file selected', 'danger')
            return redirect(url_for('app_backup.index'))
            
        file = request.files['backup_file']
        
        if file.filename == '':
            flash('No file selected', 'danger')
            return redirect(url_for('app_backup.index'))
            
        if not file.filename.endswith('.sqlite'):
            flash('Invalid file format. Only SQLite database files (.sqlite) are allowed', 'danger')
            return redirect(url_for('app_backup.index'))
        
        try:
            # Save the uploaded file to a temporary location
            temp_dir = tempfile.mkdtemp()
            temp_path = os.path.join(temp_dir, 'temp_import.sqlite')
            file.save(temp_path)
            
            # Validate that this is a valid SQLite database with the expected schema
            if not validate_sqlite_backup(temp_path):
                flash('Invalid backup file or incompatible database schema', 'danger')
                return redirect(url_for('app_backup.index'))
            
            # Get database path from app config
            db_uri = current_app.config['SQLALCHEMY_DATABASE_URI']
            logger.info(f"Import - Database URI from config: {db_uri}")
            
            # Try to find the actual database file
            if db_uri.startswith('sqlite:////'):  # Absolute path
                db_path = db_uri.replace('sqlite:////', '')
                logger.info(f"Import - Absolute path detected: {db_path}")
            elif db_uri.startswith('sqlite:///'):  # Relative path
                # Try different approaches to find the database
                app_root = current_app.root_path
                parent_dir = os.path.dirname(app_root)
                
                # Option 1: Database in the app directory
                path1 = os.path.join(app_root, db_uri.replace('sqlite:///', ''))
                # Option 2: Database in the parent directory
                path2 = os.path.join(parent_dir, db_uri.replace('sqlite:///', ''))
                # Option 3: Database in the instance folder
                path3 = os.path.join(parent_dir, 'instance', db_uri.replace('sqlite:///', ''))
                
                logger.info(f"Import - Checking paths: {path1}, {path2}, {path3}")
                
                if os.path.exists(path1):
                    db_path = path1
                    logger.info(f"Import - Found database at path1: {db_path}")
                elif os.path.exists(path2):
                    db_path = path2
                    logger.info(f"Import - Found database at path2: {db_path}")
                elif os.path.exists(path3):
                    db_path = path3
                    logger.info(f"Import - Found database at path3: {db_path}")
                else:
                    # Default to the app root path
                    db_path = path1
                    logger.warning(f"Import - Database not found, defaulting to: {db_path}")
            else:
                # If using a different format, default to the instance folder
                db_path = os.path.join(os.path.dirname(current_app.root_path), 'nexpostgres.sqlite')
                logger.info(f"Import - Using default path: {db_path}")
            
            # For import, we don't need to check if the file exists
            # If it doesn't exist, we'll create it by copying the imported file
            
            # Create a backup of the current database before importing
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            auto_backup_dir = os.path.join(current_app.root_path, 'app_backups')
            os.makedirs(auto_backup_dir, exist_ok=True)
            auto_backup_path = os.path.join(auto_backup_dir, f"pre_import_backup_{timestamp}.sqlite")
            
            # Create a backup before importing
            shutil.copy2(db_path, auto_backup_path)
            
            # Close all database connections
            db.session.close_all()
            
            # Replace the current database with the imported one
            shutil.copy2(temp_path, db_path)
            
            # Clean up temporary files
            shutil.rmtree(temp_dir, ignore_errors=True)
            
            flash('Database imported successfully. A backup of your previous database was created.', 'success')
            return redirect(url_for('dashboard.index'))
            
        except Exception as e:
            flash(f'Error importing database: {str(e)}', 'danger')
            return redirect(url_for('app_backup.index'))
    
    return render_template('app_backup/import.html')

def validate_sqlite_backup(file_path):
    """Validate that the SQLite file has the expected schema."""
    try:
        conn = sqlite3.connect(file_path)
        cursor = conn.cursor()
        
        # Check for required tables
        required_tables = ['user', 'vps_server', 'postgres_database', 'backup_job', 'backup_log', 'restore_log']
        
        # Get list of tables in the database
        cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
        tables = [row[0] for row in cursor.fetchall()]
        
        # Check if all required tables exist
        for table in required_tables:
            if table not in tables:
                return False
                
        conn.close()
        return True
        
    except Exception:
        return False

def format_size(size_bytes):
    """Format file size in bytes to human-readable format."""
    if size_bytes < 1024:
        return f"{size_bytes} bytes"
    elif size_bytes < 1024 * 1024:
        return f"{size_bytes / 1024:.2f} KB"
    elif size_bytes < 1024 * 1024 * 1024:
        return f"{size_bytes / (1024 * 1024):.2f} MB"
    else:
        return f"{size_bytes / (1024 * 1024 * 1024):.2f} GB"