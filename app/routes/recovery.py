from flask import Blueprint, render_template, request, jsonify, flash, redirect, url_for
from flask_login import login_required
from app.utils.backup_metadata_service import BackupMetadataService
from app.models.database import VpsServer, PostgresDatabase, BackupJob
from app.utils.restore_service import RestoreService
import logging

logger = logging.getLogger(__name__)

recovery_bp = Blueprint('recovery', __name__, url_prefix='/recovery')

@recovery_bp.route('/')
@login_required
def index():
    """Database recovery main page."""
    try:
        # Get all databases that have backups available
        databases_with_backups = BackupMetadataService.get_all_databases_with_backups()
        
        # Get all servers for recovery target selection
        servers = VpsServer.query.all()
        
        return render_template('recovery/index.html', 
                             databases=databases_with_backups,
                             servers=servers)
    except Exception as e:
        logger.error(f"Error loading recovery page: {str(e)}")
        flash('Error loading recovery page', 'error')
        return redirect(url_for('dashboard.index'))

@recovery_bp.route('/api/database/<database_name>/backups')
@login_required
def get_database_backups(database_name):
    """API endpoint to get all backups for a specific database."""
    try:
        backups = BackupMetadataService.get_database_backups(database_name)
        return jsonify({
            'success': True,
            'backups': backups
        })
    except Exception as e:
        logger.error(f"Error fetching backups for database {database_name}: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@recovery_bp.route('/initiate', methods=['POST'])
@login_required
def initiate_recovery():
    """Initiate database recovery process."""
    try:
        data = request.get_json()
        
        database_name = data.get('database_name')
        backup_key = data.get('backup_key')
        target_server_id = data.get('target_server_id')
        
        if not all([database_name, backup_key, target_server_id]):
            return jsonify({
                'success': False,
                'error': 'Missing required parameters'
            }), 400
        
        # Get target server
        target_server = VpsServer.query.get(target_server_id)
        if not target_server:
            return jsonify({
                'success': False,
                'error': 'Target server not found'
            }), 404
        
        # Get backup information
        backups = BackupMetadataService.get_database_backups(database_name)
        selected_backup = next((b for b in backups if b['key'] == backup_key), None)
        
        if not selected_backup:
            return jsonify({
                'success': False,
                'error': 'Selected backup not found'
            }), 404
        
        # Get the database and backup job
        database = PostgresDatabase.query.filter_by(name=database_name).first()
        if not database:
            return jsonify({
                'success': False,
                'error': f'Database {database_name} not found'
            }), 404
            
        backup_job = BackupJob.query.filter_by(database_id=database.id).first()
        
        if not backup_job:
            return jsonify({
                'success': False,
                'error': 'No backup job found for this database'
            }), 404
        
        # Initiate the recovery process
        restore_service = RestoreService()
        recovery_result = restore_service.initiate_recovery(
            backup_job=backup_job,
            backup_key=backup_key,
            target_server=target_server,
            backup_info=selected_backup
        )
        
        if recovery_result['success']:
            flash(f'Database recovery initiated successfully. Recovery ID: {recovery_result["recovery_id"]}', 'success')
            return jsonify({
                'success': True,
                'recovery_id': recovery_result['recovery_id'],
                'message': 'Recovery process initiated successfully'
            })
        else:
            return jsonify({
                'success': False,
                'error': recovery_result.get('error', 'Unknown error occurred')
            }), 500
            
    except Exception as e:
        logger.error(f"Error initiating recovery: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@recovery_bp.route('/status/<recovery_id>')
@login_required
def recovery_status(recovery_id):
    """Get the status of a recovery process."""
    try:
        restore_service = RestoreService()
        status = restore_service.get_recovery_status(recovery_id)
        
        return jsonify({
            'success': True,
            'status': status
        })
    except Exception as e:
        logger.error(f"Error getting recovery status for {recovery_id}: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500