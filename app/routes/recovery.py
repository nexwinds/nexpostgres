from flask import Blueprint, render_template, request, jsonify, flash, redirect, url_for
from flask_login import login_required
from app.utils.backup_metadata_service import BackupMetadataService
from app.models.database import VpsServer, PostgresDatabase, BackupJob, S3Storage
from app.utils.restore_service import RestoreService
import logging

logger = logging.getLogger(__name__)

recovery_bp = Blueprint('recovery', __name__, url_prefix='/recovery')

# Shared utility functions for recovery operations
def get_recovery_context():
    """Get common context data for recovery pages."""
    s3_storages = S3Storage.query.all()
    servers = VpsServer.query.all()
    return {
        's3_storages': s3_storages,
        'servers': servers
    }

def validate_recovery_request(data, recovery_type='database'):
    """Validate recovery request data."""
    required_fields = ['target_server_id', 's3_storage_id']
    
    if recovery_type == 'database':
        required_fields.extend(['database_name', 'backup_key'])
    elif recovery_type == 'cluster':
        required_fields.append('backup_key')
    
    missing_fields = [field for field in required_fields if not data.get(field)]
    if missing_fields:
        return False, f"Missing required fields: {', '.join(missing_fields)}"
    
    return True, None

def get_target_server_and_storage(target_server_id, s3_storage_id):
    """Get and validate target server and S3 storage."""
    target_server = VpsServer.query.get(target_server_id)
    if not target_server:
        return None, None, 'Target server not found'
    
    s3_storage = S3Storage.query.get(s3_storage_id)
    if not s3_storage:
        return None, None, 'S3 storage not found'
    
    return target_server, s3_storage, None

# Legacy route - redirect to database recovery
@recovery_bp.route('/')
@login_required
def index():
    """Legacy recovery route - redirect to database recovery."""
    return redirect(url_for('recovery.recovery_db'))

@recovery_bp.route('/recovery-db')
@login_required
def recovery_db():
    """Database recovery page - disaster recovery with S3 selection."""
    try:
        context = get_recovery_context()
        return render_template('recovery/recovery_db.html', **context)
    except Exception as e:
        logger.error(f"Error loading database recovery page: {str(e)}")
        flash('Error loading database recovery page', 'error')
        return redirect(url_for('dashboard.index'))

@recovery_bp.route('/recovery-cluster')
@login_required
def recovery_cluster():
    """Cluster recovery page - full cluster disaster recovery."""
    try:
        context = get_recovery_context()
        return render_template('recovery/recovery_cluster.html', **context)
    except Exception as e:
        logger.error(f"Error loading cluster recovery page: {str(e)}")
        flash('Error loading cluster recovery page', 'error')
        return redirect(url_for('dashboard.index'))

@recovery_bp.route('/api/s3/<int:s3_storage_id>/databases')
@login_required
def get_s3_databases(s3_storage_id):
    """API endpoint to get all databases with metadata from a specific S3 storage."""
    try:
        s3_storage = S3Storage.query.get(s3_storage_id)
        if not s3_storage:
            return jsonify({
                'success': False,
                'error': 'S3 storage not found'
            }), 404
            
        # Get S3 backup structure for this storage
        databases = BackupMetadataService.get_s3_databases_with_metadata(s3_storage)
        return jsonify({
            'success': True,
            'databases': databases
        })
    except Exception as e:
        logger.error(f"Error fetching databases from S3 storage {s3_storage_id}: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

@recovery_bp.route('/api/s3/<int:s3_storage_id>/cluster-backups')
@login_required
def get_s3_cluster_backups(s3_storage_id):
    """Get cluster backups available in S3 storage for recovery."""
    try:
        s3_storage = S3Storage.query.get_or_404(s3_storage_id)
        
        # Get cluster backups from backup metadata
        backups = BackupMetadataService.get_cluster_backups_for_s3_storage(s3_storage_id)
        
        return jsonify({
            'success': True,
            'backups': backups
        })
    except Exception as e:
        logger.error(f"Error getting S3 cluster backups for storage {s3_storage_id}: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

def process_recovery_initiation(data, recovery_type='database'):
    """Common logic for processing recovery initiation."""
    # Validate request data
    is_valid, error_msg = validate_recovery_request(data, recovery_type)
    if not is_valid:
        return {
            'success': False,
            'error': error_msg,
            'status_code': 400
        }
    
    # Get target server and S3 storage
    target_server, s3_storage, error_msg = get_target_server_and_storage(
        data.get('target_server_id'), 
        data.get('s3_storage_id')
    )
    if error_msg:
        return {
            'success': False,
            'error': error_msg,
            'status_code': 404
        }
    
    backup_key = data.get('backup_key')
    
    return {
        'success': True,
        'target_server': target_server,
        's3_storage': s3_storage,
        'backup_key': backup_key
    }

@recovery_bp.route('/initiate', methods=['POST'])
@login_required
def initiate_recovery():
    """Unified recovery endpoint using shared WAL-G configuration."""
    try:
        data = request.get_json()
        recovery_type = data.get('recovery_type', 'database')
        
        # Process common recovery logic
        result = process_recovery_initiation(data, recovery_type)
        if not result['success']:
            return jsonify({
                'success': False,
                'error': result['error']
            }), result['status_code']
        
        target_server = result['target_server']
        s3_storage = result['s3_storage']
        backup_key = result['backup_key']
        
        # Validate backup exists using shared WAL-G configuration
        from app.utils.walg_config import WalgConfig
        
        # Get database name for WAL-G environment
        database_name = data.get('database_name', 'postgres')
        
        # Create WAL-G environment for validation
        walg_env = WalgConfig.create_env(s3_storage, database_name)
        
        # Validate backup exists
        backup_valid, backup_message = WalgConfig.verify_backup(
            None,  # SSH manager not needed for S3 validation
            walg_env,
            backup_key
        )
        
        if not backup_valid:
            return jsonify({
                'success': False,
                'error': f'Backup validation failed: {backup_message}'
            }), 404
        
        # Create backup info for the restore service
        backup_name = backup_key.split('/')[-1] if '/' in backup_key else backup_key
        selected_backup = {
            'key': backup_key,
            'backup_name': backup_name,
            's3_storage': s3_storage
        }
        
        # Handle database-specific logic
        if recovery_type == 'database':
            database_name = data.get('database_name')
            # Check if this is a backup identifier rather than actual database name
            if database_name and database_name.startswith('basebackups_'):
                # Use 'postgres' as the actual database name for WAL-G prefix
                actual_database_name = 'postgres'
                logger.info(f"Detected backup identifier '{database_name}', using 'postgres' as database name")
            else:
                actual_database_name = database_name
            selected_backup['database_name'] = actual_database_name
            
            # For disaster recovery, register database if it doesn't exist
            database = PostgresDatabase.query.filter_by(name=database_name).first()
            backup_job = None
            
            if database:
                backup_job = BackupJob.query.filter_by(vps_server_id=database.vps_server_id).first()
            else:
                from app.models.database import db
                database = PostgresDatabase(
                    name=database_name,
                    vps_server_id=target_server.id,
                    size='Unknown'
                )
                db.session.add(database)
                db.session.commit()
            
            # Initiate database recovery
            restore_service = RestoreService()
            recovery_result = restore_service.initiate_recovery(
                backup_key=backup_key,
                target_server=target_server,
                backup_info=selected_backup,
                database_id=database.id,
                s3_storage=s3_storage,
                backup_job=backup_job
            )
        else:
            # Initiate cluster recovery
            restore_service = RestoreService()
            recovery_result = restore_service.initiate_cluster_recovery(
                backup_key=backup_key,
                target_server=target_server,
                backup_info=selected_backup,
                s3_storage=s3_storage
            )
        
        return handle_recovery_result(recovery_result, recovery_type.capitalize())
            
    except Exception as e:
        logger.error(f"Error initiating {recovery_type} recovery: {str(e)}")
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500

# Legacy endpoints for backward compatibility
@recovery_bp.route('/initiate-db', methods=['POST'])
@login_required
def initiate_database_recovery():
    """Legacy database recovery endpoint - redirects to unified endpoint."""
    data = request.get_json()
    data['recovery_type'] = 'database'
    request._cached_json = data
    return initiate_recovery()

@recovery_bp.route('/initiate-cluster', methods=['POST'])
@login_required
def initiate_cluster_recovery():
    """Legacy cluster recovery endpoint - redirects to unified endpoint."""
    data = request.get_json()
    data['recovery_type'] = 'cluster'
    request._cached_json = data
    return initiate_recovery()

def handle_recovery_result(recovery_result, recovery_type):
    """Handle the result of a recovery operation."""
    if recovery_result['success']:
        flash(f'{recovery_type} recovery initiated successfully. Recovery ID: {recovery_result["recovery_id"]}', 'success')
        return jsonify({
            'success': True,
            'recovery_id': recovery_result['recovery_id'],
            'message': f'{recovery_type} recovery process initiated successfully'
        })
    else:
        return jsonify({
            'success': False,
            'error': recovery_result.get('error', 'Unknown error occurred')
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