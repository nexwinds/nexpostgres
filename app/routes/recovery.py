from flask import Blueprint, render_template, request, jsonify, flash, redirect, url_for
from flask_login import login_required
from app.utils.backup_metadata_service import BackupMetadataService
from app.models.database import VpsServer, PostgresDatabase, BackupJob, S3Storage
from app.utils.restore_service import RestoreService
import logging

logger = logging.getLogger(__name__)

recovery_bp = Blueprint('recovery', __name__, url_prefix='/recovery')

@recovery_bp.route('/')
@login_required
def index():
    """Database recovery main page - disaster recovery with S3 selection."""
    try:
        # Get all S3 storage configurations for selection
        s3_storages = S3Storage.query.all()
        
        # Get all servers for recovery target selection
        servers = VpsServer.query.all()
        
        return render_template('recovery/index.html', 
                             s3_storages=s3_storages,
                             servers=servers)
    except Exception as e:
        logger.error(f"Error loading recovery page: {str(e)}")
        flash('Error loading recovery page', 'error')
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

@recovery_bp.route('/initiate', methods=['POST'])
@login_required
def initiate_recovery():
    """Initiate database recovery process."""
    try:
        data = request.get_json()
        
        database_name = data.get('database_name')
        backup_key = data.get('backup_key')
        target_server_id = data.get('target_server_id')
        s3_storage_id = data.get('s3_storage_id')
        
        if not all([database_name, backup_key, target_server_id, s3_storage_id]):
            return jsonify({
                'success': False,
                'error': 'Missing required parameters'
            }), 400
        
        # Get S3 storage
        s3_storage = S3Storage.query.get(s3_storage_id)
        if not s3_storage:
            return jsonify({
                'success': False,
                'error': 'S3 storage not found'
            }), 404
        
        # Get target server
        target_server = VpsServer.query.get(target_server_id)
        if not target_server:
            return jsonify({
                'success': False,
                'error': 'Target server not found'
            }), 404
        
        # For recovery, validate that the backup exists in the specified S3 storage
        # We don't use get_database_backups since it's for backup jobs, not recovery
        import boto3
        from botocore.exceptions import ClientError
        
        try:
            # Initialize S3 client for the specified storage
            s3_client = boto3.client(
                's3',
                aws_access_key_id=s3_storage.access_key,
                aws_secret_access_key=s3_storage.secret_key,
                region_name=s3_storage.region,
                endpoint_url=s3_storage.endpoint if s3_storage.endpoint else None
            )
            
            # Check if the backup key exists in S3
            s3_client.head_object(Bucket=s3_storage.bucket, Key=backup_key)
            
            # Create backup info for the restore service
            # Extract backup name from backup key (e.g., "postgres/dbname/backup_20230823_120000.tar.gz")
            backup_name = backup_key.split('/')[-1] if '/' in backup_key else backup_key
            selected_backup = {
                'key': backup_key,
                'backup_name': backup_name,
                'database_name': database_name,
                's3_storage': s3_storage
            }
            
        except ClientError as e:
            if e.response['Error']['Code'] == '404':
                return jsonify({
                    'success': False,
                    'error': 'Selected backup not found in S3 storage'
                }), 404
            else:
                logger.error(f"Error accessing S3 storage: {str(e)}")
                return jsonify({
                    'success': False,
                    'error': f'Error accessing S3 storage: {str(e)}'
                }), 500
        except Exception as e:
            logger.error(f"Error validating backup: {str(e)}")
            return jsonify({
                'success': False,
                'error': f'Error validating backup: {str(e)}'
            }), 500
        
        # For disaster recovery, we may need to register the database if it doesn't exist
        database = PostgresDatabase.query.filter_by(name=database_name).first()
        backup_job = None
        
        if database:
            # Database exists, try to find backup job
            backup_job = BackupJob.query.filter_by(database_id=database.id).first()
        else:
            # Database doesn't exist - this is a disaster recovery scenario
            # We'll create a temporary database entry for the recovery process
            from app.models.database import db
            database = PostgresDatabase(
                name=database_name,
                vps_server_id=target_server_id,
                size='Unknown'  # Will be updated after recovery
            )
            db.session.add(database)
            db.session.commit()
        
        # Initiate the recovery process
        restore_service = RestoreService()
        recovery_result = restore_service.initiate_recovery(
            backup_key=backup_key,
            target_server=target_server,
            backup_info=selected_backup,
            database_id=database.id,
            s3_storage=s3_storage,
            backup_job=backup_job
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