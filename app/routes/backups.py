from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from app.models.database import BackupJob, RestoreLog, PostgresDatabase, S3Storage, VpsServer
from app.models.database import db
from app.utils.backup_service import BackupService, BackupRestoreService
from app.utils.unified_validation_service import UnifiedValidationService

backups_bp = Blueprint('backups', __name__)


@backups_bp.route('/backups')
def backups():
    """Display backup jobs or backup logs based on parameters."""
    # Check if backup_job_id is provided to show logs instead of jobs
    backup_job_id = request.args.get('backup_job_id')
    status = request.args.get('status')
    days = request.args.get('days')
    
    if backup_job_id or status or days:
        # Show backup logs (previous backup_logs functionality)
        backup_job = None
        if backup_job_id:
            backup_job = BackupJob.query.get(backup_job_id)
        
        # Get backup logs using metadata service
        from app.utils.backup_metadata_service import BackupMetadataService
        logs = BackupMetadataService.get_all_backup_logs(
            job_id=int(backup_job_id) if backup_job_id else None,
            status=status,
            days=int(days) if days and days != 'all' else None
        )
        
        backup_jobs = BackupJob.query.all()
        return render_template('backups/logs.html', 
                             logs=logs, 
                             backup_jobs=backup_jobs,
                             backup_job=backup_job,
                             selected_backup_job_id=backup_job_id,
                             selected_status=status)
    else:
        # Show backup jobs (original functionality)
        backup_jobs = BackupJob.query.all()
        return render_template('backups/index.html', backup_jobs=backup_jobs)


@backups_bp.route('/backups/add', methods=['GET', 'POST'])
def add_backup():
    """Add a new backup job."""
    if request.method == 'POST':
        # Validate form data
        is_valid, errors, validated_data = UnifiedValidationService.validate_backup_form_data(request.form)
        
        if not is_valid:
            UnifiedValidationService.flash_validation_errors(errors)
            # Only show servers without backup jobs
            servers = VpsServer.query.filter(~VpsServer.id.in_(
                db.session.query(BackupJob.vps_server_id).distinct()
            )).all()
            s3_storages = S3Storage.query.all()
            return render_template('backups/add.html', 
                                 servers=servers, 
                                 s3_storages=s3_storages)
        
        # Create and configure backup job
        backup_service = BackupService()
        backup_job, message = backup_service.create_backup_job(
            name=validated_data['name'],
            server_id=validated_data['server_id'],
            cron_expression=validated_data['cron_expression'],
            s3_storage_id=validated_data['s3_storage_id'],
            retention_count=validated_data['retention_count']
        )
        
        if not backup_job:
            flash(message, 'danger')
            servers = VpsServer.query.filter(~VpsServer.id.in_(
                db.session.query(BackupJob.vps_server_id).distinct()
            )).all()
            s3_storages = S3Storage.query.all()
            return render_template('backups/add.html', 
                                 servers=servers, 
                                 s3_storages=s3_storages)
        
        if backup_job:
            # Schedule the backup job
            schedule_success, schedule_message = backup_service.schedule_backup_job_safe(backup_job)
            if schedule_success:
                success, message = True, f'Backup job "{backup_job.name}" created and scheduled successfully'
            else:
                success, message = True, f'Backup job "{backup_job.name}" created but scheduling failed: {schedule_message}'
        else:
            success, message = False, 'Failed to create backup job'
        
        if success:
            flash(message, 'success')
            return redirect(url_for('backups.backups'))
        else:
            flash(message, 'danger')
    
    # Only show servers without backup jobs for new backup creation
    servers = VpsServer.query.filter(~VpsServer.id.in_(
        db.session.query(BackupJob.vps_server_id).distinct()
    )).all()
    s3_storages = S3Storage.query.all()
    return render_template('backups/add.html', 
                         servers=servers, 
                         s3_storages=s3_storages)


@backups_bp.route('/backups/edit/<int:backup_job_id>', methods=['GET', 'POST'])
def edit_backup(backup_job_id):
    """Edit an existing backup job."""
    # Validate backup job exists
    is_valid, error, backup_job = UnifiedValidationService.validate_backup_job_exists(backup_job_id)
    if not is_valid:
        flash(error, 'danger')
        return redirect(url_for('backups.backups'))
    
    if request.method == 'POST':
        # Add backup job ID to form data for validation
        form_data = dict(request.form)
        form_data['backup_job_id'] = backup_job_id
        
        # Validate form data
        is_valid, errors, validated_data = UnifiedValidationService.validate_backup_form_data(form_data)
        
        if not is_valid:
            UnifiedValidationService.flash_validation_errors(errors)
            # For editing: show current server + servers without backup jobs
            servers_without_backup = VpsServer.query.filter(~VpsServer.id.in_(
                db.session.query(BackupJob.vps_server_id).distinct()
            )).all()
            # Ensure current server is included
            if backup_job.server not in servers_without_backup:
                servers = [backup_job.server] + servers_without_backup
            else:
                servers = servers_without_backup
            s3_storages = S3Storage.query.all()
            return render_template('backups/edit.html', 
                                 backup_job=backup_job,
                                 servers=servers, 
                                 s3_storages=s3_storages)
        
        # Update backup job
        backup_service = BackupService()
        backup_service.update_backup_job(
            backup_job=backup_job,
            name=validated_data['name'],
            server_id=validated_data['server_id'],
            cron_expression=validated_data['cron_expression'],
            enabled=validated_data.get('enabled', True),
            s3_storage_id=validated_data['s3_storage_id'],
            retention_count=validated_data['retention_count']
        )
        success, message = True, f'Backup job "{backup_job.name}" updated successfully'
        
        if success:
            flash(message, 'success')
            return redirect(url_for('backups.backups'))
        else:
            flash(message, 'danger')
    
    # For editing: show current server + servers without backup jobs
    servers_without_backup = VpsServer.query.filter(~VpsServer.id.in_(
        db.session.query(BackupJob.vps_server_id).distinct()
    )).all()
    # Ensure current server is included
    if backup_job.server not in servers_without_backup:
        servers = [backup_job.server] + servers_without_backup
    else:
        servers = servers_without_backup
    s3_storages = S3Storage.query.all()
    return render_template('backups/edit.html', 
                         backup_job=backup_job,
                         servers=servers, 
                         s3_storages=s3_storages)


@backups_bp.route('/backups/delete/<int:backup_job_id>', methods=['POST'])
def delete_backup(backup_job_id):
    """Delete a backup job."""
    # Validate backup job exists
    is_valid, error, backup_job = UnifiedValidationService.validate_backup_job_exists(backup_job_id)
    if not is_valid:
        flash(error, 'danger')
        return redirect(url_for('backups.backups'))
    
    backup_service = BackupService()
    success, message = backup_service.delete_backup_job(backup_job)
    
    flash(message, 'success' if success else 'danger')
    return redirect(url_for('backups.backups'))


@backups_bp.route('/backups/execute/<int:backup_job_id>', methods=['POST'])
def execute_backup(backup_job_id):
    """Execute a backup job manually."""
    # Validate backup job exists
    is_valid, error, backup_job = UnifiedValidationService.validate_backup_job_exists(backup_job_id)
    if not is_valid:
        flash(error, 'danger')
        return redirect(url_for('backups.backups'))
    
    backup_service = BackupService()
    success, message = backup_service.execute_backup(backup_job)
    
    flash(message, 'success' if success else 'danger')
    return redirect(url_for('backups.backups'))


# Removed backup_logs route - functionality merged into /backups route


@backups_bp.route('/restore', methods=['GET', 'POST'])
def restore():
    """Initiate database restore - routine restoration with database selection."""
    if request.method == 'POST':
        # Validate form data
        is_valid, errors, validated_data = UnifiedValidationService.validate_restore_form_data(request.form)
        
        if not is_valid:
            UnifiedValidationService.flash_validation_errors(errors)
            # Get databases that have backup jobs (one-to-one relationship)
            databases_with_backups = PostgresDatabase.query.join(BackupJob).all()
            return render_template('backups/restore.html', 
                                 databases=databases_with_backups)
        
        # Execute restore
        restore_service = BackupRestoreService()
        success, message = restore_service.execute_restore(validated_data)
        
        if success:
            flash(message, 'success')
            return redirect(url_for('backups.restore_logs'))
        else:
            flash(message, 'danger')
    
    # Get databases that have backup jobs (one-to-one relationship)
    databases_with_backups = PostgresDatabase.query.join(BackupJob).all()
    return render_template('backups/restore.html', 
                         databases=databases_with_backups)


@backups_bp.route('/restore_logs')
def restore_logs():
    """Display restore logs."""
    logs = RestoreLog.query.order_by(RestoreLog.created_at.desc()).all()
    return render_template('backups/restore_logs.html', logs=logs)


@backups_bp.route('/restore_logs/<int:log_id>')
def view_restore_log(log_id):
    """View detailed restore log."""
    log = RestoreLog.query.get_or_404(log_id)
    return render_template('backups/view_restore_log.html', log=log)


@backups_bp.route('/restore_logs/<int:log_id>/status')
def restore_log_status(log_id):
    """Get current status of a restore log for polling."""
    try:
        log = RestoreLog.query.get_or_404(log_id)
        
        return jsonify({
            'success': True,
            'log': {
                'id': log.id,
                'status': log.status,
                'start_time': log.start_time.isoformat() if log.start_time else None,
                'end_time': log.end_time.isoformat() if log.end_time else None,
                'log_output': log.log_output,
                'error_message': getattr(log, 'error_message', None)
            }
        })
    except Exception as e:
        return jsonify({
            'success': False,
            'error': str(e)
        }), 500


@backups_bp.route('/view_log/<backup_id>')
def view_log(backup_id):
    """View detailed backup log."""
    # Parse backup_id to get job_id and backup_name
    try:
        if '_' in backup_id:
            job_id, backup_name = backup_id.split('_', 1)
            from app.utils.backup_metadata_service import BackupMetadataService
            log = BackupMetadataService.find_backup_by_name_or_time(int(job_id), backup_name=backup_name)
            if not log:
                flash('Backup log not found', 'error')
                return redirect(url_for('backups.backup_logs'))
        else:
            flash('Invalid backup log ID', 'error')
            return redirect(url_for('backups.backup_logs'))
    except (ValueError, TypeError):
        flash('Invalid backup log ID', 'error')
        return redirect(url_for('backups.backup_logs'))
    
    return render_template('backups/view_log.html', log=log)


@backups_bp.route('/test_s3/<int:s3_storage_id>', methods=['POST'])
def test_s3(s3_storage_id):
    """Test S3 connection."""
    # Validate S3 storage exists
    is_valid, error, s3_storage = UnifiedValidationService.validate_s3_storage_exists(s3_storage_id)
    if not is_valid:
        return jsonify({'success': False, 'message': error})
    
    # Use BackupService for S3 testing
    success, message = BackupService.test_s3_connection(
        s3_storage.bucket_name,
        s3_storage.region,
        s3_storage.access_key,
        s3_storage.secret_key
    )
    
    return jsonify({'success': success, 'message': message})


@backups_bp.route('/fix_archive_command/<int:database_id>', methods=['POST'])
def fix_archive_command(database_id):
    """Fix PostgreSQL archive command configuration."""
    # Validate database exists
    is_valid, error, database = UnifiedValidationService.validate_database_exists(database_id)
    if not is_valid:
        flash(error, 'danger')
        return redirect(url_for('backups.backups'))
    
    backup_service = BackupService()
    success, message = backup_service.fix_archive_command(database)
    
    flash(message, 'success' if success else 'danger')
    return redirect(url_for('backups.backups'))


# Removed apply_retention route - WAL-G handles retention automatically during backup operations


@backups_bp.route('/api/logs/<int:backup_job_id>')
def api_logs(backup_job_id):
    """API endpoint for backup logs."""
    # Validate backup job exists
    is_valid, error, backup_job = UnifiedValidationService.validate_backup_job_exists(backup_job_id)
    if not is_valid:
        return jsonify({'error': error}), 404
    
    logs = BackupService.get_backup_logs_for_api(backup_job_id)
    
    return jsonify({
        'logs': logs
    })


@backups_bp.route('/api/recovery_points/<int:database_id>')
def api_recovery_points(database_id):
    """API endpoint for recovery points."""
    # Validate database exists
    is_valid, error, database = UnifiedValidationService.validate_database_exists(database_id)
    if not is_valid:
        return jsonify({'error': error}), 404
    
    restore_service = BackupRestoreService()
    success, recovery_points = restore_service.get_recovery_points(database)
    
    if not success:
        return jsonify({'error': 'Failed to retrieve recovery points'}), 500
    
    return jsonify({'recovery_points': recovery_points})


@backups_bp.route('/api/backups/<int:backup_job_id>/recovery-points')
def api_backup_recovery_points(backup_job_id):
    """API endpoint for recovery points by backup job ID with date filtering."""
    # Validate backup job exists
    is_valid, error, backup_job = UnifiedValidationService.validate_backup_job_exists(backup_job_id)
    if not is_valid:
        return jsonify({'error': error}), 404
    
    # Get the date parameter
    selected_date = request.args.get('date')
    if not selected_date:
        return jsonify({'error': 'Date parameter is required'}), 400
    
    # Get the database associated with this backup job
    database = backup_job.database
    if not database:
        return jsonify({'error': 'No database associated with this backup job'}), 404
    
    # Get real recovery points from the backup system
    try:
        restore_service = BackupRestoreService()
        success, recovery_points = restore_service.get_recovery_points(database)
        
        if not success:
            return jsonify({'error': 'Failed to retrieve recovery points'}), 500
        
        # Filter recovery points by the selected date
        from datetime import datetime
        import logging
        logger = logging.getLogger(__name__)
        
        logger.info(f"Total recovery points retrieved: {len(recovery_points)}")
        logger.info(f"Selected date for filtering: {selected_date}")
        
        filtered_points = []
        
        for point in recovery_points:
            try:
                logger.info(f"Processing recovery point: {point}")
                # Parse the datetime from the recovery point
                # Handle both ISO format with and without timezone
                datetime_str = point['datetime']
                if datetime_str.endswith('Z'):
                    datetime_str = datetime_str.replace('Z', '+00:00')
                point_datetime = datetime.fromisoformat(datetime_str)
                point_date = point_datetime.date().isoformat()
                
                logger.info(f"Point date: {point_date}, Selected date: {selected_date}")
                
                # Check if this point matches the selected date
                if point_date == selected_date:
                    filtered_point = {
                        'timestamp': point['datetime'],
                        'time': point_datetime.strftime('%H:%M'),
                        'size': point.get('size', 'Unknown')
                    }
                    filtered_points.append(filtered_point)
                    logger.info(f"Added filtered point: {filtered_point}")
            except (ValueError, KeyError) as e:
                logger.error(f"Error processing recovery point {point}: {e}")
                # Skip invalid recovery points
                continue
        
        logger.info(f"Total filtered points: {len(filtered_points)}")
        
        return jsonify({'recovery_points': filtered_points})
        
    except Exception as e:
        return jsonify({'error': f'Error retrieving recovery points: {str(e)}'}), 500


@backups_bp.route('/debug/restore_log/<int:log_id>')
def debug_restore_log(log_id):
    """Debug endpoint for restore log relationships."""
    log = RestoreLog.query.get_or_404(log_id)
    
    debug_info = {
        'restore_log_id': log.id,
        'database_id': log.database_id,
        'database_name': log.database.name if log.database else 'N/A',
        'backup_job_id': log.backup_job_id,
        'backup_job_name': log.backup_job.name if log.backup_job else 'N/A',
        'backup_log_id': log.backup_log_id,
        'backup_log_status': log.backup_log.status if log.backup_log else 'N/A',
        'status': log.status,
        'created_at': log.created_at.isoformat() if log.created_at else None,
        'completed_at': log.completed_at.isoformat() if log.completed_at else None
    }
    
    return jsonify(debug_info)


# Helper route for AJAX requests
@backups_bp.route('/get_backup_logs/<int:backup_job_id>')
def get_backup_logs(backup_job_id):
    """Get backup logs for a specific backup job (AJAX endpoint)."""
    # Validate backup job exists
    is_valid, error, backup_job = UnifiedValidationService.validate_backup_job_exists(backup_job_id)
    if not is_valid:
        return jsonify({'error': error}), 404
    
    from app.utils.backup_metadata_service import BackupMetadataService
    logs = BackupMetadataService.get_backup_logs_for_job(backup_job_id, status='completed')
    
    return jsonify({
        'logs': [{
            'id': log.id,
            'start_time': log.start_time.strftime('%Y-%m-%d %H:%M:%S') if log.start_time else '',
            'end_time': log.end_time.strftime('%Y-%m-%d %H:%M:%S') if log.end_time else '',
            'backup_name': getattr(log, 'backup_name', f"backup_{log.id}")
        } for log in logs]
    })


@backups_bp.route('/get_databases_for_server/<int:server_id>')
def get_databases_for_server(server_id):
    """Get databases for a specific server (AJAX endpoint)."""
    databases = PostgresDatabase.query.filter_by(server_id=server_id).all()
    
    return jsonify({
        'databases': [{
            'id': db.id,
            'name': db.name
        } for db in databases]
    })