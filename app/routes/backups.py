from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from app.models.database import BackupJob, BackupLog, RestoreLog, PostgresDatabase, S3Storage
from app.models.database import db
from app.utils.backup_service import BackupService, BackupRestoreService, S3TestService
from app.utils.unified_validation_service import UnifiedValidationService
from datetime import datetime

backups_bp = Blueprint('backups', __name__)


@backups_bp.route('/backups')
def backups():
    """Display all backup jobs."""
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
            # Only show databases without backup jobs
            databases = PostgresDatabase.query.filter(~PostgresDatabase.id.in_(
                db.session.query(BackupJob.database_id).distinct()
            )).all()
            s3_storages = S3Storage.query.all()
            return render_template('backups/add.html', 
                                 databases=databases, 
                                 s3_storages=s3_storages)
        
        # Create backup job first
        backup_service = BackupService()
        backup_job = backup_service.create_backup_job(
            name=validated_data['name'],
            database_id=validated_data['database_id'],
            backup_type=validated_data['backup_type'],
            cron_expression=validated_data['cron_expression'],
            s3_storage_id=validated_data['s3_storage_id'],
            retention_count=validated_data['retention_count']
        )
        
        if not backup_job:
            flash('Failed to create backup job', 'danger')
            databases = PostgresDatabase.query.all()
            s3_storages = S3Storage.query.all()
            return render_template('backups/add.html', 
                                 databases=databases, 
                                 s3_storages=s3_storages)
        
        # Check and configure backup with the created backup job
        success, message = backup_service.check_and_configure_backup(
            validated_data['database'], 
            validated_data['s3_storage'],
            backup_job
        )
        
        if not success:
            # Delete the backup job if configuration fails
            backup_service.delete_backup_job(backup_job)
            flash(message, 'danger')
            databases = PostgresDatabase.query.all()
            s3_storages = S3Storage.query.all()
            return render_template('backups/add.html', 
                                 databases=databases, 
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
    
    # Only show databases without backup jobs for new backup creation
    databases = PostgresDatabase.query.filter(~PostgresDatabase.id.in_(
        db.session.query(BackupJob.database_id).distinct()
    )).all()
    s3_storages = S3Storage.query.all()
    return render_template('backups/add.html', 
                         databases=databases, 
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
            # For editing: show current database + databases without backup jobs
            databases_without_backup = PostgresDatabase.query.filter(~PostgresDatabase.id.in_(
                db.session.query(BackupJob.database_id).distinct()
            )).all()
            # Ensure current database is included
            if backup_job.database not in databases_without_backup:
                databases = [backup_job.database] + databases_without_backup
            else:
                databases = databases_without_backup
            s3_storages = S3Storage.query.all()
            return render_template('backups/edit.html', 
                                 backup_job=backup_job,
                                 databases=databases, 
                                 s3_storages=s3_storages)
        
        # Update backup job
        backup_service = BackupService()
        backup_service.update_backup_job(
            backup_job=backup_job,
            name=validated_data['name'],
            database_id=validated_data['database_id'],
            backup_type=validated_data['backup_type'],
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
    
    # For editing: show current database + databases without backup jobs
    databases_without_backup = PostgresDatabase.query.filter(~PostgresDatabase.id.in_(
        db.session.query(BackupJob.database_id).distinct()
    )).all()
    # Ensure current database is included
    if backup_job.database not in databases_without_backup:
        databases = [backup_job.database] + databases_without_backup
    else:
        databases = databases_without_backup
    s3_storages = S3Storage.query.all()
    return render_template('backups/edit.html', 
                         backup_job=backup_job,
                         databases=databases, 
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


@backups_bp.route('/backup_logs')
def backup_logs():
    """Display backup logs with filtering."""
    # Get filter parameters
    backup_job_id = request.args.get('backup_job_id')
    status = request.args.get('status')
    
    # Get backup job if backup_job_id is provided
    backup_job = None
    if backup_job_id:
        backup_job = BackupJob.query.get(backup_job_id)
    
    # Query backup logs with filters
    query = BackupLog.query
    if backup_job_id:
        query = query.filter_by(backup_job_id=backup_job_id)
    if status:
        query = query.filter_by(status=status)
    logs = query.order_by(BackupLog.start_time.desc()).all()
    
    backup_jobs = BackupJob.query.all()
    return render_template('backups/logs.html', 
                         logs=logs, 
                         backup_jobs=backup_jobs,
                         backup_job=backup_job,
                         selected_backup_job_id=backup_job_id,
                         selected_status=status)


@backups_bp.route('/restore', methods=['GET', 'POST'])
def restore():
    """Initiate database restore."""
    if request.method == 'POST':
        # Validate form data
        is_valid, errors, validated_data = UnifiedValidationService.validate_restore_form_data(request.form)
        
        if not is_valid:
            UnifiedValidationService.flash_validation_errors(errors)
            backup_jobs = BackupJob.query.all()
            databases = PostgresDatabase.query.all()
            return render_template('backups/restore.html', 
                                 backup_jobs=backup_jobs,
                                 databases=databases)
        
        # Execute restore
        restore_service = BackupRestoreService()
        success, message = restore_service.execute_restore(validated_data)
        
        if success:
            flash(message, 'success')
            return redirect(url_for('backups.restore_logs'))
        else:
            flash(message, 'danger')
    
    backup_jobs = BackupJob.query.all()
    databases = PostgresDatabase.query.all()
    return render_template('backups/restore.html', 
                         backup_jobs=backup_jobs,
                         databases=databases)


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


@backups_bp.route('/view_log/<int:id>')
def view_log(id):
    """View detailed backup log."""
    log = BackupLog.query.get_or_404(id)
    return render_template('backups/view_log.html', log=log)


@backups_bp.route('/test_s3/<int:s3_storage_id>', methods=['POST'])
def test_s3(s3_storage_id):
    """Test S3 connection."""
    # Validate S3 storage exists
    is_valid, error, s3_storage = UnifiedValidationService.validate_s3_storage_exists(s3_storage_id)
    if not is_valid:
        return jsonify({'success': False, 'message': error})
    
    s3_test_service = S3TestService()
    success, message = s3_test_service.test_s3_connection(s3_storage)
    
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


@backups_bp.route('/apply_retention/<int:backup_job_id>', methods=['POST'])
def apply_retention(backup_job_id):
    """Apply retention policy to backup job."""
    # Validate backup job exists
    is_valid, error, backup_job = UnifiedValidationService.validate_backup_job_exists(backup_job_id)
    if not is_valid:
        flash(error, 'danger')
        return redirect(url_for('backups.backups'))
    
    backup_service = BackupService()
    success, message = backup_service.apply_retention_policy(backup_job)
    
    flash(message, 'success' if success else 'danger')
    return redirect(url_for('backups.backups'))


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
    
    logs = BackupLog.query.filter_by(
        backup_job_id=backup_job_id,
        status='completed'
    ).order_by(BackupLog.end_time.desc()).all()
    
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