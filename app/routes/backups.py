from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from app.models.database import BackupJob, BackupLog, PostgresDatabase, RestoreLog, S3Storage, db
from app.utils.ssh_manager import SSHManager
from app.utils.postgres_manager import PostgresManager
from app.utils.scheduler import schedule_backup_job, execute_manual_backup
from app.routes.auth import login_required, first_login_required
from datetime import datetime, timedelta
import functools
import os
import time
import re

backups_bp = Blueprint('backups', __name__, url_prefix='/backups')

# Helper function to create SSH and PostgreSQL manager
def get_managers(server):
    ssh = SSHManager(
        host=server.host,
        port=server.port,
        username=server.username,
        ssh_key_content=server.ssh_key_content
    )
    
    if not ssh.connect():
        return None, None
    
    return ssh, PostgresManager(ssh)

@backups_bp.route('/')
@login_required
@first_login_required
def index():
    backup_jobs = BackupJob.query.all()
    return render_template('backups/index.html', backup_jobs=backup_jobs)

@backups_bp.route('/add', methods=['GET', 'POST'])
@login_required
@first_login_required
def add():
    databases = PostgresDatabase.query.all()
    s3_storages = S3Storage.query.all()
    
    if not databases:
        flash('You need to add a database first', 'warning')
        return redirect(url_for('databases.add'))
    
    if not s3_storages:
        flash('You need to add an S3 storage configuration first', 'warning')
        return redirect(url_for('s3_storage.add'))
    
    if request.method == 'POST':
        name = request.form.get('name')
        database_id = request.form.get('database_id', type=int)
        backup_type = request.form.get('backup_type')
        cron_expression = request.form.get('cron_expression')
        s3_storage_id = request.form.get('s3_storage_id', type=int)
        retention_count = request.form.get('retention_count', type=int, default=7)
        
        # Validate data
        database = PostgresDatabase.query.get(database_id)
        s3_storage = S3Storage.query.get(s3_storage_id)
        
        if not database:
            flash('Selected database does not exist', 'danger')
            return render_template('backups/add.html', databases=databases, s3_storages=s3_storages)
        
        if not s3_storage:
            flash('Selected S3 storage configuration does not exist', 'danger')
            return render_template('backups/add.html', databases=databases, s3_storages=s3_storages)
        
        if backup_type not in ['full', 'incr']:
            flash('Invalid backup type', 'danger')
            return render_template('backups/add.html', databases=databases, s3_storages=s3_storages)
        
        # Validate retention count
        if not retention_count or retention_count < 1:
            flash('Retention count must be at least 1', 'danger')
            return render_template('backups/add.html', databases=databases, s3_storages=s3_storages)
        
        # Create backup job
        backup_job = BackupJob(
            name=name,
            database_id=database_id,
            vps_server_id=database.vps_server_id,
            backup_type=backup_type,
            cron_expression=cron_expression,
            s3_storage_id=s3_storage_id,
            retention_count=retention_count
        )
        
        db.session.add(backup_job)
        db.session.commit()
        
        # Verify and setup backup configuration if needed
        _check_and_configure_backup(database, s3_storage)
        
        # Schedule the backup job
        try:
            schedule_backup_job(backup_job)
            flash('Backup job created and scheduled successfully', 'success')
        except Exception as e:
            flash(f'Backup job created but scheduling failed: {str(e)}', 'warning')
        
        return redirect(url_for('backups.index'))
    
    return render_template('backups/add.html', databases=databases, s3_storages=s3_storages)

def _check_and_configure_backup(database, s3_storage):
    """Helper function to check and configure backup if needed"""
    try:
        # Connect to server and check backup configuration
        ssh, pg_manager = get_managers(database.server)
        
        if not ssh or not pg_manager:
            flash('Unable to connect to server to verify backup configuration', 'warning')
            return
            
        # Check if configuration is valid
        check_cmd = f"sudo -u postgres pgbackrest --stanza={database.name} check"
        check_result = ssh.execute_command(check_cmd)
        
        if check_result['exit_code'] != 0:
            flash('Configuring backup system...', 'info')
            
            # Configure pgBackRest based on storage type
            if s3_storage:
                pg_manager.setup_pgbackrest_config(
                    database.name,
                    s3_storage.bucket,
                    s3_storage.region,
                    s3_storage.access_key,
                    s3_storage.secret_key
                )
            else:
                pg_manager.update_pgbackrest_config(database.name)
                
            flash('Backup system configured successfully', 'success')
        else:
            flash('Backup configuration is valid', 'success')
            
        # Clean up
        ssh.disconnect()
    except Exception as e:
        flash(f'Configuration check failed: {str(e)}', 'warning')

@backups_bp.route('/edit/<int:id>', methods=['GET', 'POST'])
@login_required
@first_login_required
def edit(id):
    backup_job = BackupJob.query.get_or_404(id)
    databases = PostgresDatabase.query.all()
    s3_storages = S3Storage.query.all()
    
    if request.method == 'POST':
        name = request.form.get('name')
        database_id = request.form.get('database_id', type=int)
        backup_type = request.form.get('backup_type')
        cron_expression = request.form.get('cron_expression')
        enabled = request.form.get('enabled') == 'true'
        s3_storage_id = request.form.get('s3_storage_id', type=int)
        retention_count = request.form.get('retention_count', type=int, default=7)
        
        # Validate data
        database = PostgresDatabase.query.get(database_id)
        s3_storage = S3Storage.query.get(s3_storage_id)
        
        if not database:
            flash('Selected database does not exist', 'danger')
            return render_template('backups/edit.html', backup_job=backup_job, databases=databases, s3_storages=s3_storages)
        
        if not s3_storage:
            flash('Selected S3 storage configuration does not exist', 'danger')
            return render_template('backups/edit.html', backup_job=backup_job, databases=databases, s3_storages=s3_storages)
        
        if backup_type not in ['full', 'incr']:
            flash('Invalid backup type', 'danger')
            return render_template('backups/edit.html', backup_job=backup_job, databases=databases, s3_storages=s3_storages)
        
        # Validate retention count
        if not retention_count or retention_count < 1:
            flash('Retention count must be at least 1', 'danger')
            return render_template('backups/edit.html', backup_job=backup_job, databases=databases, s3_storages=s3_storages)
        
        # Update backup job
        backup_job.name = name
        backup_job.database_id = database_id
        backup_job.vps_server_id = database.vps_server_id
        backup_job.backup_type = backup_type
        backup_job.cron_expression = cron_expression
        backup_job.enabled = enabled
        backup_job.s3_storage_id = s3_storage_id
        backup_job.retention_count = retention_count
        
        db.session.commit()

        # Check configuration if job is enabled
        if enabled:
            _check_and_configure_backup(database, s3_storage)
            
            # Reschedule the job
            try:
                schedule_backup_job(backup_job)
                flash('Backup job updated and rescheduled successfully', 'success')
            except Exception as e:
                flash(f'Backup job updated but rescheduling failed: {str(e)}', 'warning')
        else:
            flash('Backup job updated successfully (job is disabled)', 'success')
        
        return redirect(url_for('backups.index'))
    
    return render_template('backups/edit.html', backup_job=backup_job, databases=databases, s3_storages=s3_storages)

@backups_bp.route('/delete/<int:id>', methods=['POST'])
@login_required
@first_login_required
def delete(id):
    backup_job = BackupJob.query.get_or_404(id)
    db.session.delete(backup_job)
    db.session.commit()
    flash('Backup job deleted successfully', 'success')
    return redirect(url_for('backups.index'))

@backups_bp.route('/execute/<int:id>', methods=['POST'])
@login_required
@first_login_required
def execute(id):
    backup_job = BackupJob.query.get_or_404(id)
    
    try:
        # Execute the backup directly
        success, message = execute_manual_backup(backup_job.id)
        flash('Backup executed successfully' if success else f'Backup failed: {message}', 
              'success' if success else 'danger')
    except Exception as e:
        flash(f'Error executing backup: {str(e)}', 'danger')
    
    return redirect(url_for('backups.logs', job_id=backup_job.id))

@backups_bp.route('/logs')
@login_required
@first_login_required
def logs():
    job_id = request.args.get('job_id', type=int)
    status = request.args.get('status')
    days = request.args.get('days', '7')
    
    # Build query based on filters
    query = BackupLog.query
    
    # Filter by backup job
    if job_id:
        query = query.filter(BackupLog.backup_job_id == job_id)
        backup_job = BackupJob.query.get(job_id)
    else:
        backup_job = None
        # Get all backup jobs for dropdown
        backup_jobs = BackupJob.query.all()
        
    # Filter by status
    if status:
        query = query.filter(BackupLog.status == status)
    
    # Filter by date
    if days and days != 'all':
        date_threshold = datetime.utcnow() - timedelta(days=int(days))
        query = query.filter(BackupLog.start_time >= date_threshold)
        
    # Order by start time (most recent first)
    logs = query.order_by(BackupLog.start_time.desc()).all()
    
    if job_id:
        return render_template('backups/logs.html', logs=logs, backup_job=backup_job, 
                               selected_status=status, selected_days=days)
    else:
        return render_template('backups/logs.html', logs=logs, backup_jobs=backup_jobs, 
                               selected_job_id=job_id, selected_status=status, selected_days=days)

@backups_bp.route('/logs/view/<int:id>')
@login_required
@first_login_required
def view_log(id):
    log = BackupLog.query.get_or_404(id)
    # Calculate size_mb from size_bytes
    if log.size_bytes:
        log.size_mb = round(log.size_bytes / (1024 * 1024), 2)
    else:
        log.size_mb = None
    return render_template('backups/view_log.html', log=log)

@backups_bp.route('/restore', methods=['GET', 'POST'])
@login_required
@first_login_required
def restore():
    databases = PostgresDatabase.query.all()
    backup_jobs = BackupJob.query.all()
    
    if request.method == 'POST':
        # Get form data
        backup_job_id = request.form.get('backup_job_id', type=int)
        database_id = request.form.get('database_id', type=int)
        backup_log_id = request.form.get('backup_log_id', type=int)
        recovery_time = request.form.get('recovery_time')
        restore_to_same = request.form.get('restore_to_same') == 'true'
        use_recovery_time = request.form.get('use_recovery_time') == 'true'
        
        # Debug info
        print(f"Form data - backup_job_id: {backup_job_id}, database_id: {database_id}, backup_log_id: {backup_log_id}, use_recovery_time: {use_recovery_time}")
        
        # Use target database if not restoring to same
        if not restore_to_same:
            target_database_id = request.form.get('target_database_id', type=int)
            if target_database_id:
                database_id = target_database_id
        
        # Validate backup job and database
        if not backup_job_id:
            flash('Please select a backup job', 'danger')
            return render_template('backups/restore.html', databases=databases, backup_jobs=backup_jobs)
            
        backup_job = BackupJob.query.get(backup_job_id)
        if not backup_job:
            flash('Selected backup job does not exist', 'danger')
            return render_template('backups/restore.html', databases=databases, backup_jobs=backup_jobs)
            
        # Set database ID from backup job if not set
        if not database_id:
            database_id = backup_job.database_id
            
        # Validate database
        database = PostgresDatabase.query.get(database_id)
        if not database:
            flash('Selected database does not exist', 'danger')
            return render_template('backups/restore.html', databases=databases, backup_jobs=backup_jobs)
        
        # Get backup information if log ID is provided
        backup_name = None
        if backup_log_id:
            backup_log = BackupLog.query.get(backup_log_id)
            if not backup_log:
                flash('Selected backup does not exist', 'danger')
                return render_template('backups/restore.html', databases=databases, backup_jobs=backup_jobs)
                
            # Get backup job and check against database
            backup_job = backup_log.backup_job
            if backup_job.database_id != database_id and restore_to_same:
                flash('Backup job does not match selected database', 'danger')
                return render_template('backups/restore.html', databases=databases, backup_jobs=backup_jobs)
            
            # Get the backup name from the database
            # Format for pgBackRest is typically like "latest", "20230101-123456F", etc.
            server = backup_job.server
            ssh = SSHManager(
                host=server.host,
                port=server.port,
                username=server.username,
                ssh_key_content=server.ssh_key_content
            )
            
            if ssh.connect():
                pg_manager = PostgresManager(ssh)
                backups = pg_manager.list_backups(backup_job.database.name)
                ssh.disconnect()
                
                # Find a backup name corresponding to this log time
                if backups:
                    backup_log_time = backup_log.start_time
                    for backup in backups:
                        timestamp_str = backup.get('info', {}).get('timestamp', '')
                        if timestamp_str:
                            try:
                                timestamp_parts = timestamp_str.split('-')
                                if len(timestamp_parts) >= 6:
                                    year = int(timestamp_parts[0])
                                    month = int(timestamp_parts[1])
                                    day = int(timestamp_parts[2])
                                    hour = int(timestamp_parts[3])
                                    minute = int(timestamp_parts[4])
                                    second = int(timestamp_parts[5].split('.')[0])
                                    
                                    backup_time = datetime(year, month, day, hour, minute, second)
                                    time_diff = abs((backup_time - backup_log_time).total_seconds())
                                    
                                    # If the backup time is close to the log time (within 60 seconds)
                                    if time_diff <= 60:
                                        backup_name = backup.get('name', '')
                                        break
                            except:
                                continue
        
        # If no specific backup was found but we have a job, use latest backup
        if not backup_name and backup_job:
            server = backup_job.server
            ssh = SSHManager(
                host=server.host,
                port=server.port,
                username=server.username,
                ssh_key_content=server.ssh_key_content
            )
            
            if ssh.connect():
                pg_manager = PostgresManager(ssh)
                backups = pg_manager.list_backups(backup_job.database.name)
                ssh.disconnect()
                
                if backups and len(backups) > 0:
                    # Use the most recent backup (typically the first one listed)
                    backup_name = backups[0].get('name', 'latest')
                    
                    # Find the corresponding backup log in our database
                    if not backup_log_id and backup_name != 'latest':
                        # Try to find a backup log that matches this backup time
                        try:
                            timestamp_str = backups[0].get('info', {}).get('timestamp', '')
                            if timestamp_str:
                                timestamp_parts = timestamp_str.split('-')
                                if len(timestamp_parts) >= 6:
                                    year = int(timestamp_parts[0])
                                    month = int(timestamp_parts[1])
                                    day = int(timestamp_parts[2])
                                    hour = int(timestamp_parts[3])
                                    minute = int(timestamp_parts[4])
                                    second = int(timestamp_parts[5].split('.')[0])
                                    
                                    backup_time = datetime(year, month, day, hour, minute, second)
                                    
                                    # Find backup logs within a 60-second window of this time
                                    potential_logs = BackupLog.query.filter(
                                        BackupLog.backup_job_id == backup_job.id,
                                        BackupLog.status == 'success',
                                        BackupLog.start_time >= backup_time - timedelta(seconds=60),
                                        BackupLog.start_time <= backup_time + timedelta(seconds=60)
                                    ).all()
                                    
                                    if potential_logs:
                                        # Use the closest match
                                        closest_log = min(potential_logs, 
                                            key=lambda log: abs((log.start_time - backup_time).total_seconds()))
                                        backup_log_id = closest_log.id
                                        print(f"Found matching backup log ID {backup_log_id} for latest backup")
                        except Exception as e:
                            print(f"Error finding matching backup log: {str(e)}")
                    
                    # If we still don't have a backup log ID, use the most recent successful backup log
                    if not backup_log_id and backup_name == 'latest':
                        most_recent_log = BackupLog.query.filter(
                            BackupLog.backup_job_id == backup_job.id,
                            BackupLog.status == 'success'
                        ).order_by(BackupLog.start_time.desc()).first()
                        
                        if most_recent_log:
                            backup_log_id = most_recent_log.id
                            print(f"Using most recent backup log ID {backup_log_id} for 'latest' backup")
                        else:
                            print("No successful backup logs found for this job")
        
        # Validate backup selection or recovery point
        if use_recovery_time:
            # For point-in-time recovery, recovery_time is required
            if not recovery_time:
                # If no recovery point is available but a backup is selected or found,
                # use the backup instead of requiring a recovery point
                if backup_name:
                    # Fall back to using the backup directly
                    use_recovery_time = False
                    recovery_time = None
                else:
                    flash('No recovery points available. Either select a backup or disable point-in-time recovery', 'danger')
                    return render_template('backups/restore.html', databases=databases, backup_jobs=backup_jobs)
        else:
            # For regular backup restore, ensure we have a backup_name
            if not backup_name:
                flash('No valid backup found to restore. Please check that backups exist for the selected job.', 'danger')
                return render_template('backups/restore.html', databases=databases, backup_jobs=backup_jobs)
            # If using backup, we don't need recovery_time
            recovery_time = None
        
        # Get server information
        server = database.server
        
        # Connect to server via SSH
        ssh = SSHManager(
            host=server.host,
            port=server.port,
            username=server.username,
            ssh_key_content=server.ssh_key_content
        )
        
        if not ssh.connect():
            flash('Failed to connect to server via SSH', 'danger')
            return render_template('backups/restore.html', databases=databases, backup_jobs=backup_jobs)
        
        # Create PostgreSQL manager
        pg_manager = PostgresManager(ssh)
        
        # Create restore log entry
        restore_log = RestoreLog(
            database_id=database_id,
            backup_log_id=backup_log_id if backup_log_id else None,
            restore_point=datetime.fromisoformat(recovery_time) if recovery_time and use_recovery_time else None,
            status='in_progress'
        )
        
        db.session.add(restore_log)
        db.session.commit()
        
        # Double-check that the backup_log relationship is established
        if backup_log_id:
            backup_log = BackupLog.query.get(backup_log_id)
            if backup_log:
                print(f"Backup log found: {backup_log.id}, job: {backup_log.backup_job.name if backup_log.backup_job else 'None'}")
            else:
                print(f"Warning: Backup log {backup_log_id} not found")
        
        try:
            # Execute restore
            success, log_output = pg_manager.restore_backup(
                database.name, backup_name, recovery_time
            )
            
            # Update restore log
            restore_log.status = 'success' if success else 'failed'
            restore_log.end_time = datetime.utcnow()
            restore_log.log_output = log_output
            
            # Add information about which backup was actually used
            if success and backup_name:
                if "restore backup set" in log_output:
                    # Extract the actual backup set name from the log output
                    import re
                    backup_set_match = re.search(r'restore backup set ([0-9\-A-Z]+)', log_output)
                    if backup_set_match:
                        actual_backup_name = backup_set_match.group(1)
                        print(f"Actual backup set used: {actual_backup_name}")
                        
                        # If we don't have a backup_log_id yet, try to find one matching this backup name
                        if not restore_log.backup_log_id:
                            # Parse the timestamp from the backup name
                            try:
                                timestamp_parts = actual_backup_name.split('-')
                                if len(timestamp_parts) >= 2:
                                    # Format is typically YYYYMMDD-HHMMSSF
                                    date_part = timestamp_parts[0]
                                    time_part = timestamp_parts[1][:-1]  # Remove the F or I suffix
                                    
                                    year = int(date_part[0:4])
                                    month = int(date_part[4:6])
                                    day = int(date_part[6:8])
                                    hour = int(time_part[0:2])
                                    minute = int(time_part[2:4])
                                    second = int(time_part[4:6])
                                    
                                    backup_time = datetime(year, month, day, hour, minute, second)
                                    
                                    # Find backup logs within a 60-second window of this time
                                    potential_logs = BackupLog.query.filter(
                                        BackupLog.backup_job_id == backup_job.id,
                                        BackupLog.status == 'success',
                                        BackupLog.start_time >= backup_time - timedelta(seconds=60),
                                        BackupLog.start_time <= backup_time + timedelta(seconds=60)
                                    ).all()
                                    
                                    if potential_logs:
                                        # Use the closest match
                                        closest_log = min(potential_logs, 
                                            key=lambda log: abs((log.start_time - backup_time).total_seconds()))
                                        restore_log.backup_log_id = closest_log.id
                                        print(f"Found matching backup log ID {restore_log.backup_log_id} for actual backup used")
                            except Exception as e:
                                print(f"Error finding matching backup log for actual backup: {str(e)}")
            
            db.session.commit()
            
            if success:
                flash('Restore completed successfully', 'success')
            else:
                flash(f'Restore failed: {log_output}', 'danger')
                
        except Exception as e:
            # Update restore log with error
            restore_log.status = 'failed'
            restore_log.end_time = datetime.utcnow()
            restore_log.log_output = str(e)
            
            db.session.commit()
            flash(f'Error during restore: {str(e)}', 'danger')
        finally:
            # Disconnect SSH
            ssh.disconnect()
        
        return redirect(url_for('backups.restore_logs'))
    
    return render_template('backups/restore.html', databases=databases, backup_jobs=backup_jobs)

@backups_bp.route('/restore/logs')
@login_required
@first_login_required
def restore_logs():
    # Get filter parameters
    database_id = request.args.get('database_id', type=int)
    status = request.args.get('status')
    days = request.args.get('days', '7')
    
    # Build query based on filters
    query = RestoreLog.query.join(RestoreLog.backup_log, isouter=True)
    
    # Filter by database
    if database_id:
        query = query.filter(RestoreLog.database_id == database_id)
    
    # Filter by status
    if status:
        query = query.filter(RestoreLog.status == status)
    
    # Filter by date
    if days and days != 'all':
        date_threshold = datetime.utcnow() - timedelta(days=int(days))
        query = query.filter(RestoreLog.start_time >= date_threshold)
    
    # Get all databases for the filter dropdown
    databases = PostgresDatabase.query.all()
    
    # Order by start time (most recent first)
    logs = query.order_by(RestoreLog.start_time.desc()).all()
    
    return render_template('backups/restore_logs.html', logs=logs, databases=databases, 
                          selected_database_id=database_id, selected_status=status, selected_days=days)

@backups_bp.route('/restore/logs/view/<int:id>')
@login_required
@first_login_required
def view_restore_log(id):
    # Use a join to ensure backup_log is loaded
    log = RestoreLog.query.options(
        db.joinedload(RestoreLog.backup_log).joinedload(BackupLog.backup_job)
    ).get_or_404(id)
    
    return render_template('backups/view_restore_log.html', log=log)

@backups_bp.route('/test-s3', methods=['POST'])
@login_required
@first_login_required
def test_s3():
    # Get parameters from the request
    bucket = request.json.get('bucket')
    region = request.json.get('region')
    access_key = request.json.get('access_key')
    secret_key = request.json.get('secret_key')
    
    # Validate parameters
    if not all([bucket, region, access_key, secret_key]):
        return jsonify({'success': False, 'message': 'Missing required parameters'})
    
    # Try to install AWS CLI if needed
    try:
        # Create a test file
        test_content = f"Test file for S3 connection to {bucket} - {datetime.utcnow().isoformat()}"
        test_file = f"/tmp/s3_test_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.txt"
        
        # Write test commands to shell script
        script_content = f"""#!/bin/bash
export AWS_ACCESS_KEY_ID="{access_key}"
export AWS_SECRET_ACCESS_KEY="{secret_key}"
export AWS_DEFAULT_REGION="{region}"

# Create test file
echo "{test_content}" > {test_file}

# Try to upload to S3
aws s3 cp {test_file} s3://{bucket}/test/ 2>&1

# Check result
if [ $? -eq 0 ]; then
  echo "S3 connection successful"
  exit 0
else
  echo "S3 connection failed"
  exit 1
fi
"""
        script_file = f"/tmp/s3_test_{datetime.utcnow().strftime('%Y%m%d%H%M%S')}.sh"
        
        with open(script_file, 'w') as f:
            f.write(script_content)
        
        # Make script executable
        os.chmod(script_file, 0o755)
        
        # Execute script
        result = os.popen(script_file).read()
        
        # Clean up
        os.remove(script_file)
        if os.path.exists(test_file):
            os.remove(test_file)
        
        # Check result
        if "S3 connection successful" in result:
            return jsonify({'success': True, 'message': 'S3 connection successful'})
        else:
            return jsonify({'success': False, 'message': f'S3 connection failed: {result}'})
        
    except Exception as e:
        return jsonify({'success': False, 'message': f'Error testing S3 connection: {str(e)}'})

@backups_bp.route('/fix-archive-command/<int:id>', methods=['POST'])
@login_required
@first_login_required
def fix_archive_command(id):
    backup_job = BackupJob.query.get_or_404(id)
    server = backup_job.server
    database = backup_job.database
    
    try:
        # Connect to server via SSH
        ssh, pg_manager = get_managers(server)
        if not ssh or not pg_manager:
            flash('Failed to connect to server via SSH', 'danger')
            return redirect(url_for('backups.index'))
        
        # Find PostgreSQL configuration file
        find_conf_cmd = ssh.execute_command("sudo find /etc/postgresql -name postgresql.conf | head -1")
        if find_conf_cmd['exit_code'] != 0 or not find_conf_cmd['stdout'].strip():
            flash('Could not locate PostgreSQL configuration file', 'danger')
            ssh.disconnect()
            return redirect(url_for('backups.index'))
            
        pg_conf_file = find_conf_cmd['stdout'].strip()
        flash(f'Found PostgreSQL configuration at: {pg_conf_file}', 'info')
            
        # Backup original file
        ssh.execute_command(f"sudo cp {pg_conf_file} {pg_conf_file}.bak.$(date +%Y%m%d%H%M%S)")
        
        # Get data directory
        data_dir = pg_manager.get_data_directory()
        if not data_dir:
            data_dir = '/var/lib/postgresql/17/main'  # Default for PostgreSQL 17
            flash(f'Using default data directory: {data_dir}', 'warning')
        else:
            flash(f'Found data directory: {data_dir}', 'info')
            
        # Create completely new archive settings with careful quoting and escaping
        # Note: The %p and %f are PostgreSQL placeholders, not Python formatting
        archive_settings = f"""
#------------------------------------------------------------------------------
# WRITE-AHEAD LOG (WAL) ARCHIVING
#------------------------------------------------------------------------------

archive_mode = on                # enables archiving; off, on, or always
archive_command = 'pgbackrest --stanza={database.name} archive-push %p'
                                # command to use to archive a WAL file
wal_level = replica              # minimal, replica, or logical
max_wal_senders = 10             # max number of walsender processes
"""
        
        # Write settings to temporary file
        temp_file = "/tmp/archive_settings.conf"
        ssh.execute_command(f"echo '{archive_settings}' | sudo tee {temp_file} > /dev/null")
        
        # Read current config file content
        read_conf = ssh.execute_command(f"sudo cat {pg_conf_file}")
        
        # Check if archive settings already exist and remove them
        if "archive_mode" in read_conf['stdout']:
            # Create a new config file without existing archive settings
            ssh.execute_command(f"""sudo grep -v "^[ \\t]*archive_mode\\|^[ \\t]*archive_command\\|^[ \\t]*wal_level\\|^[ \\t]*max_wal_senders" {pg_conf_file} > /tmp/pg_conf_clean""")
            ssh.execute_command(f"sudo cp /tmp/pg_conf_clean {pg_conf_file}")
            flash('Removed existing archive settings from PostgreSQL config', 'info')
            
        # Append new settings
        ssh.execute_command(f"sudo bash -c 'cat {temp_file} >> {pg_conf_file}'")
        flash('Added clean archive settings to PostgreSQL config', 'info')
        
        # Set correct permissions
        ssh.execute_command(f"sudo chown postgres:postgres {pg_conf_file}")
        ssh.execute_command(f"sudo chmod 600 {pg_conf_file}")
        
        # Create pgbackrest directories and config
        ssh.execute_command("sudo mkdir -p /etc/pgbackrest/conf.d")
        ssh.execute_command("sudo mkdir -p /var/lib/pgbackrest")
        ssh.execute_command("sudo chown -R postgres:postgres /etc/pgbackrest /var/lib/pgbackrest")
        
        # Create pgbackrest stanza config
        stanza_conf = f"""[global]
repo1-path=/var/lib/pgbackrest
repo1-retention-full=7

[{database.name}]
pg1-path={data_dir}
"""

        stanza_file = f"/etc/pgbackrest/conf.d/{database.name}.conf"
        ssh.execute_command(f"echo '{stanza_conf}' | sudo tee {stanza_file} > /dev/null")
        ssh.execute_command(f"sudo chown postgres:postgres {stanza_file}")
        ssh.execute_command(f"sudo chmod 600 {stanza_file}")
        
        # Try different methods to stop PostgreSQL - with output capture for diagnostics
        stop_result = ssh.execute_command("sudo systemctl stop postgresql || sudo systemctl stop postgres")
        if stop_result['exit_code'] != 0:
            flash(f'Warning: Issue stopping PostgreSQL: {stop_result["stderr"]}', 'warning')
            # Try more forceful stop
            ssh.execute_command("sudo pkill -9 postgres || true")
            time.sleep(2)
            
        # Validate configuration before restart
        pg_ctl_check = ssh.execute_command(f"sudo -u postgres pg_ctl -D {data_dir} check || echo 'Config check not supported'")
        if "failed" in pg_ctl_check['stderr'].lower():
            flash(f'PostgreSQL config validation failed: {pg_ctl_check["stderr"]}', 'warning')
            
        # Try multiple methods to start PostgreSQL
        flash('Attempting to start PostgreSQL...', 'info')
        start_methods = [
            f"sudo systemctl start postgresql",
            f"sudo systemctl start postgres",
            f"sudo -u postgres pg_ctl -D {data_dir} start -l /tmp/pg_start.log",
            f"sudo service postgresql start"
        ]
        
        pg_started = False
        for method in start_methods:
            start_result = ssh.execute_command(method)
            # Check if PostgreSQL is running after this attempt
            is_running = "active (running)" in ssh.execute_command("sudo systemctl status postgresql || sudo systemctl status postgres")['stdout']
            if is_running:
                flash(f'PostgreSQL successfully started using: {method}', 'success')
                pg_started = True
                break
            else:
                # Check log for errors
                log_check = ssh.execute_command("sudo tail -20 /var/log/postgresql/postgresql*.log")
                if log_check['exit_code'] == 0 and log_check['stdout'].strip():
                    flash(f'PostgreSQL start logs: {log_check["stdout"][:200]}', 'info')
        
        # Final check if PostgreSQL is running
        final_check = ssh.execute_command("sudo systemctl status postgresql || sudo systemctl status postgres")
        is_running = "active (running)" in final_check['stdout']
        
        if is_running:
            # Initialize pgBackRest stanza
            ssh.execute_command(f"sudo -u postgres pgbackrest --stanza={database.name} stanza-create --force")
            
            # Verify archive_mode and archive_command
            archive_mode = ssh.execute_command("sudo -u postgres psql -t -c 'SHOW archive_mode;'")
            archive_command = ssh.execute_command("sudo -u postgres psql -t -c 'SHOW archive_command;'")
            
            if "on" in archive_mode['stdout'].lower() and "pgbackrest" in archive_command['stdout'].lower():
                flash('SUCCESS: PostgreSQL is now running with proper archive settings!', 'success')
            else:
                flash('PostgreSQL is running, but archive settings may not be correct.', 'warning')
        else:
            flash(f'PostgreSQL failed to start. Check logs using: sudo tail -50 /var/log/postgresql/postgresql*.log', 'danger')
        
        # Disconnect SSH
        ssh.disconnect()
        
    except Exception as e:
        flash(f'Error fixing archive command: {str(e)}', 'danger')
        
    return redirect(url_for('backups.index'))

@backups_bp.route('/apply-retention/<int:id>', methods=['POST'])
@login_required
@first_login_required
def apply_retention(id):
    """Manually apply the retention policy to delete old backups"""
    backup_job = BackupJob.query.get_or_404(id)
    database = backup_job.database
    
    try:
        # Connect to server
        ssh, pg_manager = get_managers(database.server)
        
        if not ssh or not pg_manager:
            flash('Unable to connect to server', 'danger')
            return redirect(url_for('backups.index'))
        
        # Apply the retention policy
        success, message = pg_manager.cleanup_old_backups(database.name, backup_job.retention_count)
        
        if success:
            flash(f'Retention policy applied: {message}', 'success')
        else:
            flash(f'Failed to apply retention policy: {message}', 'danger')
        
        # Clean up
        ssh.disconnect()
    except Exception as e:
        flash(f'Error applying retention policy: {str(e)}', 'danger')
    
    return redirect(url_for('backups.index'))

@backups_bp.route('/api/logs')
@login_required
@first_login_required
def api_logs():
    job_id = request.args.get('job_id', type=int)
    if not job_id:
        return jsonify({'success': False, 'message': 'No job ID provided'})
        
    logs = BackupLog.query.filter_by(
        backup_job_id=job_id, 
        status='success'
    ).order_by(BackupLog.start_time.desc()).all()
    
    logs_data = []
    for log in logs:
        size_mb = None
        if log.size_bytes:
            size_mb = round(log.size_bytes / (1024 * 1024), 2)
            
        logs_data.append({
            'id': log.id,
            'start_time': log.start_time.strftime('%Y-%m-%d %H:%M'),
            'backup_type': log.backup_type,
            'size_mb': size_mb
        })
    
    return jsonify({
        'success': True, 
        'logs': logs_data
    })

@backups_bp.route('/api/recovery-points')
@login_required
@first_login_required
def api_recovery_points():
    job_id = request.args.get('job_id', type=int)
    if not job_id:
        return jsonify({'success': False, 'message': 'No job ID provided'})
        
    # Get the backup job
    backup_job = BackupJob.query.get(job_id)
    if not backup_job:
        return jsonify({'success': False, 'message': 'Backup job not found'})
    
    database = backup_job.database
    server = backup_job.server
    
    # Connect to server via SSH
    try:
        ssh = SSHManager(
            host=server.host,
            port=server.port,
            username=server.username,
            ssh_key_content=server.ssh_key_content
        )
        
        if not ssh.connect():
            return jsonify({'success': False, 'message': 'Failed to connect to server via SSH'})
        
        # Create PostgreSQL manager
        pg_manager = PostgresManager(ssh)
        
        # Get available backups to determine recovery points
        backups = pg_manager.list_backups(database.name)
        
        if not backups:
            ssh.disconnect()
            return jsonify({'success': False, 'message': 'No backups found for this database'})
        
        # Extract recovery points from backups
        recovery_points = []
        for backup in backups:
            # Get timestamp from backup info
            timestamp_str = backup.get('info', {}).get('timestamp', '')
            if timestamp_str:
                try:
                    # Convert timestamp to datetime and format
                    timestamp_parts = timestamp_str.split('-')
                    if len(timestamp_parts) >= 6:
                        year = int(timestamp_parts[0])
                        month = int(timestamp_parts[1])
                        day = int(timestamp_parts[2])
                        hour = int(timestamp_parts[3])
                        minute = int(timestamp_parts[4])
                        second = int(timestamp_parts[5].split('.')[0])
                        
                        dt = datetime(year, month, day, hour, minute, second)
                        recovery_points.append({
                            'datetime': dt.strftime('%Y-%m-%dT%H:%M:%S'),
                            'formatted': dt.strftime('%Y-%m-%d %H:%M:%S'),
                            'backup_name': backup.get('name', '')
                        })
                except (ValueError, IndexError) as e:
                    pass  # Skip invalid timestamps
        
        ssh.disconnect()
        
        # Sort recovery points by datetime (newest first)
        recovery_points.sort(key=lambda x: x['datetime'], reverse=True)
        
        return jsonify({
            'success': True,
            'recovery_points': recovery_points
        })
        
    except Exception as e:
        if 'ssh' in locals() and ssh:
            ssh.disconnect()
        return jsonify({
            'success': False,
            'message': f'Error retrieving recovery points: {str(e)}'
        })

@backups_bp.route('/debug/restore-log/<int:id>')
@login_required
@first_login_required
def debug_restore_log(id):
    """Debug route to check restore log relationships"""
    restore_log = RestoreLog.query.get_or_404(id)
    
    # Get the backup log directly
    backup_log = None
    if restore_log.backup_log_id:
        backup_log = BackupLog.query.get(restore_log.backup_log_id)
    
    # Build debug info
    debug_info = {
        "restore_log_id": restore_log.id,
        "backup_log_id": restore_log.backup_log_id,
        "backup_log_exists": backup_log is not None,
        "backup_job_id": backup_log.backup_job_id if backup_log else None,
        "backup_job_name": backup_log.backup_job.name if backup_log and hasattr(backup_log, 'backup_job') else None
    }
    
    return jsonify(debug_info) 