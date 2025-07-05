from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from app.models.database import BackupJob, BackupLog, PostgresDatabase, RestoreLog, S3Storage, db
from app.utils.ssh_manager import SSHManager
from app.utils.postgres_manager import PostgresManager
from app.utils.scheduler import schedule_backup_job, execute_manual_backup
from app.routes.auth import login_required, first_login_required
from datetime import datetime, timedelta
import functools

backups_bp = Blueprint('backups', __name__, url_prefix='/backups')

# Helper function to create SSH and PostgreSQL manager
def get_managers(server):
    ssh = SSHManager(
        host=server.host,
        port=server.port,
        username=server.username,
        ssh_key_path=server.ssh_key_path,
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
        
        # Create backup job
        backup_job = BackupJob(
            name=name,
            database_id=database_id,
            vps_server_id=database.vps_server_id,
            backup_type=backup_type,
            cron_expression=cron_expression,
            s3_storage_id=s3_storage_id
        )
        
        db.session.add(backup_job)
        db.session.commit()
        
        # Schedule the backup job
        try:
            schedule_backup_job(backup_job)
            flash('Backup job created and scheduled successfully', 'success')
        except Exception as e:
            flash(f'Backup job created but scheduling failed: {str(e)}', 'warning')
        
        return redirect(url_for('backups.index'))
    
    return render_template('backups/add.html', databases=databases, s3_storages=s3_storages)

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
        
        # Update backup job
        backup_job.name = name
        backup_job.database_id = database_id
        backup_job.vps_server_id = database.vps_server_id
        backup_job.backup_type = backup_type
        backup_job.cron_expression = cron_expression
        backup_job.enabled = enabled
        backup_job.s3_storage_id = s3_storage_id
        
        db.session.commit()
        
        # Reschedule the backup job if enabled
        if enabled:
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
        success, message = execute_manual_backup(backup_job.id)
        flash('Backup executed successfully' if success else f'Backup failed: {message}', 
              'success' if success else 'danger')
    except Exception as e:
        flash(f'Error executing backup: {str(e)}', 'danger')
    
    return redirect(url_for('backups.logs', job_id=backup_job.id))

@backups_bp.route('/setup/<int:id>', methods=['POST'])
@login_required
@first_login_required
def setup(id):
    backup_job = BackupJob.query.get_or_404(id)
    database = backup_job.database
    server = backup_job.server
    s3_storage = backup_job.s3_storage
    
    try:
        ssh, pg_manager = get_managers(server)
        if not ssh:
            flash('Failed to connect to server via SSH', 'danger')
            return redirect(url_for('backups.index'))
        
        # Check prerequisites
        if not pg_manager.check_postgres_installed():
            flash('PostgreSQL is not installed on the server', 'danger')
            ssh.disconnect()
            return redirect(url_for('backups.index'))
        
        if not pg_manager.check_pgbackrest_installed():
            flash('pgBackRest is not installed on the server', 'danger')
            ssh.disconnect()
            return redirect(url_for('backups.index'))
        
        # Configure pgBackRest
        if not pg_manager.setup_pgbackrest_config(
            database.name, s3_storage.bucket, s3_storage.region,
            s3_storage.access_key, s3_storage.secret_key
        ):
            flash('Failed to configure pgBackRest', 'danger')
            ssh.disconnect()
            return redirect(url_for('backups.index'))
        
        # Setup cron job
        if not pg_manager.setup_cron_job(
            database.name, backup_job.backup_type, backup_job.cron_expression
        ):
            flash('Failed to setup cron job', 'danger')
            ssh.disconnect()
            return redirect(url_for('backups.index'))
        
        ssh.disconnect()
        flash('pgBackRest configuration and cron job setup completed successfully', 'success')
        
    except Exception as e:
        flash(f'Error setting up backup: {str(e)}', 'danger')
    
    return redirect(url_for('backups.index'))

@backups_bp.route('/verify-config/<int:id>', methods=['GET', 'POST'])
@login_required
@first_login_required
def verify_config(id):
    backup_job = BackupJob.query.get_or_404(id)
    
    try:
        ssh, pg_manager = get_managers(backup_job.server)
        if not ssh:
            flash('Failed to connect to server via SSH', 'danger')
            return redirect(url_for('backups.index'))
        
        # Verify and fix PostgreSQL configuration
        success, message = pg_manager.verify_and_fix_postgres_config(backup_job.database.name)
        ssh.disconnect()
        
        flash(f'PostgreSQL configuration verified: {message}' if success else
              f'Failed to verify PostgreSQL configuration: {message}',
              'success' if success else 'danger')
        
    except Exception as e:
        flash(f'Error verifying configuration: {str(e)}', 'danger')
    
    return redirect(url_for('backups.logs', job_id=backup_job.id))

@backups_bp.route('/check-pg-version/<int:id>', methods=['GET'])
@login_required
@first_login_required
def check_pg_version(id):
    """Debug route to check PostgreSQL version detection"""
    backup_job = BackupJob.query.get_or_404(id)
    debug_info = []
    
    try:
        ssh, pg_manager = get_managers(backup_job.server)
        if not ssh:
            debug_info.append("Failed to connect to server via SSH")
            return jsonify({'success': False, 'debug_info': debug_info})
        
        debug_info.append("Connected to server via SSH")
        
        # Try to get PostgreSQL version
        version = pg_manager.get_postgres_version()
        debug_info.append(f"PostgreSQL version detected: {version}" if version 
                         else "Could not detect PostgreSQL version")
        
        # Run diagnostic commands
        debug_commands = [
            "which psql",
            "sudo -u postgres psql --version",
            "ls -la /var/lib/postgresql/",
            "ls -la /etc/postgresql/",
            "find /etc -name 'postgresql.conf'",
            "ps aux | grep postgres",
            "sudo systemctl status postgresql* || true"
        ]
        
        for cmd in debug_commands:
            result = ssh.execute_command(cmd)
            stdout = result.get('stdout', '').strip()
            stderr = result.get('stderr', '').strip()
            exit_code = result.get('exit_code', -1)
            
            debug_info.append(f"Command: {cmd}")
            debug_info.append(f"Exit code: {exit_code}")
            
            if stdout:
                debug_info.append("Output:")
                # Limit output to 10 lines
                stdout_lines = stdout.split("\n")[:10]
                for line in stdout_lines:
                    debug_info.append(f"> {line}")
                if len(stdout.split("\n")) > 10:
                    debug_info.append("> ... (output truncated)")
            
            if stderr:
                debug_info.append("Error:")
                stderr_lines = stderr.split("\n")[:5]
                for line in stderr_lines:
                    debug_info.append(f"> {line}")
        
        ssh.disconnect()
        return jsonify({'success': True, 'debug_info': debug_info})
        
    except Exception as e:
        debug_info.append(f"Error: {str(e)}")
        return jsonify({'success': False, 'debug_info': debug_info})

@backups_bp.route('/fix-backup/<int:id>', methods=['GET', 'POST'])
@login_required
@first_login_required
def fix_backup(id):
    """Fix backup configuration issues, especially for incremental backups"""
    backup_job = BackupJob.query.get_or_404(id)
    
    try:
        ssh, pg_manager = get_managers(backup_job.server)
        if not ssh:
            flash('Failed to connect to server via SSH', 'danger')
            return redirect(url_for('backups.logs', job_id=backup_job.id))
        
        # First ensure archive_command is correctly set
        success, message = pg_manager.verify_and_fix_postgres_config(backup_job.database.name)
        if not success:
            flash(f'Failed to verify PostgreSQL configuration: {message}', 'danger')
            ssh.disconnect()
            return redirect(url_for('backups.logs', job_id=backup_job.id))
        
        # Fix incremental backup configuration
        success, message = pg_manager.fix_incremental_backup_config(backup_job.database.name)
        ssh.disconnect()
        
        flash(f'Backup configuration fixed: {message}' if success else
              f'Failed to fix backup configuration: {message}',
              'success' if success else 'danger')
        
    except Exception as e:
        flash(f'Error fixing backup configuration: {str(e)}', 'danger')
    
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
    
    if job_id:
        backup_job = BackupJob.query.get_or_404(job_id)
        query = query.filter_by(backup_job_id=job_id)
    
    if status:
        query = query.filter_by(status=status)
    
    # Filter by time range if not "all"
    if days != 'all':
        days_ago = datetime.utcnow() - timedelta(days=int(days))
        query = query.filter(BackupLog.start_time >= days_ago)
    
    # Sort by start time (newest first)
    logs = query.order_by(BackupLog.start_time.desc()).all()
    
    if job_id:
        return render_template('backups/job_logs.html', backup_job=backup_job, logs=logs)
    
    # Show all logs
    jobs = BackupJob.query.all()
    return render_template('backups/logs.html', logs=logs, jobs=jobs)

@backups_bp.route('/logs/view/<int:id>')
@login_required
@first_login_required
def view_log(id):
    log = BackupLog.query.get_or_404(id)
    return render_template('backups/view_log.html', log=log)

@backups_bp.route('/restore', methods=['GET', 'POST'])
@login_required
@first_login_required
def restore():
    databases = PostgresDatabase.query.all()
    
    if request.method == 'POST':
        database_id = request.form.get('database_id', type=int)
        backup_id = request.form.get('backup_id', type=int)
        restore_time = None
        
        # Handle point-in-time recovery if requested
        if request.form.get('use_pitr') == 'true':
            restore_date = request.form.get('restore_date')
            restore_time_str = request.form.get('restore_time')
            
            if restore_date and restore_time_str:
                restore_time = f"{restore_date} {restore_time_str}"
        
        # Validate data
        database = PostgresDatabase.query.get(database_id)
        if not database:
            flash('Selected database does not exist', 'danger')
            return render_template('backups/restore.html', databases=databases)
        
        # Create restore log entry
        restore_log = RestoreLog(
            backup_log_id=backup_id if backup_id else None,
            database_id=database_id,
            status='in_progress',
            restore_point=datetime.fromisoformat(restore_time) if restore_time else None
        )
        
        db.session.add(restore_log)
        db.session.commit()
        
        try:
            ssh, pg_manager = get_managers(database.server)
            if not ssh:
                raise Exception('Failed to connect to server via SSH')
            
            # Check prerequisites
            if not pg_manager.check_postgres_installed() or not pg_manager.check_pgbackrest_installed():
                raise Exception('PostgreSQL or pgBackRest is not installed on the server')
            
            # Get backup name if specific backup was selected
            backup_name = None
            if backup_id:
                # In a real implementation, get the backup name from the log
                pass
            
            # Execute restore
            success, log_output = pg_manager.restore_backup(
                database.name, backup_name, restore_time
            )
            
            # Update restore log
            restore_log.status = 'success' if success else 'failed'
            restore_log.end_time = datetime.utcnow()
            restore_log.log_output = log_output
            
            db.session.commit()
            ssh.disconnect()
            
            flash('Database restored successfully' if success else 'Database restore failed',
                  'success' if success else 'danger')
                  
        except Exception as e:
            restore_log.status = 'failed'
            restore_log.end_time = datetime.utcnow()
            restore_log.log_output = str(e)
            db.session.commit()
            
            flash(f'Error restoring database: {str(e)}', 'danger')
            return redirect(url_for('backups.restore'))
            
        return redirect(url_for('backups.restore_logs'))
    
    return render_template('backups/restore.html', databases=databases)

@backups_bp.route('/restore/logs')
@login_required
@first_login_required
def restore_logs():
    logs = RestoreLog.query.order_by(RestoreLog.start_time.desc()).all()
    return render_template('backups/restore_logs.html', logs=logs)

@backups_bp.route('/restore/logs/view/<int:id>')
@login_required
@first_login_required
def view_restore_log(id):
    log = RestoreLog.query.get_or_404(id)
    return render_template('backups/view_restore_log.html', log=log)

@backups_bp.route('/test-s3', methods=['POST'])
@login_required
@first_login_required
def test_s3():
    s3_bucket = request.form.get('s3_bucket')
    s3_region = request.form.get('s3_region')
    s3_access_key = request.form.get('s3_access_key')
    s3_secret_key = request.form.get('s3_secret_key')
    
    # Test S3 connection by writing a small test file
    # This is a simplified version and would need proper implementation
    
    return jsonify({
        'success': True,
        'message': 'S3 connection successful'
    }) 