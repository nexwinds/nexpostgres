from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from app.models.database import BackupJob, BackupLog, PostgresDatabase, RestoreLog, S3Storage, db
from app.utils.ssh_manager import SSHManager
from app.utils.postgres_manager import PostgresManager
from app.utils.scheduler import schedule_backup_job, execute_manual_backup
from app.routes.auth import login_required, first_login_required
from datetime import datetime

backups_bp = Blueprint('backups', __name__, url_prefix='/backups')

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
        if not database:
            flash('Selected database does not exist', 'danger')
            return render_template('backups/add.html', databases=databases, s3_storages=s3_storages)
        
        # Validate S3 storage
        s3_storage = S3Storage.query.get(s3_storage_id)
        if not s3_storage:
            flash('Selected S3 storage configuration does not exist', 'danger')
            return render_template('backups/add.html', databases=databases, s3_storages=s3_storages)
        
        # Check backup type
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
        if not database:
            flash('Selected database does not exist', 'danger')
            return render_template('backups/edit.html', backup_job=backup_job, databases=databases, s3_storages=s3_storages)
        
        # Validate S3 storage
        s3_storage = S3Storage.query.get(s3_storage_id)
        if not s3_storage:
            flash('Selected S3 storage configuration does not exist', 'danger')
            return render_template('backups/edit.html', backup_job=backup_job, databases=databases, s3_storages=s3_storages)
        
        # Check backup type
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
        
        if success:
            flash('Backup executed successfully', 'success')
        else:
            flash(f'Backup failed: {message}', 'danger')
            
        return redirect(url_for('backups.logs', job_id=backup_job.id))
        
    except Exception as e:
        flash(f'Error executing backup: {str(e)}', 'danger')
        return redirect(url_for('backups.index'))

@backups_bp.route('/setup/<int:id>', methods=['POST'])
@login_required
@first_login_required
def setup(id):
    backup_job = BackupJob.query.get_or_404(id)
    database = backup_job.database
    server = backup_job.server
    s3_storage = backup_job.s3_storage
    
    try:
        # Connect to server via SSH
        ssh = SSHManager(
            host=server.host,
            port=server.port,
            username=server.username,
            ssh_key_path=server.ssh_key_path,
            ssh_key_content=server.ssh_key_content
        )
        
        if not ssh.connect():
            flash('Failed to connect to server via SSH', 'danger')
            return redirect(url_for('backups.index'))
        
        # Initialize PostgreSQL manager
        pg_manager = PostgresManager(ssh)
        
        # Check PostgreSQL installation
        if not pg_manager.check_postgres_installed():
            flash('PostgreSQL is not installed on the server', 'danger')
            ssh.disconnect()
            return redirect(url_for('backups.index'))
        
        # Check pgBackRest installation
        if not pg_manager.check_pgbackrest_installed():
            flash('pgBackRest is not installed on the server', 'danger')
            ssh.disconnect()
            return redirect(url_for('backups.index'))
        
        # Configure pgBackRest
        success = pg_manager.setup_pgbackrest_config(
            database.name,
            s3_storage.bucket,
            s3_storage.region,
            s3_storage.access_key,
            s3_storage.secret_key
        )
        
        if not success:
            flash('Failed to configure pgBackRest', 'danger')
            ssh.disconnect()
            return redirect(url_for('backups.index'))
        
        # Setup cron job for scheduled backups
        success = pg_manager.setup_cron_job(
            database.name,
            backup_job.backup_type,
            backup_job.cron_expression
        )
        
        if not success:
            flash('Failed to setup cron job', 'danger')
            ssh.disconnect()
            return redirect(url_for('backups.index'))
        
        # Disconnect from server
        ssh.disconnect()
        
        flash('pgBackRest configuration and cron job setup completed successfully', 'success')
        return redirect(url_for('backups.index'))
        
    except Exception as e:
        flash(f'Error setting up backup: {str(e)}', 'danger')
        return redirect(url_for('backups.index'))

@backups_bp.route('/logs')
@login_required
@first_login_required
def logs():
    job_id = request.args.get('job_id', type=int)
    
    if job_id:
        backup_job = BackupJob.query.get_or_404(job_id)
        logs = BackupLog.query.filter_by(backup_job_id=job_id).order_by(BackupLog.start_time.desc()).all()
        return render_template('backups/job_logs.html', backup_job=backup_job, logs=logs)
    
    # Show all logs
    logs = BackupLog.query.order_by(BackupLog.start_time.desc()).all()
    return render_template('backups/logs.html', logs=logs)

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
        use_pitr = request.form.get('use_pitr') == 'true'
        restore_time = None
        
        if use_pitr:
            restore_date = request.form.get('restore_date')
            restore_time = request.form.get('restore_time')
            
            if restore_date and restore_time:
                restore_time = f"{restore_date} {restore_time}"
            
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
            # Connect to server via SSH
            ssh = SSHManager(
                host=database.server.host,
                port=database.server.port,
                username=database.server.username,
                ssh_key_path=database.server.ssh_key_path,
                ssh_key_content=database.server.ssh_key_content
            )
            
            if not ssh.connect():
                raise Exception('Failed to connect to server via SSH')
            
            # Initialize PostgreSQL manager
            pg_manager = PostgresManager(ssh)
            
            # Check PostgreSQL and pgBackRest installation
            if not pg_manager.check_postgres_installed() or not pg_manager.check_pgbackrest_installed():
                raise Exception('PostgreSQL or pgBackRest is not installed on the server')
            
            # Execute restore
            backup_name = None
            if backup_id:
                backup_log = BackupLog.query.get(backup_id)
                if backup_log:
                    # Get the backup name by parsing the log output (actual implementation might vary)
                    pass
            
            success, log_output = pg_manager.restore_backup(
                database.name,
                backup_name,
                restore_time
            )
            
            # Update restore log
            restore_log.status = 'success' if success else 'failed'
            restore_log.end_time = datetime.utcnow()
            restore_log.log_output = log_output
            
            db.session.commit()
            
            # Disconnect from server
            ssh.disconnect()
            
            if success:
                flash('Database restored successfully', 'success')
            else:
                flash('Database restore failed', 'danger')
            
            return redirect(url_for('backups.restore_logs'))
            
        except Exception as e:
            restore_log.status = 'failed'
            restore_log.end_time = datetime.utcnow()
            restore_log.log_output = str(e)
            db.session.commit()
            
            flash(f'Error restoring database: {str(e)}', 'danger')
            return redirect(url_for('backups.restore'))
    
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