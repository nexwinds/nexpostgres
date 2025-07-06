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
    if days != 'all':
        try:
            days_ago = datetime.utcnow() - timedelta(days=int(days))
            query = query.filter(BackupLog.start_time >= days_ago)
        except ValueError:
            pass
    
    # Get logs with pagination
    logs = query.order_by(BackupLog.start_time.desc()).all()
    
    if backup_job:
        return render_template('backups/job_logs.html', logs=logs, backup_job=backup_job)
    else:
        return render_template('backups/job_logs.html', logs=logs, backup_job=None, backup_jobs=backup_jobs)

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
        backup_name = request.form.get('backup_name')
        restore_time = request.form.get('restore_time')
        
        # Validate data
        database = PostgresDatabase.query.get(database_id)
        
        if not database:
            flash('Selected database does not exist', 'danger')
            return render_template('backups/restore.html', databases=databases)
        
        # Ensure either backup name or restore time is provided
        if not backup_name and not restore_time:
            flash('Please provide either a backup name or a restore time', 'danger')
            return render_template('backups/restore.html', databases=databases)
        
        # Get server information
        server = database.server
        
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
            return render_template('backups/restore.html', databases=databases)
        
        # Create PostgreSQL manager
        pg_manager = PostgresManager(ssh)
        
        # Create restore log entry
        restore_log = RestoreLog(
            database_id=database_id,
            backup_name=backup_name,
            restore_time=restore_time,
            status='in_progress'
        )
        
        db.session.add(restore_log)
        db.session.commit()
        
        try:
            # Execute restore
            success, log_output = pg_manager.restore_backup(
                database.name, backup_name, restore_time
            )
            
            # Update restore log
            restore_log.status = 'success' if success else 'failed'
            restore_log.end_time = datetime.utcnow()
            restore_log.log_output = log_output
            
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

@backups_bp.route('/check/<int:id>')
@login_required
@first_login_required
def check(id):
    backup_job = BackupJob.query.get_or_404(id)
    server = backup_job.server
    database = backup_job.database
    
    try:
        # Connect to server via SSH
        ssh, pg_manager = get_managers(server)
        if not ssh or not pg_manager:
            return jsonify({
                'success': False,
                'message': 'Failed to connect to server via SSH'
            })
        
        # Run detailed health checks
        health_details = {}
        issues = []
        
        # Check if pgBackRest is installed
        pgbackrest_installed = ssh.execute_command("which pgbackrest")
        health_details['pgbackrest_installed'] = pgbackrest_installed['exit_code'] == 0
        
        if not health_details['pgbackrest_installed']:
            issues.append("pgBackRest is not installed")
        
        # Check if PostgreSQL is running
        pg_running = ssh.execute_command("ps aux | grep postgres | grep -v grep")
        health_details['postgres_running'] = pg_running['exit_code'] == 0 and pg_running['stdout'].strip()
        
        if not health_details['postgres_running']:
            issues.append("PostgreSQL is not running")
            
            # Check for potential issues preventing start
            stale_check = ssh.execute_command("sudo find /var/run/postgresql -name '*.lock' | grep -c lock")
            if stale_check['exit_code'] == 0 and int(stale_check['stdout'].strip() or '0') > 0:
                issues.append("Stale lock files found in /var/run/postgresql")
        
        # Find PostgreSQL configuration
        pg_conf = pg_manager._find_postgresql_conf()
        health_details['pg_config_file'] = pg_conf
        
        if pg_conf:
            # Check archive settings
            archive_settings = ssh.execute_command(f"sudo grep -E 'archive_mode|archive_command' {pg_conf}")
            
            # Parse archive mode
            archive_mode_on = False
            if archive_settings['exit_code'] == 0:
                if re.search(r'archive_mode\s*=\s*on', archive_settings['stdout'], re.IGNORECASE):
                    archive_mode_on = True
                    health_details['archive_mode'] = 'on'
                else:
                    health_details['archive_mode'] = 'off or not set'
                    issues.append("archive_mode is not set to 'on'")
            else:
                health_details['archive_mode'] = 'not configured'
                issues.append("archive_mode is not configured")
            
            # Parse archive command
            archive_cmd_correct = False
            if archive_settings['exit_code'] == 0:
                cmd_match = re.search(r"archive_command\s*=\s*['\"](.*?)['\"]", archive_settings['stdout'])
                if cmd_match:
                    cmd = cmd_match.group(1)
                    health_details['archive_command'] = cmd
                    if 'pgbackrest' in cmd and 'archive-push' in cmd:
                        archive_cmd_correct = True
                    else:
                        issues.append("archive_command is not properly configured for pgBackRest")
                else:
                    health_details['archive_command'] = 'not set'
                    issues.append("archive_command is not configured")
            else:
                health_details['archive_command'] = 'not configured'
                issues.append("archive_command is not configured")
        else:
            issues.append("Could not locate PostgreSQL configuration file")
        
        # Check each pgBackRest configuration file separately for better diagnosis
        main_conf = ssh.execute_command("sudo test -f /etc/pgbackrest/pgbackrest.conf && echo 'exists'")
        health_details['pgbackrest_main_conf_exists'] = main_conf['exit_code'] == 0 and 'exists' in main_conf['stdout']
        
        if not health_details['pgbackrest_main_conf_exists']:
            issues.append("pgBackRest main configuration file not found")
        
        stanza_conf = ssh.execute_command(f"sudo test -d /etc/pgbackrest/conf.d && sudo test -f /etc/pgbackrest/conf.d/{database.name}.conf && echo 'exists'")
        health_details['pgbackrest_stanza_conf_exists'] = stanza_conf['exit_code'] == 0 and 'exists' in stanza_conf['stdout']
        
        if not health_details['pgbackrest_stanza_conf_exists']:
            issues.append(f"pgBackRest stanza configuration file for '{database.name}' not found")
        
        # Check directory permissions
        for dir_path in ["/etc/pgbackrest", "/var/lib/pgbackrest", "/var/log/pgbackrest"]:
            dir_exists = ssh.execute_command(f"sudo test -d {dir_path} && echo 'exists'")
            if dir_exists['exit_code'] != 0 or 'exists' not in dir_exists['stdout']:
                issues.append(f"Required directory {dir_path} does not exist")
            else:
                permissions = ssh.execute_command(f"sudo ls -ld {dir_path}")
                if permissions['exit_code'] == 0:
                    if 'postgres' not in permissions['stdout']:
                        issues.append(f"{dir_path} is not owned by postgres user")
        
        # Run a pgbackrest check command to validate configuration
        if health_details.get('pgbackrest_installed', False) and health_details.get('pgbackrest_main_conf_exists', False):
            pgbackrest_check = ssh.execute_command(f"sudo -u postgres pgbackrest --stanza={database.name} check 2>&1")
            health_details['pgbackrest_check_passed'] = pgbackrest_check['exit_code'] == 0
            
            if not health_details['pgbackrest_check_passed']:
                issues.append(f"pgBackRest configuration check failed: {pgbackrest_check['stdout'].split('ERROR:')[-1].strip() if 'ERROR:' in pgbackrest_check['stdout'] else 'Unknown error'}")
        
        # Try to list backups if everything else is ok
        backups = []
        if health_details.get('pgbackrest_installed', False) and health_details.get('pgbackrest_main_conf_exists', False) and health_details.get('pgbackrest_stanza_conf_exists', False):
            backups = pg_manager.list_backups(database.name)
        
        # Calculate overall status
        overall_status = (
            health_details.get('postgres_running', False) and
            health_details.get('pgbackrest_installed', False) and 
            health_details.get('pgbackrest_main_conf_exists', False) and 
            health_details.get('pgbackrest_stanza_conf_exists', False) and
            health_details.get('pgbackrest_check_passed', False) and
            archive_mode_on and
            archive_cmd_correct
        )
        
        # Disconnect SSH
        ssh.disconnect()
        
        # Format response
        if overall_status:
            return jsonify({
                'success': True,
                'health': health_details,
                'backups': backups,
                'message': 'Backup system is healthy'
            })
        else:
            return jsonify({
                'success': False,
                'health': health_details,
                'backups': backups,
                'message': f'Issues found: {issues[0] if issues else "Unknown issue"}{" (+"+str(len(issues)-1)+" more)" if len(issues) > 1 else ""}',
                'issues': issues,
                'fix_url': url_for('backups.fix_config', id=backup_job.id)
            })
            
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        })

@backups_bp.route('/fix-config/<int:id>', methods=['POST'])
@login_required
@first_login_required
def fix_config(id):
    backup_job = BackupJob.query.get_or_404(id)
    server = backup_job.server
    database = backup_job.database
    s3_storage = backup_job.s3_storage
    
    try:
        # Connect to server via SSH
        ssh, pg_manager = get_managers(server)
        if not ssh or not pg_manager:
            flash('Failed to connect to server via SSH', 'danger')
            return redirect(url_for('backups.index'))
        
        # Check if PostgreSQL is installed
        if not pg_manager.check_postgres_installed():
            flash('PostgreSQL is not installed on the server. Please install PostgreSQL first.', 'danger')
            ssh.disconnect()
            return redirect(url_for('backups.index'))
        
        # Get data directory
        data_dir = pg_manager.get_data_directory()
        if not data_dir:
            flash('Could not determine PostgreSQL data directory. Using default.', 'warning')
            data_dir = "/var/lib/postgresql/17/main"  # Default for PostgreSQL 17
        
        # Find PostgreSQL configuration file
        pg_conf_file = pg_manager._find_postgresql_conf()
        if not pg_conf_file:
            flash('Could not locate PostgreSQL configuration file', 'danger')
            ssh.disconnect()
            return redirect(url_for('backups.index'))
        
        flash(f'Found PostgreSQL configuration at: {pg_conf_file}', 'info')
            
        # Backup original configuration
        ssh.execute_command(f"sudo cp {pg_conf_file} {pg_conf_file}.bak.$(date +%Y%m%d%H%M%S)")
        
        # Install pgbackrest if not installed
        pgbackrest_check = ssh.execute_command("which pgbackrest")
        if pgbackrest_check['exit_code'] != 0:
            flash('pgBackRest not installed. Installing now...', 'info')
            ssh.execute_command("sudo apt-get update")
            ssh.execute_command("sudo apt-get install -y pgbackrest")
            
            # Verify installation
            pgbackrest_verify = ssh.execute_command("which pgbackrest")
            if pgbackrest_verify['exit_code'] != 0:
                flash('Failed to install pgBackRest. Please check system package manager.', 'danger')
                ssh.disconnect()
                return redirect(url_for('backups.index'))
        
        # Create required pgBackRest directories with proper ownership and permissions
        # Main directories with extra verification
        dirs = ["/etc/pgbackrest", "/var/log/pgbackrest", "/var/lib/pgbackrest"]
        for dir_path in dirs:
            # Check if directory exists first
            dir_check = ssh.execute_command(f"sudo test -d {dir_path} && echo 'exists'")
            if dir_check['exit_code'] != 0 or 'exists' not in dir_check['stdout']:
                ssh.execute_command(f"sudo mkdir -p {dir_path}")
                flash(f'Created directory {dir_path}', 'info')
            
            # Always ensure permissions are correct
            ssh.execute_command(f"sudo chmod 750 {dir_path}")
            ssh.execute_command(f"sudo chown -R postgres:postgres {dir_path}")
        
        # Explicitly create and verify conf.d directory with proper permissions
        conf_d_path = "/etc/pgbackrest/conf.d"
        ssh.execute_command(f"sudo mkdir -p {conf_d_path}")
        ssh.execute_command(f"sudo chmod 750 {conf_d_path}")
        ssh.execute_command(f"sudo chown -R postgres:postgres {conf_d_path}")
        
        # Verify directories were created
        dir_status = {}
        for dir_path in dirs + [conf_d_path]:
            check = ssh.execute_command(f"sudo test -d {dir_path} && echo 'exists'")
            dir_status[dir_path] = 'exists' if check['exit_code'] == 0 and 'exists' in check['stdout'] else 'missing'
        
        missing_dirs = [d for d, status in dir_status.items() if status == 'missing']
        if missing_dirs:
            flash(f'WARNING: Failed to create directories: {", ".join(missing_dirs)}', 'warning')
        else:
            flash('Successfully created and verified all required pgBackRest directories', 'success')
        
        flash('Created pgBackRest directories with proper permissions', 'info')
        
        # Create main pgBackRest configuration file
        main_config = """[global]
# Path where backups and archives are stored
repo1-path=/var/lib/pgbackrest

# Configuration include path
config-include-path=/etc/pgbackrest/conf.d

# Backup retention policy
repo1-retention-full=7
repo1-retention-full-type=count

# Memory and process settings
process-max=4

# Log settings
log-level-console=info
log-level-file=debug
log-path=/var/log/pgbackrest

# Performance settings
compress-level=6
compress=y
delta=y
start-fast=y
"""

        if s3_storage:
            # Add S3 configuration if available
            main_config += f"""
# S3 settings
repo1-type=s3
repo1-s3-bucket={s3_storage.bucket}
repo1-s3-region={s3_storage.region}
repo1-s3-endpoint=s3.{s3_storage.region}.amazonaws.com
repo1-s3-key={s3_storage.access_key}
repo1-s3-key-secret={s3_storage.secret_key}
"""

        # Write the main configuration directly to the server
        write_main_config = ssh.execute_command(f"echo '{main_config}' | sudo tee /etc/pgbackrest/pgbackrest.conf > /dev/null")
        if write_main_config['exit_code'] != 0:
            flash(f"Error creating pgbackrest.conf: {write_main_config['stderr']}", 'danger')
        
        # Verify the file was created
        config_check = ssh.execute_command("sudo test -f /etc/pgbackrest/pgbackrest.conf && echo 'exists'")
        if config_check['exit_code'] != 0 or 'exists' not in config_check['stdout']:
            flash('Failed to create main pgBackRest configuration file', 'danger')
        else:
            # Set proper permissions
            ssh.execute_command("sudo chmod 640 /etc/pgbackrest/pgbackrest.conf")
            ssh.execute_command("sudo chown postgres:postgres /etc/pgbackrest/pgbackrest.conf")
            flash('Created main pgBackRest configuration file', 'success')
        
        # Create stanza-specific configuration
        stanza_config = f"""[{database.name}]
pg1-path={data_dir}
"""

        # Write the stanza configuration directly to the server
        stanza_config_path = f"/etc/pgbackrest/conf.d/{database.name}.conf"
        write_stanza_config = ssh.execute_command(f"echo '{stanza_config}' | sudo tee {stanza_config_path} > /dev/null")
        if write_stanza_config['exit_code'] != 0:
            flash(f"Error creating stanza config: {write_stanza_config['stderr']}", 'danger')
        
        # Verify the stanza file was created
        stanza_check = ssh.execute_command(f"sudo test -f {stanza_config_path} && echo 'exists'")
        if stanza_check['exit_code'] != 0 or 'exists' not in stanza_check['stdout']:
            flash(f'Failed to create stanza configuration file for {database.name}', 'danger')
        else:
            # Set proper permissions
            ssh.execute_command(f"sudo chmod 640 {stanza_config_path}")
            ssh.execute_command(f"sudo chown postgres:postgres {stanza_config_path}")
            flash(f'Created stanza configuration file for {database.name}', 'success')
        
        # Update the PostgreSQL configuration
        # First, properly format the archive_command with correct quoting
        archive_command = f"pgbackrest --stanza={database.name} archive-push %p"
        
        # Use multiple approaches to ensure settings are applied
        # 1. Direct file modification
        ssh.execute_command(f"""
        sudo sed -i 's/^[ \\t]*#*[ \\t]*archive_mode[ \\t]*=.*/archive_mode = on/' {pg_conf_file}
        sudo sed -i 's/^[ \\t]*#*[ \\t]*wal_level[ \\t]*=.*/wal_level = replica/' {pg_conf_file}
        sudo sed -i 's/^[ \\t]*#*[ \\t]*max_wal_senders[ \\t]*=.*/max_wal_senders = 10/' {pg_conf_file}
        sudo sed -i "s|^[ \\t]*#*[ \\t]*archive_command[ \\t]*=.*|archive_command = 'pgbackrest --stanza={database.name} archive-push %p'|" {pg_conf_file}
        """)
        
        # 2. Also use ALTER SYSTEM to ensure settings are applied
        alter_cmds = [
            f"sudo -u postgres psql -c \"ALTER SYSTEM SET archive_mode TO 'on';\"",
            f"sudo -u postgres psql -c \"ALTER SYSTEM SET wal_level TO 'replica';\"",
            f"sudo -u postgres psql -c \"ALTER SYSTEM SET max_wal_senders TO '10';\"",
            f"sudo -u postgres psql -c \"ALTER SYSTEM SET archive_command TO 'pgbackrest --stanza={database.name} archive-push %p';\"",
        ]
        
        for cmd in alter_cmds:
            ssh.execute_command(cmd)
        
        flash('Updated PostgreSQL configuration with archive settings', 'info')
        
        # 3. Ensure postgresql.auto.conf has the correct settings
        auto_conf_file = f"{pg_conf_file[:-13]}auto.conf"  # Replace postgresql.conf with postgresql.auto.conf
        ssh.execute_command(f"sudo grep -q 'archive_mode' {auto_conf_file} || echo \"archive_mode = 'on'\" | sudo tee -a {auto_conf_file} > /dev/null")
        ssh.execute_command(f"sudo grep -q 'archive_command' {auto_conf_file} || echo \"archive_command = 'pgbackrest --stanza={database.name} archive-push %p'\" | sudo tee -a {auto_conf_file} > /dev/null")
        
        # Always perform a full restart for archive_mode changes
        flash('archive_mode changes require a full PostgreSQL restart', 'info')
        restart_success = False
        
        # Force a complete stop and start cycle
        # 1. Stop PostgreSQL with multiple methods to ensure it's stopped
        ssh.execute_command("sudo systemctl stop postgresql")
        ssh.execute_command("sudo service postgresql stop")
        ssh.execute_command("sudo pkill -9 postgres || true")  # Force kill if still running
        time.sleep(3)  # Wait for complete shutdown
        
        # 2. Verify PostgreSQL is stopped
        is_running = ssh.execute_command("ps aux | grep postgres | grep -v grep")['exit_code'] == 0
        if is_running:
            flash('Warning: PostgreSQL processes still running after stop attempt', 'warning')
        
        # 3. Start PostgreSQL
        start_result = ssh.execute_command("sudo systemctl start postgresql")
        if start_result['exit_code'] == 0:
            restart_success = True
            flash('Successfully restarted PostgreSQL', 'success')
        else:
            # Try alternate methods
            ssh.execute_command("sudo service postgresql start")
            time.sleep(3)
            is_running = ssh.execute_command("ps aux | grep postgres | grep -v grep")['exit_code'] == 0
            if is_running:
                restart_success = True
                flash('PostgreSQL restarted successfully using alternate method', 'success')
            else:
                flash('Failed to restart PostgreSQL. Check PostgreSQL logs for errors.', 'danger')
                # Show PostgreSQL logs for debugging
                logs = ssh.execute_command("sudo tail -n 20 /var/log/postgresql/postgresql-*.log || sudo journalctl -u postgresql -n 20")
                if logs['exit_code'] == 0:
                    flash(f'PostgreSQL logs: {logs["stdout"]}', 'info')
        
        # Wait for PostgreSQL to fully start
        time.sleep(5)
        
        # Double-check archive settings directly to diagnose issues
        critical_settings = {
            'archive_mode': 'sudo -u postgres psql -t -c "SHOW archive_mode;"',
            'archive_command': 'sudo -u postgres psql -t -c "SHOW archive_command;"',
            'wal_level': 'sudo -u postgres psql -t -c "SHOW wal_level;"'
        }
        
        flash('Checking PostgreSQL settings after restart:', 'info')
        for setting, cmd in critical_settings.items():
            result = ssh.execute_command(cmd)
            if result['exit_code'] == 0:
                value = result['stdout'].strip()
                flash(f'{setting}: {value}', 'info')
                
                # If archive_mode is not 'on', try a more direct approach
                if setting == 'archive_mode' and 'on' not in value.lower():
                    flash(f'WARNING: {setting} is not set to "on" after restart. Attempting direct fix...', 'warning')
                    ssh.execute_command(f"sudo -u postgres psql -c \"ALTER SYSTEM SET archive_mode TO 'on';\"")
                    ssh.execute_command(f"echo 'archive_mode = on' | sudo tee -a {pg_conf_file} > /dev/null")
                    flash('Attempted emergency fix for archive_mode. Will need another restart.', 'warning')
                
                # If archive_command doesn't contain pgbackrest, try to fix it
                if setting == 'archive_command' and 'pgbackrest' not in value.lower():
                    flash(f'WARNING: {setting} is not properly configured for pgBackRest. Attempting direct fix...', 'warning')
                    pgbackrest_cmd = f"'pgbackrest --stanza={database.name} archive-push %p'"
                    ssh.execute_command(f"sudo -u postgres psql -c \"ALTER SYSTEM SET archive_command TO {pgbackrest_cmd};\"")
                    ssh.execute_command(f"echo \"archive_command = {pgbackrest_cmd}\" | sudo tee -a {pg_conf_file} > /dev/null")
                    flash('Attempted emergency fix for archive_command. Will need another restart.', 'warning')
                    
                    # Restart again if necessary
                    ssh.execute_command("sudo systemctl restart postgresql")
                    time.sleep(5)
            else:
                flash(f'Failed to check {setting}: {result["stderr"]}', 'warning')
                
        # Final validation of the settings
        final_check = ssh.execute_command("""
        sudo -u postgres psql -t -c "
        SELECT name, setting FROM pg_settings 
        WHERE name IN ('archive_mode', 'archive_command', 'wal_level', 'max_wal_senders');
        "
        """)
        
        if final_check['exit_code'] == 0:
            flash('Current PostgreSQL settings:', 'info')
            flash(final_check['stdout'], 'info')
            
            # Check if archive_mode is on
            archive_mode_check = ssh.execute_command("sudo -u postgres psql -t -c 'SHOW archive_mode;'")
            if archive_mode_check['exit_code'] == 0 and 'on' not in archive_mode_check['stdout'].lower():
                flash('WARNING: archive_mode is still not set to ON. Attempting direct configuration file modification.', 'warning')
                
                # Extra attempt to force archive_mode on
                ssh.execute_command(f"echo 'archive_mode = on' | sudo tee -a {pg_conf_file}")
                ssh.execute_command("sudo systemctl restart postgresql")
                time.sleep(3)
        
        # Create or update pgBackRest stanza after PostgreSQL restart
        flash('Creating pgBackRest stanza...', 'info')
        
        # First check if stanza already exists
        stanza_check = ssh.execute_command(f"sudo -u postgres pgbackrest info --stanza={database.name} || echo 'not found'")
        if 'not found' not in stanza_check['stdout'] and stanza_check['exit_code'] == 0:
            flash(f'Stanza {database.name} already exists, updating configuration', 'info')
            
            # Update stanza with new configuration
            stanza_update = ssh.execute_command(f"sudo -u postgres pgbackrest --stanza={database.name} --log-level-console=detail check")
            if stanza_update['exit_code'] == 0:
                flash('Successfully updated existing stanza', 'success')
            else:
                flash(f'Warning: Stanza check failed: {stanza_update["stderr"]}', 'warning')
                
                # Check for S3 endpoint issue
                if "requires option: repo1-s3-endpoint" in stanza_update["stderr"] and s3_storage:
                    flash('Fixing S3 endpoint configuration for stanza check...', 'info')
                    endpoint_fix = f"sudo sed -i '/repo1-s3-region/a repo1-s3-endpoint=s3.{s3_storage.region}.amazonaws.com' /etc/pgbackrest/pgbackrest.conf"
                    ssh.execute_command(endpoint_fix)
                    
                    # Try check again
                    stanza_update = ssh.execute_command(f"sudo -u postgres pgbackrest --stanza={database.name} --log-level-console=detail check")
                    if stanza_update['exit_code'] == 0:
                        flash('Successfully updated existing stanza after fixing S3 endpoint', 'success')
        
        # Create stanza if it doesn't exist or needs to be recreated
        stanza_create_cmd = f"sudo -u postgres pgbackrest --stanza={database.name} --log-level-console=detail stanza-create --force"
        stanza_result = ssh.execute_command(stanza_create_cmd)
        if stanza_result['exit_code'] == 0:
            flash(f'Successfully created pgBackRest stanza for {database.name}', 'success')
        else:
            flash(f'Failed to create pgBackRest stanza: {stanza_result["stderr"]}', 'danger')
            
            # Check for S3 endpoint issue
            if "requires option: repo1-s3-endpoint" in stanza_result["stderr"] and s3_storage:
                flash('Fixing S3 endpoint configuration for stanza creation...', 'info')
                endpoint_fix = f"sudo grep -q 'repo1-s3-endpoint' /etc/pgbackrest/pgbackrest.conf || echo 'repo1-s3-endpoint=s3.{s3_storage.region}.amazonaws.com' | sudo tee -a /etc/pgbackrest/pgbackrest.conf > /dev/null"
                ssh.execute_command(endpoint_fix)
                
                # Try stanza creation again
                retry_result = ssh.execute_command(stanza_create_cmd)
                if retry_result['exit_code'] == 0:
                    flash(f'Successfully created pgBackRest stanza after fixing S3 endpoint', 'success')
                else:
                    # Try with additional options if still failing
                    retry_cmd = f"sudo -u postgres pgbackrest --stanza={database.name} --no-online --log-level-console=detail stanza-create --force"
                    retry_result = ssh.execute_command(retry_cmd)
                    if retry_result['exit_code'] == 0:
                        flash(f'Successfully created pgBackRest stanza on second attempt', 'success')
                    else:
                        flash(f'All stanza creation attempts failed. Error: {retry_result["stderr"]}', 'danger')
            
        # Check configuration
        check_cmd = f"sudo -u postgres pgbackrest --stanza={database.name} check"
        check_result = ssh.execute_command(check_cmd)
        if check_result['exit_code'] == 0:
            flash('pgBackRest check passed successfully', 'success')
        else:
            flash(f'pgBackRest check failed: {check_result["stderr"]}', 'warning')
            
            # Check for specific S3 endpoint issue
            if "requires option: repo1-s3-endpoint" in check_result["stderr"]:
                flash('Detected missing S3 endpoint configuration, fixing...', 'info')
                
                # Fix the S3 endpoint issue
                if s3_storage:
                    # Add S3 endpoint to configuration
                    endpoint_fix = f"sudo grep -q 'repo1-s3-endpoint' /etc/pgbackrest/pgbackrest.conf || echo 'repo1-s3-endpoint=s3.{s3_storage.region}.amazonaws.com' | sudo tee -a /etc/pgbackrest/pgbackrest.conf > /dev/null"
                    ssh.execute_command(endpoint_fix)
                    
                    # Verify permissions
                    ssh.execute_command("sudo chmod 640 /etc/pgbackrest/pgbackrest.conf")
                    ssh.execute_command("sudo chown postgres:postgres /etc/pgbackrest/pgbackrest.conf")
                    
                    # Try check again
                    recheck = ssh.execute_command(check_cmd)
                    if recheck['exit_code'] == 0:
                        flash('Successfully fixed S3 endpoint configuration', 'success')
                    else:
                        flash(f'Still having issues after fixing S3 endpoint: {recheck["stderr"]}', 'warning')
            
        # Get pgBackRest info for validation
        info_cmd = f"sudo -u postgres pgbackrest --stanza={database.name} info"
        info_result = ssh.execute_command(info_cmd)
        flash(f'pgBackRest stanza info: {info_result["stdout"]}', 'info')
        
        # Disconnect SSH
        ssh.disconnect()
        
        flash('Backup configuration has been updated. Please check the status by clicking the Check button.', 'info')
    except Exception as e:
        flash(f'Error fixing backup configuration: {str(e)}', 'danger')
        
    return redirect(url_for('backups.index')) 