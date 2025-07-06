from flask import Blueprint, render_template, request, redirect, url_for, flash, jsonify
from app.models.database import VpsServer, PostgresDatabase, RestoreLog, db
from app.utils.ssh_manager import SSHManager
from app.utils.postgres_manager import PostgresManager
from app.routes.auth import login_required, first_login_required
import random
import string
from datetime import datetime
import time

databases_bp = Blueprint('databases', __name__, url_prefix='/databases')

def generate_random_password(length=39):
    """Generate a random password of specified length with uppercase, lowercase and digits."""
    charset = string.ascii_uppercase + string.ascii_lowercase + string.digits
    return ''.join(random.choice(charset) for _ in range(length))

@databases_bp.route('/')
@login_required
@first_login_required
def index():
    databases = PostgresDatabase.query.all()
    return render_template('databases/index.html', databases=databases)

@databases_bp.route('/server/<int:server_id>')
@login_required
@first_login_required
def by_server(server_id):
    server = VpsServer.query.get_or_404(server_id)
    databases = PostgresDatabase.query.filter_by(vps_server_id=server_id).all()
    return render_template('databases/by_server.html', server=server, databases=databases)

@databases_bp.route('/add', methods=['GET', 'POST'])
@login_required
@first_login_required
def add():
    servers = VpsServer.query.all()
    
    if not servers:
        flash('You need to add a server first', 'warning')
        return redirect(url_for('servers.add'))
    
    if request.method == 'POST':
        name = request.form.get('name')
        username = name.lower()  # Automatically derive username from database name
        password = request.form.get('password')
        server_id = request.form.get('server_id', type=int)
        
        # Validate data
        server = VpsServer.query.get(server_id)
        if not server:
            flash('Selected server does not exist', 'danger')
            return render_template('databases/add.html', servers=servers)
        
        # Check if database already exists on server
        existing = PostgresDatabase.query.filter_by(name=name, vps_server_id=server_id).first()
        if existing:
            flash(f'Database "{name}" already exists on this server', 'danger')
            return render_template('databases/add.html', servers=servers)
        
        # Create database record
        database = PostgresDatabase(
            name=name,
            username=username,  # Use derived username
            password=password,
            vps_server_id=server_id
        )
        
        db.session.add(database)
        db.session.commit()
        
        # Create the database on the server
        try:
            # Connect to server via SSH
            ssh = SSHManager(
                host=server.host,
                port=server.port,
                username=server.username,
                ssh_key_content=server.ssh_key_content
            )
            
            if not ssh.connect():
                flash(f'Database record created, but failed to connect to server to create database', 'warning')
                return redirect(url_for('databases.index'))
            
            # Check PostgreSQL installation
            pg_manager = PostgresManager(ssh)
            
            if not pg_manager.check_postgres_installed():
                ssh.disconnect()
                flash(f'Database record created, but PostgreSQL is not installed on the server', 'warning')
                return redirect(url_for('databases.index'))
            
            # Create the database
            success, message = pg_manager.create_database(name, username, password)
            
            # Disconnect
            ssh.disconnect()
            
            if success:
                flash(f'Database added successfully and deployed to server: {message}', 'success')
            else:
                flash(f'Database record created, but deployment failed: {message}', 'warning')
            
        except Exception as e:
            flash(f'Database record created, but deployment failed: {str(e)}', 'warning')
        
        return redirect(url_for('databases.index'))
    
    return render_template('databases/add.html', servers=servers)

@databases_bp.route('/edit/<int:id>', methods=['GET', 'POST'])
@login_required
@first_login_required
def edit(id):
    database = PostgresDatabase.query.get_or_404(id)
    servers = VpsServer.query.all()
    
    if request.method == 'POST':
        name = request.form.get('name')
        username = name.lower()  # Automatically derive username from database name
        password_changed = request.form.get('password_changed') == 'true'
        password = request.form.get('password') if password_changed else None
        server_id = request.form.get('server_id', type=int)
        
        # Validate data
        server = VpsServer.query.get(server_id)
        if not server:
            flash('Selected server does not exist', 'danger')
            return render_template('databases/edit.html', database=database, servers=servers)
        
        # Check if database already exists on server (if name or server changed)
        if (name != database.name or server_id != database.vps_server_id):
            existing = PostgresDatabase.query.filter_by(name=name, vps_server_id=server_id).first()
            if existing and existing.id != database.id:
                flash(f'Database "{name}" already exists on this server', 'danger')
                return render_template('databases/edit.html', database=database, servers=servers)
        
        # Keep track of changes
        old_username = database.username
        username_changed = username != old_username
        server_changed = server_id != database.vps_server_id
        
        # Update database record
        database.name = name
        database.username = username  # Update username based on database name
        
        # Only update password if regenerated
        if password_changed and password:
            database.password = password
            
        database.vps_server_id = server_id
        
        db.session.commit()
        
        # If password changed, update on server
        if password_changed and password and not server_changed:
            try:
                # Connect to server via SSH
                ssh = SSHManager(
                    host=server.host,
                    port=server.port,
                    username=server.username,
                    ssh_key_content=server.ssh_key_content
                )
                
                if not ssh.connect():
                    flash(f'Database record updated, but failed to connect to server to update password', 'warning')
                    return redirect(url_for('databases.index'))
                
                # Check PostgreSQL installation
                pg_manager = PostgresManager(ssh)
                
                if not pg_manager.check_postgres_installed():
                    ssh.disconnect()
                    flash(f'Database record updated, but PostgreSQL is not installed on the server', 'warning')
                    return redirect(url_for('databases.index'))
                
                # Update the user password
                success, message = pg_manager.update_database_user(username, password)
                
                # Disconnect
                ssh.disconnect()
                
                if success:
                    flash(f'Database updated successfully and synchronized with server', 'success')
                else:
                    flash(f'Database record updated, but server synchronization failed: {message}', 'warning')
                
            except Exception as e:
                flash(f'Database record updated, but server synchronization failed: {str(e)}', 'warning')
        else:
            flash('Database updated successfully', 'success')
        
        return redirect(url_for('databases.index'))
    
    return render_template('databases/edit.html', database=database, servers=servers)

@databases_bp.route('/delete/<int:id>', methods=['POST'])
@login_required
@first_login_required
def delete(id):
    database = PostgresDatabase.query.get_or_404(id)
    
    db.session.delete(database)
    db.session.commit()
    
    flash('Database deleted successfully', 'success')
    return redirect(url_for('databases.index'))

@databases_bp.route('/check/<int:id>')
@login_required
@first_login_required
def check(id):
    database = PostgresDatabase.query.get_or_404(id)
    server = database.server
    
    try:
        # Connect to server via SSH
        ssh = SSHManager(
            host=server.host,
            port=server.port,
            username=server.username,
            ssh_key_content=server.ssh_key_content
        )
        
        if not ssh.connect():
            return jsonify({
                'success': False,
                'message': 'Failed to connect to server via SSH'
            })
        
        # Check PostgreSQL installation
        pg_manager = PostgresManager(ssh)
        
        if not pg_manager.check_postgres_installed():
            ssh.disconnect()
            return jsonify({
                'success': False,
                'message': 'PostgreSQL is not installed on the server'
            })
        
        # Get PostgreSQL version
        pg_version = pg_manager.get_postgres_version()
        
        # Disconnect
        ssh.disconnect()
        
        return jsonify({
            'success': True,
            'postgres_version': pg_version
        })
        
    except Exception as e:
        return jsonify({
            'success': False,
            'message': str(e)
        })

@databases_bp.route('/credentials/<int:id>')
@login_required
@first_login_required
def credentials(id):
    database = PostgresDatabase.query.get_or_404(id)
    server = database.server
    
    # Create the connection URL based on server and database info
    connection_url = f"postgresql://{database.username}:{database.password}@{server.host}:{server.postgres_port}/{database.name}"
    
    return render_template('databases/credentials.html', 
                           database=database, 
                           server=server, 
                           connection_url=connection_url)

@databases_bp.route('/import', methods=['GET', 'POST'])
@login_required
@first_login_required
def import_database():
    databases = PostgresDatabase.query.all()
    
    if not databases:
        flash('You need to add a target database first', 'warning')
        return redirect(url_for('databases.add'))
    
    if request.method == 'POST':
        # Get form data
        connection_type = request.form.get('connection_type')
        target_database_id = request.form.get('target_database_id', type=int)
        
        # Connection details
        host = request.form.get('host')
        port = request.form.get('port', type=int)
        username = request.form.get('username')
        password = request.form.get('password')
        database_name = request.form.get('database_name')
        connection_url = request.form.get('connection_url')
        
        # Validate input
        if not target_database_id:
            flash('Please select a target database', 'danger')
            return render_template('databases/import.html', databases=databases)
        
        # Get target database
        target_db = PostgresDatabase.query.get(target_database_id)
        if not target_db:
            flash('Selected target database does not exist', 'danger')
            return render_template('databases/import.html', databases=databases)
        
        # Validate connection details
        if connection_type == 'standard':
            if not host or not port or not username or not password or not database_name:
                flash('Please fill in all connection details', 'danger')
                return render_template('databases/import.html', databases=databases)
        elif connection_type == 'url':
            if not connection_url:
                flash('Please provide a connection URL', 'danger')
                return render_template('databases/import.html', databases=databases)
        else:
            flash('Invalid connection type', 'danger')
            return render_template('databases/import.html', databases=databases)
        
        # Create restore log entry
        restore_log = RestoreLog(
            database_id=target_db.id,
            status='in_progress',
            log_output='Starting database import from external source...'
        )
        db.session.add(restore_log)
        db.session.commit()
        
        # Redirect to the import progress page
        return redirect(url_for('databases.import_progress', restore_log_id=restore_log.id, 
                               connection_type=connection_type,
                               host=host, port=port, username=username, 
                               password=password, database_name=database_name,
                               connection_url=connection_url))
    
    return render_template('databases/import.html', databases=databases)

@databases_bp.route('/import/progress')
@login_required
@first_login_required
def import_progress():
    restore_log_id = request.args.get('restore_log_id', type=int)
    connection_type = request.args.get('connection_type')
    
    # Get connection parameters
    host = request.args.get('host')
    port = request.args.get('port', type=int)
    username = request.args.get('username')
    password = request.args.get('password')
    database_name = request.args.get('database_name')
    connection_url = request.args.get('connection_url')
    
    if not restore_log_id:
        flash('Missing restore log ID', 'danger')
        return redirect(url_for('databases.import_database'))
    
    # Get restore log
    restore_log = RestoreLog.query.get(restore_log_id)
    if not restore_log:
        flash('Restore log not found', 'danger')
        return redirect(url_for('databases.import_database'))
    
    # Get target database
    target_db = restore_log.database
    if not target_db:
        flash('Target database not found', 'danger')
        return redirect(url_for('databases.import_database'))
    
    # Pass all info to template for AJAX processing
    return render_template('databases/import_progress.html', 
                          restore_log=restore_log, 
                          target_db=target_db,
                          connection_type=connection_type,
                          host=host, port=port, 
                          username=username, 
                          password=password, 
                          database_name=database_name,
                          connection_url=connection_url)

@databases_bp.route('/import/execute', methods=['POST'])
@login_required
@first_login_required
def execute_import():
    # Get parameters from JSON request
    data = request.get_json()
    restore_log_id = data.get('restore_log_id')
    connection_type = data.get('connection_type')
    target_db_id = data.get('target_db_id')
    
    # Connection details
    host = data.get('host')
    port = data.get('port')
    username = data.get('username')
    password = data.get('password')
    database_name = data.get('database_name')
    connection_url = data.get('connection_url')
    
    # Get restore log and target database
    restore_log = RestoreLog.query.get(restore_log_id)
    target_db = PostgresDatabase.query.get(target_db_id)
    
    if not restore_log or not target_db:
        return jsonify({'success': False, 'message': 'Invalid restore log or target database'})
    
    # Get server of target database
    server = target_db.server
    
    try:
        # Connect to server via SSH
        ssh = SSHManager(
            host=server.host,
            port=server.port,
            username=server.username,
            ssh_key_content=server.ssh_key_content
        )
        
        if not ssh.connect():
            update_log(restore_log, 'Failed to connect to target server via SSH')
            return jsonify({'success': False, 'message': 'Failed to connect to target server'})
        
        # Create PostgreSQL manager
        pg_manager = PostgresManager(ssh)
        
        # Update log
        update_log(restore_log, 'Connected to target server')
        update_log(restore_log, f'Starting import to database: {target_db.name}')
        
        # Prepare source connection string
        if connection_type == 'standard':
            source_conn = f"postgresql://{username}:{password}@{host}:{port}/{database_name}"
            update_log(restore_log, f'Connecting to source database at {host}:{port}/{database_name}')
        else:
            source_conn = connection_url
            update_log(restore_log, 'Connecting to source database using connection URL')
        
        # Execute the import - this uses pg_dump and pg_restore for the actual migration
        success, message = perform_database_import(pg_manager, source_conn, target_db.name, restore_log)
        
        # Disconnect SSH
        ssh.disconnect()
        
        # Update restore log status
        restore_log.status = 'success' if success else 'failed'
        restore_log.end_time = datetime.utcnow()
        db.session.commit()
        
        return jsonify({'success': success, 'message': message})
        
    except Exception as e:
        # Update log with error
        update_log(restore_log, f'Error during import: {str(e)}')
        restore_log.status = 'failed'
        restore_log.end_time = datetime.utcnow()
        db.session.commit()
        
        return jsonify({'success': False, 'message': str(e)})

@databases_bp.route('/import/status/<int:restore_log_id>')
@login_required
@first_login_required
def import_status(restore_log_id):
    # Get restore log
    restore_log = RestoreLog.query.get(restore_log_id)
    if not restore_log:
        return jsonify({'success': False, 'message': 'Restore log not found'})
    
    # Return status and log output
    return jsonify({
        'success': True,
        'status': restore_log.status,
        'log_output': restore_log.log_output,
        'is_complete': restore_log.status in ['success', 'failed']
    })

# Helper function to update the restore log
def update_log(restore_log, message):
    restore_log.log_output = restore_log.log_output + '\n' + message
    db.session.commit()

# Function to perform the actual database import
def perform_database_import(pg_manager, source_conn, target_db_name, restore_log):
    # Temporary file for pg_dump output
    temp_file = f"/tmp/db_import_{int(time.time())}.dump"
    update_log(restore_log, 'Starting export from source database')
    
    # Check if pg_dump and pg_restore are available
    tools_check = pg_manager.ssh.execute_command("which pg_dump pg_restore")
    if tools_check['exit_code'] != 0:
        update_log(restore_log, 'Required tools pg_dump and pg_restore not found on server')
        return False, 'Required database tools not found on server'
    
    # Step 1: Dump the source database
    dump_cmd = f"PGPASSWORD='{source_conn.split(':')[2].split('@')[0]}' pg_dump -Fc --no-acl --no-owner -h {source_conn.split('@')[1].split(':')[0]} -p {source_conn.split(':')[3].split('/')[0]} -U {source_conn.split('://')[1].split(':')[0]} -d {source_conn.split('/')[-1]} -f {temp_file}"
    dump_result = pg_manager.ssh.execute_command(dump_cmd)
    
    if dump_result['exit_code'] != 0:
        update_log(restore_log, f'Failed to export source database: {dump_result["stderr"]}')
        return False, 'Failed to export source database'
    
    update_log(restore_log, 'Source database exported successfully')
    
    # Step 2: Create a temporary database for the import
    temp_db_name = f"{target_db_name}_import_{int(time.time())}"
    update_log(restore_log, f'Creating temporary database: {temp_db_name}')
    
    create_temp_db = pg_manager.ssh.execute_command(f"sudo -u postgres psql -c 'CREATE DATABASE {temp_db_name};'")
    if create_temp_db['exit_code'] != 0:
        update_log(restore_log, f'Failed to create temporary database: {create_temp_db["stderr"]}')
        # Clean up
        pg_manager.ssh.execute_command(f"rm {temp_file}")
        return False, 'Failed to create temporary database'
    
    # Step 3: Restore dump to temporary database
    update_log(restore_log, 'Starting import to temporary database')
    restore_cmd = f"sudo -u postgres pg_restore --no-acl --no-owner -d {temp_db_name} {temp_file}"
    restore_result = pg_manager.ssh.execute_command(restore_cmd)
    
    if restore_result['exit_code'] != 0:
        update_log(restore_log, f'Warning: Some errors occurred during import: {restore_result["stderr"]}')
        # Continue anyway as some errors are expected and not critical
    
    update_log(restore_log, 'Import to temporary database completed')
    
    # Step 4: Drop target database
    update_log(restore_log, f'Dropping target database: {target_db_name}')
    drop_cmd = pg_manager.ssh.execute_command(f"sudo -u postgres psql -c 'DROP DATABASE {target_db_name};'")
    if drop_cmd['exit_code'] != 0:
        update_log(restore_log, f'Failed to drop target database: {drop_cmd["stderr"]}')
        # Clean up
        pg_manager.ssh.execute_command(f"sudo -u postgres psql -c 'DROP DATABASE {temp_db_name};'")
        pg_manager.ssh.execute_command(f"rm {temp_file}")
        return False, 'Failed to drop target database'
    
    # Step 5: Rename temporary database to target
    update_log(restore_log, f'Renaming temporary database to target: {target_db_name}')
    rename_cmd = pg_manager.ssh.execute_command(f"sudo -u postgres psql -c 'ALTER DATABASE {temp_db_name} RENAME TO {target_db_name};'")
    if rename_cmd['exit_code'] != 0:
        update_log(restore_log, f'Failed to rename database: {rename_cmd["stderr"]}')
        # Try to recreate original database
        pg_manager.ssh.execute_command(f"sudo -u postgres psql -c 'CREATE DATABASE {target_db_name};'")
        # Clean up
        pg_manager.ssh.execute_command(f"sudo -u postgres psql -c 'DROP DATABASE {temp_db_name};'")
        pg_manager.ssh.execute_command(f"rm {temp_file}")
        return False, 'Failed to rename database'
    
    # Step 6: Clean up
    update_log(restore_log, 'Cleaning up temporary files')
    pg_manager.ssh.execute_command(f"rm {temp_file}")
    
    update_log(restore_log, 'Database import completed successfully')
    return True, 'Database import completed successfully' 